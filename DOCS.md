# retina-orchestrator — how it works

Internal documentation of the code as it stands on the current research
branch. `main.go` and `--help` remain the source of truth for flags.

The agent side is documented in `retina-agent/DOCS.md`.

## 1. System overview

```
                 POST /api/v1/pds                       GET /api/v1/stream (NDJSON FIEs)
 generator / ───────────────────────┐              ┌──────────────────────► clients
 bulk_push.sh                       ▼              │   GET /api/v1/sse (NDJSON events)
                           ┌──────────────────────────────────┐
                           │           orchestrator           │
                           │                                  │
                           │  ResearchScheduler (heap)        │
                           │      │ Next()        ▲ Update()  │
                           │      ▼               │           │
                           │  pdQueue[agentID]    │           │──► DuckDB capture files
                           │      │               │           │──► events-*.jsonl
                           └──────┼───────────────┼───────────┘
                         PD (CSV line)         FIE (CSV line)
                                  ▼               │
                           ┌──────────────────────────────────┐
                           │   agent  (one TCP conn each)     │
                           │   reader → processor → writer    │
                           │              │                   │
                           │           caracal subprocess     │
                           └──────────────────────────────────┘
```

The JSON authentication request/response remains unchanged. After it succeeds,
PD records are `id,"destination",near_ttl,protocol,first_half_word,second_half_word`
and FIE records are
`id,capture_unix,"near_address",near_delta,"far_address",far_delta`.

- **Probing Directive (PD)**: "agent X, probe destination D with protocol P at
  TTL `near_ttl` and `near_ttl+1`". Carries the flow identifier (ICMP half
  words or UDP ports).
- **Forwarding Info Element (FIE)**: result of one PD execution: the reply
  address seen at the near TTL and at the far TTL (either can be missing), with
  sent/received timestamps.
- Wire types live in `retina-commons/api/v1/types.go` (v1.0.0 in both repos).

## 2. Process layout

`main.go` parses flags (every flag has a `RETINA_*` env fallback), starts a
Prometheus server on `--metrics-addr` (default `:9312`), builds the
`Orchestrator` and calls `Run`. `RETINA_SECRET` is env-only.

`Orchestrator.Run` (`internal/orchestrator/orchestrator.go`)
starts five goroutines in one `errgroup`; any of them returning an error
cancels the shared context and stops the whole process:

| Goroutine        | What it does                                                            |
| ---------------- | ----------------------------------------------------------------------- |
| `runAPIServer`   | HTTP server on `--api-addr`; emits `OrchestratorStarted/Stopped` events |
| `runAgentServer` | TCP listener on `--agent-addr`, one goroutine per agent connection      |
| `runScheduler`   | Loop: `scheduler.Next()` → bounded-wait dispatch to the agent queue      |
| `runCapturer`    | Drains `captureCh` into DuckDB, plus a periodic flush ticker            |
| closer           | On ctx done, `scheduler.Close()` (which unblocks `Next`)                |

## 3. PD path (downstream)

### 3.1 Insertion — `POST /api/v1/pds`

The handler in `internal/orchestrator/api_server.go` accepts a body of
`{"probing_directives":[...]}`. The handler calls `scheduler.Insert` for every
PD **sequentially and synchronously**, then emits one `PDBulkInsertionEvent`
containing the full request, then replies with `{inserted_count, assigned_ids}`
(HTTP 500 if it aborted part-way).

`ResearchScheduler.Insert` (`internal/orchestrator/research_scheduler.go`):

1. Atomically assigns a dense ID starting at 0 and overwrites the request's
   `ProbingDirectiveID`.
2. Copies the PD into one event and pushes it onto the scheduler event channel
   (size `--rr-event-channel-size`, 1024); the call blocks when it is full.

The record is only created later, inside the scheduler goroutine
(`insert` in `internal/orchestrator/research_scheduler.go`):
the first issuance is due immediately. Every later issuance stays on the PD's
fixed grid at `--rr-starting-period` (default 10 s).

### 3.2 Scheduling — `ResearchScheduler`

All scheduler state is touched **only from the goroutine that calls `Next`**.
Each agent owns a min-heap of its PDs, and agents are nodes in either an
included or excluded linked list. `Insert`, `Update`, and agent-state changes
write to one event channel.

`Next()` (`internal/orchestrator/research_scheduler.go`):

1. Applies at most `--rr-max-events-per-pass` pending events (64 by default).
2. Scans the included agents once and selects the earliest heap root.
3. If it is not due, sleeps on a timer that wakes early for an event or shutdown.
4. When due, returns the PD and reschedules it on its original fixed-period
   grid. Missed slots are skipped, so stalls and exclusions cause at most one
   catch-up issuance and do not accumulate phase drift.
5. When `--rr-max-issuance-count` is nonzero, removes each PD after that many
   issuances. Zero issues indefinitely.

Agent connection includes that agent in scheduling; disconnection excludes it.
An excluded agent's PD heaps retain their schedules. FIE updates currently keep
only the latest capture time and near/far reply addresses; they do not adjust
periods.

### 3.3 Dispatch — per-agent queue

`runScheduler` (`internal/orchestrator/orchestrator.go`) calls `dispatch`.
`structures.Queue` is a map of agent ID to a buffered queue of size
`--pd-queue-size` (default 100). Dispatch first tries a non-blocking push. When
the queue is full it waits for room for up to `--pd-push-timeout` (default 1 s;
zero waits until room, cancellation, or disconnect).

A PD is dropped when the agent is not connected, disconnects while dispatch is
waiting, or the push timeout expires. `pds_dropped_total{agent_id,reason}` uses
reasons `not_connected`, `disconnected`, and `timeout`; warning logs are rate
limited. The scheduler has already counted the issuance before dispatch.

### 3.4 Agent connection

`agentServer` ([agent_server.go](internal/orchestrator/agent_server.go)) accepts
TCP, enables keepalive (30 s idle, 10 s × 3 probes), and runs the handshake with
a 5 s deadline: read one `AuthRequest` line, compare `Secret` to
`RETINA_SECRET` (plain equality; both empty means no auth), write
`AuthResponse`, clear deadlines.

The handshake is newline-delimited JSON. The connection then switches to CSV:

```text
PD:  id,"destination",near_ttl,protocol,first_half_word,second_half_word
FIE: id,capture_unix,"near_address",near_delta,"far_address",far_delta
```

Blank boundary lines are ignored because the JSON decoder may leave its final
newline in the shared buffered reader. No protocol version is negotiated, so
agents and orchestrators must be upgraded together.

`agentHandler` (`internal/orchestrator/orchestrator.go`)
then registers a queue consumer under the agent ID. If that ID is already
registered the new connection is closed right after a _successful_ auth
response ("Agent already connected, rejecting"). Otherwise three goroutines run
until the first error:

- **receiver**: `receiveFIE` (no read deadline) → see §4.
- **sender**: `consumer.Pop` → `sendPD` with a 5 s write deadline.
- **closer**: closes the socket when the group context ends.

On disconnect the consumer is removed and whatever was still buffered in its
channel is discarded.

## 4. FIE path (upstream)

For each FIE line received from an agent, in order:

The CSV decoder restores the authenticated agent ID, production timestamp, and
near/far reply information. Each non-empty observation's received timestamp is
`capture_unix - delta`; its sent timestamp is set equal to received timestamp
because the compact wire record does not contain RTT information. Destination,
source, protocol, and IP-version fields are not carried in the FIE row.

1. `FIEsReceivedTotal{agent_id}` is incremented.
2. `scheduler.Update(fie)` — a **blocking** send on `updateCh` (size 1024).
3. `allowFIE` — `--fie-filter-policy`: `any` (default), `one`, `both`. Filtered
   FIEs still reached the scheduler in step 2 but go no further.
4. If the capturer is enabled, a **blocking** send on `captureCh`
   (`--capturer-channel-size`, 200k).
5. `fieRingBuffer.Push(fie)` for `/api/v1/stream` clients.

Steps 2 and 4 are the backpressure points: if the scheduler goroutine or DuckDB
falls behind, the receiver goroutine stops reading the socket, the agent's TCP
send buffer fills, and the agent's 5 s write deadline eventually fires on its
side.

### 4.1 Streaming — `GET /api/v1/stream`

`structures.RingBuffer` ([ringbuffer.go](internal/orchestrator/structures/ringbuffer.go))
is a fixed array (`--ring-buffer-size`, default **100**) with one `head` and a
`tail` per consumer, all under one mutex + cond. A consumer whose tail gets
lapped is advanced and its `totalSkipped` incremented; the sequence number
handed to the client is `delivered + skipped`, so gaps in `sequence_number`
mean lost FIEs for that client. Because empty is encoded as `tail == head`, a
ring of capacity N holds N−1 readable items.

`--stream-start-from-earliest` (default true) selects the "tail follower"
variant: a new consumer starts at index 0 while fewer than N items have been
pushed. Once the ring has wrapped, it starts at `head`, which is
indistinguishable from empty — so on a wrapped ring a new client effectively
only sees new FIEs.

### 4.2 Capture — DuckDB

`DDBFIECapturer` ([capturer.go](internal/orchestrator/capturer.go)) writes a
compact row per FIE into `fies-<intervalStartUTC>.duckdb` under
`--capturer-capture-dir`, one file per `--capturer-rotation-interval` (6 h).
Row = `pd_id uint32, near/far reply address blobs, capture_second uint16,
time_deltas uint32` (five 6-bit second deltas relative to capture time; 63 =
missing/out of range). Flushed every `--capturer-batch-size` rows and every
`--capturer-flush-period`.

Startup fails if the capture dir is non-empty unless
`--capturer-allow-non-empty-capture-dir`. Any `Capture`/`Flush` error (including
a PD ID above `uint32`) is returned from `runCapturer` and therefore **stops the
whole orchestrator**.

## 5. Events

`EventBus` ([event_bus.go](internal/orchestrator/event_bus.go)) stamps each event
with its Go type name and time, optionally appends it as a JSON line to
`<events-dir>/events-<rotation>.jsonl` (same rotation interval as the
capturer), and pushes it into a tail-follower ring (`--event-bus-size`, 1 Mi).
`GET /api/v1/sse` streams that ring as plain NDJSON (not real SSE framing).

| Event                                                                             | Emitted by          | Default                                 |
| --------------------------------------------------------------------------------- | ------------------- | --------------------------------------- |
| `OrchestratorStartedEvent` / `StoppedEvent`                                       | `runAPIServer`      | on                                      |
| `AgentConnectedEvent` / `AgentDisconnectedEvent`                                  | `agentHandler`      | on                                      |
| `PDBulkInsertionEvent` (full PD list)                                             | bulk insert handler | on                                      |

## 6. Metrics

`metrics.go` registers more than is used. Actually updated: `agents_connected`,
`auth_failures_total`, `agent_disconnections_total`, `pds_sent_total`,
`fies_received_total`, `agent_queue_size`, and the `stream_*` family.
Never updated: `pds_total`, `cycle_*`, `cycles_total`, `pds_skipped_total`, all
`sse_*`.

## 7. Shutdown

SIGINT/SIGTERM cancels the root context. The scheduler is closed (cancelling
its own context, which makes `Next` return an error wrapping
`context.Canceled`), the HTTP and agent servers get 3 s each, the capturer
flushes and checkpoints.

## 8. Things worth knowing when debugging

Observations from reading the code, not confirmed bugs:

- **PD loss after scheduling** (§3.3): dispatch can still drop an already
  scheduled PD when the agent is absent, disconnects, or remains backpressured
  through `--pd-push-timeout`. The loss is visible in
  `pds_dropped_total{reason=...}` and rate-limited warnings.
- **`Insert` is documented as goroutine-safe but `AtomicFloat64Array.Add` is
  not** (`internal/orchestrator/structures/atomic_array.go`,
  called before `bucketMu` is taken). Two concurrent `POST /pds` requests can
  hand out the same ID or lose an entry.
- **Bulk insert reporting**: the log line and `PDBulkInsertionEvent` use the
  request length, not the number actually inserted.
- **Duplicate agent ID**: the second connection authenticates successfully and
  is then dropped; the agent sees EOF and enters its reconnect backoff.
  `agents_connected` is still incremented/decremented for it.
- **Update quota vs. FIE backlog**: when the scheduler is running late it never
  enters `wait`, so it applies at most 5 updates per issuance; `Update` blocks
  once `updateCh` (1024) is full, which stalls that agent's receiver (§4).
- **`impactDelay`** uses `lastIssuedAt`, which is the _latest_ issuance, not
  necessarily the one that produced the FIE, and compares agent timestamps to
  the orchestrator clock.
- **`OrchestratorStartedEvent` embeds `Config`, which includes `Secret`**
  (`json:"secret"`), so the shared secret is written to the events file and
  served on `/api/v1/sse`.
- **Period dump goroutine** ranges over `periodTicker.C` and then selects on the
  same ticker again inside `periodicDump`, so the outer loop body runs once and
  the inner loop does the work; harmless but confusing.
- **Stream lag metric** is `now − production_timestamp` (agent clock), not time
  since receipt.

## 9. Repo map

| Path                                          | Contents                                                               |
| --------------------------------------------- | ---------------------------------------------------------------------- |
| `main.go`                                     | flags, metrics server, wiring                                          |
| `internal/orchestrator/orchestrator.go`       | `Config`, `Run`, agent/stream/SSE handlers                             |
| `internal/orchestrator/agent_server.go`       | TCP listener, JSON handshake, compact CSV PD/FIE transport             |
| `internal/orchestrator/api_server.go`         | HTTP routes, bulk insert                                               |
| `internal/orchestrator/research_scheduler.go` | scheduler (DSD v1.2)                                                   |
| `internal/orchestrator/scheduler.go`          | `Scheduler` interface                                                  |
| `internal/orchestrator/event_bus.go`          | event types, bus, JSONL persistence                                    |
| `internal/orchestrator/capturer.go`           | DuckDB capture                                                         |
| `internal/orchestrator/structures/`           | `Queue`, `RingBuffer`, `AtomicFloat64Array`                            |
| `scripts/`                                    | bulk loaders, run/monitor helpers, and a legacy JSON mock agent        |
| `test/`                                       | sample PD JSONL files                                                  |
