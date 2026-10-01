# retina-orchestrator — how it works

Internal documentation of the code as it stands on the `research` branch. The
`README.md` is partly stale (it still lists `--pd-path`, `--issuance-rate`,
`--seed`, `--max-cycles`, none of which exist any more); `main.go` is the source
of truth for flags.

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
                         PD (JSON line)        FIE (JSON line)
                                  ▼               │
                           ┌──────────────────────────────────┐
                           │   agent  (one TCP conn each)     │
                           │   reader → processor → writer    │
                           │              │                   │
                           │           caracal subprocess     │
                           └──────────────────────────────────┘
```

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

`Orchestrator.Run` ([orchestrator.go:205](internal/orchestrator/orchestrator.go:205))
starts five goroutines in one `errgroup`; any of them returning an error
cancels the shared context and stops the whole process:

| Goroutine        | What it does                                                            |
| ---------------- | ----------------------------------------------------------------------- |
| `runAPIServer`   | HTTP server on `--api-addr`; emits `OrchestratorStarted/Stopped` events |
| `runAgentServer` | TCP listener on `--agent-addr`, one goroutine per agent connection      |
| `runScheduler`   | Loop: `scheduler.Next()` → `pdQueue.TryPush(pd.AgentID, pd)`            |
| `runCapturer`    | Drains `captureCh` into DuckDB, plus a periodic flush ticker            |
| closer           | On ctx done, `scheduler.Close()` (which unblocks `Next`)                |

## 3. PD path (downstream)

### 3.1 Insertion — `POST /api/v1/pds`

[api_server.go:213](internal/orchestrator/api_server.go:213). Body is
`{"probing_directives":[...]}`. The handler calls `scheduler.Insert` for every
PD **sequentially and synchronously**, then emits one `PDBulkInsertionEvent`
containing the full request, then replies with `{inserted_count, assigned_ids}`
(HTTP 500 if it aborted part-way).

`ResearchScheduler.Insert` ([research_scheduler.go:442](internal/orchestrator/research_scheduler.go:442)):

1. Assigns the ID: `periodArray.Add(...)` returns the next index, which becomes
   `ProbingDirectiveID`. Any ID in the request is overwritten. IDs are therefore
   dense, starting at 0, in admission order.
2. Admission pacing: a one-token bucket at `--rr-admission-rate` (default
   1000/s). The call sleeps/busy-waits **while holding `bucketMu`** until its
   slot, so a 10k batch takes about 10 s and concurrent requests are serialized.
3. Pushes the PD onto `insertCh` (size `--rr-insert-channel-size`, 1024);
   blocks if the scheduler goroutine is not draining.

The record is only created later, inside the scheduler goroutine
(`insert`, [research_scheduler.go:703](internal/orchestrator/research_scheduler.go:703)):
first issuance is scheduled at `now + X` where `X ~ U((1-β)·Μ, (1+β)·Μ)` and
`Μ = --rr-starting-issuance-period` (default 10 s). So nothing is issued for
roughly the first 10 s after an insert.

### 3.2 Scheduling — `ResearchScheduler`

All scheduler state (`records`, the `queue` min-heap ordered by `nextIssuance`,
`addressTAT`, counters) is touched **only from the goroutine that calls
`Next`**. `Insert` and `Update` just write to channels.

`Next()` ([research_scheduler.go:479](internal/orchestrator/research_scheduler.go:479)):

1. `drain()`: applies at most `MaxInsertDrainPerIssuance` inserts and
   `MaxUpdateDrainPerIssuance` FIE updates (5 each by default), services the
   status ticker, and blocks if the heap is empty.
2. Looks at the heap root. If it is more than `BusyTolerance` away, `wait()`
   sleeps interruptibly — still applying inserts/updates with **no quota** —
   and returns early if an insert changed the root.
3. Pops the root, counts it as late if `now - nextIssuance > LatenessTolerance`,
   then either `retire`s it (`--rr-single-issuance`) or runs `compute` and
   pushes it back.
4. Busy-waits to the exact target time and returns the PD.

`compute` ([research_scheduler.go:842](internal/orchestrator/research_scheduler.go:842))
adjusts the PD's period μ on every issuance:

- **Staleness**: once the per-PD FIE history (`m = --rr-fie-history-capacity`,
  default 6) is full, all-equivalent history → `μ·(1+α)` (slow down), otherwise
  `μ/(1+α)` (speed up). "Equivalent" = same near and same far address, nil
  counting as a value.
- **Responsible probing**: per-address GCRA. For the PD's last seen near and far
  reply addresses, `reserveAndFloor` advances that address's theoretical
  arrival time by `1/Λ` (`--rr-impact-threshold`) and returns a minimum period;
  the larger of the two, widened by `1/(1-β)`, is a floor on μ. This runs even
  when the PD has never produced an FIE (addresses nil → floor 0).
- **Clamp** to `[μmin, μmax]`.
- Next issuance = `t + U((1-β)μ, (1+β)μ)`; `lastIssuedAt = t`.

`update` ([research_scheduler.go:747](internal/orchestrator/research_scheduler.go:747))
is what an FIE does to the scheduler: unknown PD IDs are ignored; otherwise it
records `lastNear/lastFar`, appends to the history ring, and recomputes
`impactDelay` as (midpoint of the agent's sent/received timestamps) −
`lastIssuedAt`, falling back to `--rr-default-impact-delay` when that is ≤ 0.
This mixes the agent clock with the orchestrator clock.

### 3.3 Dispatch — per-agent queue

`runScheduler` ([orchestrator.go:288](internal/orchestrator/orchestrator.go:288))
calls `pdQueue.TryPush(pd.AgentID, pd)`. `structures.Queue` is a map of
agent ID → buffered channel of size `--pd-queue-size` (default 100). The push
is **non-blocking**; the PD is dropped when

- no agent with that ID is currently connected, or
- that agent's channel is full.

Both cases produce only a `Debug` log ("PD dropped: no queue for agent" — the
message is the same for a full buffer) and no metric. The scheduler has already
counted the issuance, set `lastIssuedAt` and reserved the address slots, so
from its point of view the PD was issued.

### 3.4 Agent connection

`agentServer` ([agent_server.go](internal/orchestrator/agent_server.go)) accepts
TCP, enables keepalive (30 s idle, 10 s × 3 probes), and runs the handshake with
a 5 s deadline: read one `AuthRequest` line, compare `Secret` to
`RETINA_SECRET` (plain equality; both empty means no auth), write
`AuthResponse`, clear deadlines.

`agentHandler` ([orchestrator.go:438](internal/orchestrator/orchestrator.go:438))
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
| `CurrentStatusEvent` every `--rr-status-interval`                                 | scheduler           | on                                      |
| `PDInsertedEvent`, `PeriodAdjustedEvent`, `SchedulerLateEvent`, `PeriodDumpEvent` | scheduler           | **off** (`--rr-disable-*` default true) |

`CurrentStatusEvent` is the main health signal: cumulative insertions /
issuances / updates, realized issuance and update rates, channel occupancies,
late count, clamp counts. It is only emitted from inside `drain`/`wait`, i.e.
only while the scheduler goroutine is cycling.

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

- **Silent PD loss at dispatch** (§3.3): disconnected agent or a full 100-slot
  queue drops the PD at `Debug` level while the scheduler counts it as issued.
  `cumulative_issuances` can exceed `pds_sent_total` with no other trace.
- **`Insert` is documented as goroutine-safe but `AtomicFloat64Array.Add` is
  not** ([atomic_array.go:38](internal/orchestrator/structures/atomic_array.go:38),
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
| `internal/orchestrator/agent_server.go`       | TCP listener, handshake, NDJSON send/receive                           |
| `internal/orchestrator/api_server.go`         | HTTP routes, bulk insert                                               |
| `internal/orchestrator/research_scheduler.go` | scheduler (DSD v1.2)                                                   |
| `internal/orchestrator/scheduler.go`          | `Scheduler` interface                                                  |
| `internal/orchestrator/event_bus.go`          | event types, bus, JSONL persistence                                    |
| `internal/orchestrator/capturer.go`           | DuckDB capture                                                         |
| `internal/orchestrator/structures/`           | `Queue`, `RingBuffer`, `AtomicFloat64Array`                            |
| `scripts/`                                    | `bulk_push.sh` (batched POST), `mock_agent.sh`, `orch.sh`, `memlog.sh` |
| `test/`                                       | sample PD JSONL files                                                  |
