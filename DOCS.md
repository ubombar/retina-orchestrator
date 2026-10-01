# retina-orchestrator — how it works

Internal documentation of the code on the research branch. `main.go` and `--help` remain the source of truth for flags. The agent side is documented in `retina-agent/DOCS.md`.

## 1. Overview

```
 bulk_push.sh ── POST /api/v1/pds ──┐
                                    ▼
                  ┌───────────────────────────────────┐
                  │           orchestrator            │
                  │                                   │
                  │  Scheduler (one heap per agent)   │
                  │      │ Issue                      │
                  │      ▼                            │
                  │  per-agent sender ─ buffer ─ flush│
                  │                                   │
                  │  per-agent receiver ─► FIE queue ─┼──► DuckDB capture files
                  └──────┼─────────────────▲──────────┘
                    PD (CSV line)     FIE (CSV line)
                         ▼                 │
                  ┌───────────────────────────────────┐
                  │   agent (one TCP connection each) │
                  └───────────────────────────────────┘
```

- **Probing Directive (PD)**: "agent X, probe destination D with protocol P at TTL `near_ttl` and `near_ttl+1`". It carries the flow identifier (ICMP half-words or UDP ports).
- **Forwarding Info Element (FIE)**: the result of one PD: the reply address seen at the near TTL and at the far TTL, either of which can be missing, with the agent's capture time.

All code is in `internal/retina`. The components expose plain functions and methods; the orchestrator owns every goroutine and their lifetime.

## 2. Types — `types.go`

The orchestrator uses its own compact `PD` and `FIE` structs, which mirror the CSV records field for field. The larger `ProbingDirective` and `ForwardingInfoElement` from `retina-commons` appear only at the edges: the HTTP API converts incoming PDs with `compactPD`, and `expandFIE` rebuilds a full FIE for the capturer. PD IDs are `uint32`, addresses are `netip.Addr`.

## 3. Process layout — `orchestrator.go`

`main.go` parses the flags into a `Config`, reads `RETINA_SECRET` from the environment, and calls `Orchestrator.Run`. `Run` binds the agent port and the API port first, so a port conflict fails at once, then runs these goroutines in one `errgroup`:

| Goroutine | What it does |
| --- | --- |
| closer | When the context ends, closes the agent listener and the HTTP server |
| API server | Serves `POST /api/v1/pds` |
| scheduler | `Scheduler.Run`, the only goroutine that touches the schedule |
| capturer | `runCapturer`, drains the FIE queue into DuckDB |
| accept loop | Accepts agent connections and starts `serveAgent` for each |

An error from the API server, the capturer or the accept loop stops the orchestrator. A failing agent connection never does.

## 4. Agent connections — `agent_server.go`

`ListenAgents` returns an `AgentListener`; `Accept` returns an `AgentConn` with TCP keepalive set from the config. The connection offers `Handshake`, `SendPD`, `Flush`, `ReceiveFIE` and `Close`.

- **Handshake**: reads one `AuthRequest` JSON line, compares the secret in constant time, rejects an empty agent ID, and answers with an `AuthResponse` line. It is bounded by `--agent-handshake-timeout`.
- **No deadlines after the handshake.** `SendPD`, `Flush` and `ReceiveFIE` wait for as long as the peer applies backpressure. `Close` unblocks them. A dead peer is detected by TCP keepalive when the connection is idle, and by the kernel's retransmission timeout when data is in flight.
- **Buffered sending**: `SendPD` writes the PD into a buffer of `--agent-write-buffer-size` bytes. The buffer is sent when it fills up or when `Flush` is called.

`serveAgent` runs one connection:

1. `Handshake`.
2. `Scheduler.NewIssuer(agentID)`. This fails if the agent ID already has an open issuer, which is how a duplicate agent is rejected: it sees a successful handshake and then EOF.
3. Three goroutines until the first failure: the **sender** loops `Issue` then `SendPD`; the **flusher** calls `Flush` every `--agent-flush-period`; the **receiver** loops `ReceiveFIE`, passes each FIE to `Scheduler.Update`, and pushes it into the FIE queue.
4. On exit it closes the connection and the issuer and waits for the other two goroutines.

## 5. Scheduler — `scheduler.go`

The scheduler is event-based. `Scheduler.Run` owns all state and applies events from one queue (`--scheduler-event-queue-size`); the public methods only queue events, and block while the queue is full. The scheduler goroutine never waits on an agent.

| Call | Event | What the scheduler does |
| --- | --- | --- |
| `Insert(pds)` | insert | Stores each PD in its agent's node and schedules it one starting period from now |
| `Update(fie)` | update | Nothing yet |
| `NewIssuer(agentID)` | connect | Attaches the issuer to the agent's node, or rejects it if one is attached |
| `Issuer.Issue(ctx)` | issue | Hands out the node's next PD with its due time |
| `Issuer.Close()` | disconnect | Detaches the issuer |

- **Nodes.** Each agent ID has one node holding its PDs and a min-heap ordered by due time. A node is created by the first inserted PD or the first connection for that ID, and is never removed. An agent is active while its node has an issuer.
- **Insert.** IDs are assigned and the event is queued under one mutex, so PDs enter the schedule in ID order even with concurrent requests. IDs are consecutive 32-bit numbers starting at 0; `Insert` fails when they run out. A batch is all or nothing.
- **Issue.** The scheduler answers a request at once with the heap's root and its due time, and the issuer's own goroutine sleeps until then. If the heap is empty, the request stays pending until a PD is inserted for that agent.
- **Rescheduling.** An issued PD is due again at `max(due, now) + starting period`. A late agent therefore shifts its schedule instead of skipping or repeating PDs.
- **Retirement.** With a non-zero `--scheduler-max-issuance-count`, a PD is removed from the heap after that many issuances.

## 6. HTTP API — `api_server.go`

`POST /api/v1/pds` decodes `{"probing_directives":[...]}`, converts every PD with `compactPD`, and passes them to `Scheduler.Insert` as one batch. An invalid PD rejects the request with 400 and its index; a stopped scheduler gives 503. The response is `{"inserted_count", "first_id"}`. There is no request size limit.

## 7. Capture — `capturer.go`

`runCapturer` drains the FIE queue (`--capturer-queue-size`) into `DDBFIECapturer` and calls `Flush` every `--capturer-flush-period`. At shutdown it writes what is still queued, then flushes and closes the file.

The capturer writes one compact row per FIE into `fies-<intervalStartUTC>.duckdb`, one file per `--capturer-rotation-interval`: PD ID (`uint32`), near and far reply address blobs, the capture second within the interval (`uint16`), and five 6-bit second deltas packed into a `uint32`. The format is described at the top of `capturer.go`. That file is kept as it was before the rewrite and must not be changed casually.

- Startup fails if the capture directory is not empty, unless `--capturer-allow-non-empty-capture-dir` is set.
- DuckDB's memory grows over the life of a file, which is why the rotation interval defaults to one hour.

## 8. Backpressure

Nothing is dropped and no peer is disconnected for being slow. Pressure travels through blocking calls:

- **Agent slow to read PDs**: the socket fills, `Flush` or `SendPD` blocks, the sender stops calling `Issue`, and that agent's PDs become overdue in its heap. Other agents are not affected.
- **Capturer slow**: the FIE queue fills, the receivers block, the agents' sockets fill, and the agents stop producing.
- **Scheduler busy**: `Insert` and `Update` block on the event queue. A large insert batch is applied in one go, which pauses issuance for all agents while it runs.

## 9. Failure behaviour

- **Agent process dies**: its kernel closes the socket, the receiver gets EOF at once, and the agent can reconnect immediately.
- **Agent host or network dies**: the connection lingers until keepalive (about a minute when idle) or the retransmission timeout (up to about 15 minutes with data in flight). A reconnect with the same ID is rejected until then; the agent retries with backoff.
- **Connection drops with PDs in flight**: those PDs are lost. There are no acknowledgements; each comes round again one period later.
- **Orchestrator stops**: PDs already sent and FIEs not yet received are lost. The schedule is in memory only and is not restored on restart.

## 10. Measured throughput

Measured on 2026-10-02 with 38 agents using the mock prober and 1M PDs, all on one 12-core machine. These are test-setup figures, not production guarantees.

| Setup | Result |
| --- | --- |
| Docker stack, orchestrator on 2 CPUs, 10 s period | 100k PDs/s held with no loss |
| Same, saturated (2 s period), before buffered sending | about 150k PDs/s |
| Local processes, 2 threads, saturated, before buffered sending | about 115k PDs/s, orchestrator near one full core |
| Local processes, 2 threads, saturated, with buffered sending | about 165k PDs/s, limited by the agents; orchestrator at about a third of one core |

Before buffered sending, 67% of the orchestrator's CPU was one `write` system call per PD. After it, the cost is about 20% of one core per 100k PDs/s, spread over capture, FIE reading and sending. The scheduler takes a few percent.

To profile, build a temporary binary that imports `net/http/pprof` and serves it on a local port, run it under load, and use `go tool pprof -top -cum http://127.0.0.1:<port>/debug/pprof/profile?seconds=20`.

## 11. Not implemented

Compared with the previous implementation: no FIE stream or event stream over HTTP, no event log, no metrics or pprof endpoint, no log level setting, no FIE filter policy, and no use of FIEs by the scheduler.

## 12. Repo map

| Path | Contents |
| --- | --- |
| `main.go` | flags, signal handling |
| `internal/retina/orchestrator.go` | `Config`, `Run`, `serveAgent`, capture loop |
| `internal/retina/agent_server.go` | agent listener and connection, handshake, CSV encoding |
| `internal/retina/scheduler.go` | scheduler, issuer, per-agent heap |
| `internal/retina/api_server.go` | PD insert endpoint |
| `internal/retina/capturer.go` | DuckDB capture |
| `internal/retina/types.go` | compact `PD` and `FIE` |
| `scripts/` | run script, bulk loaders, memory logger |
| `test/` | sample PD JSONL files |
