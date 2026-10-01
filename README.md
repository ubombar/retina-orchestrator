# Retina Orchestrator

Retina Orchestrator accepts Probing Directives (PDs) over HTTP, schedules them, sends them to authenticated Retina agents, and captures the Forwarding Information Elements (FIEs) the agents send back into DuckDB files.

This checkout is a research branch. Its code lives in `internal/retina` and its flags differ from released Retina versions.

## Build and test

The module requires Go 1.26.5 and cgo, which the DuckDB capturer needs.

```bash
make build
```

```bash
make test
```

`make build` formats and lints the project, then writes `./retina-orchestrator`. To compile without those steps:

```bash
go build -o retina-orchestrator .
```

The `Dockerfile` builds the same binary into an image that runs as the unprivileged user `retina` in `/app`.

## Running

`RETINA_SECRET` is the shared agent secret and is read from the environment only. An empty secret accepts agents that send an empty secret.

```bash
RETINA_SECRET='replace-with-a-secret' ./retina-orchestrator \
  --agent-addr=:50050 \
  --api-addr=:8080 \
  --capturer-capture-dir=./capture
```

`./retina-orchestrator --help` is the authoritative flag list. All other settings come from flags; there are no environment fallbacks.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--agent-addr` | `localhost:50050` | Listening address for agent connections |
| `--agent-handshake-timeout` | `5s` | Time an agent has to complete the handshake |
| `--agent-keepalive-idle` | `30s` | Idle time before TCP keepalive probes are sent |
| `--agent-keepalive-interval` | `10s` | Time between keepalive probes |
| `--agent-keepalive-count` | `3` | Unanswered probes before the connection is closed |
| `--agent-write-buffer-size` | `65536` | Per-agent buffer PDs are written to before being sent |
| `--agent-flush-period` | `100ms` | Interval at which buffered PDs are sent; a PD is delayed by at most this long |
| `--api-addr` | `localhost:8080` | Listening address for the HTTP API |
| `--api-read-header-timeout` | `5s` | Timeout for reading HTTP request headers |
| `--scheduler-starting-period` | `10s` | Issuance period of every PD, and the delay before its first issuance |
| `--scheduler-max-issuance-count` | `0` | Issuances per PD before it leaves the schedule; 0 is indefinitely |
| `--scheduler-event-queue-size` | `1024` | Size of the scheduler event queue |
| `--capturer-capture-dir` | `./capture` | Directory for the DuckDB capture files |
| `--capturer-allow-non-empty-capture-dir` | `false` | Allow starting with files already in the capture directory |
| `--capturer-rotation-interval` | `1h` | Time span covered by one capture file, at most 18h |
| `--capturer-batch-size` | `100000` | FIEs appended before the capture file is flushed |
| `--capturer-queue-size` | `200000` | Received FIEs that may wait to be captured |
| `--capturer-flush-period` | `1s` | Interval between periodic flushes of the capture file |

## Agent wire protocol

Each agent keeps one bidirectional TCP connection. The handshake is one JSON line each way (`AuthRequest`, `AuthResponse` from `retina-commons`). After it, the connection carries headerless CSV, one record per line.

PD, orchestrator to agent:

```text
probing_directive_id,"destination_address",near_ttl,protocol_number,first_half_word,second_half_word
```

The last two values are the ICMP or ICMPv6 correlation half-words, or the UDP source and destination ports.

FIE, agent to orchestrator:

```text
probing_directive_id,unix_capture_timestamp,"near_address",near_capture_delta,"far_address",far_capture_delta
```

Addresses are always quoted. A missing near or far reply is `"",0`. A delta is the whole seconds between that reply and the capture timestamp. The record carries no sent timestamps.

PD IDs are 32-bit. There is no protocol negotiation, so agent and orchestrator must be upgraded together. The matching agent is `retina-agent` on `research-v1.0.0`, and both use `retina-commons` on `research-v1.0.0`.

## HTTP API

`POST /api/v1/pds` inserts PDs. The body is `{"probing_directives":[...]}` with `ProbingDirective` objects from `retina-commons`. The whole request is inserted as one batch: if any PD is invalid, the request is rejected with 400 and nothing is inserted. IDs supplied by the client are ignored.

The response is `{"inserted_count":N,"first_id":X}`. The inserted PDs have consecutive IDs starting at `first_id`, in request order.

`scripts/bulk_push.sh` splits a PD JSONL file into such requests. There is no other endpoint.

## Behaviour

- A PD is first issued one starting period after it is inserted, and again one period after each issuance.
- Each agent pulls its own PDs. An agent that is slow or not reading delays only its own PDs; it is never disconnected for that, and nothing is dropped. Its overdue PDs go out in order, each once, when it catches up.
- PDs of an agent that is not connected wait in the schedule and are issued when it connects.
- A second connection with an agent ID that is already connected is closed after the handshake.
- Every received FIE is captured. When the capture queue is full, the orchestrator stops reading from the agents, which slows them down.
- A capturer error stops the orchestrator.
- SIGINT and SIGTERM shut down cleanly: queued FIEs are written and the capture file is closed.

See [DOCS.md](DOCS.md) for how it works inside, and for measured throughput.

## Scripts

- `scripts/orch.sh` starts the orchestrator with the research settings and captures into a new directory under `./captures/` on each start.
- `scripts/bulk_push.sh` and `scripts/insert_pds.sh` load a PD JSONL file through the HTTP API.
- `scripts/memlog.sh` logs the process memory of a running orchestrator.

For end-to-end tests without network probes, run the real agent with `--prober-type=mock`.

## License

MIT License — see [LICENSE](LICENSE).
