# Retina Orchestrator

Retina Orchestrator admits and schedules Probing Directives (PDs), dispatches
them to authenticated Retina agents, receives Forwarding Information Elements
(FIEs), and exposes FIE and scheduler-event streams over HTTP.

This checkout is a research branch. Its scheduler and operational flags may
differ from released Retina versions.

## Build and test

The module requires Go 1.26.5.

```bash
make build
make test
```

`make build` regenerates Swagger documentation, formats and lints the project,
then writes `./retina-orchestrator`. To compile without those extra steps:

```bash
go build -o retina-orchestrator .
```

## Running

`RETINA_SECRET` is the shared agent secret and is configured only through the
environment. Empty secrets disable authentication; production deployments
should set one.

```bash
RETINA_SECRET='replace-with-a-secret' ./retina-orchestrator \
  --agent-addr=0.0.0.0:50050 \
  --api-addr=0.0.0.0:8080 \
  --metrics-addr=0.0.0.0:9312 \
  --fie-filter-policy=any
```

Use `./retina-orchestrator --help` for the authoritative flag list. Important
groups are:

- Agent transport and buffering: `--agent-addr`, `--agent-buffer-length`,
  `--pd-queue-size`, `--pd-push-timeout`.
- HTTP streaming: `--api-addr`, `--ring-buffer-size`,
  `--stream-start-from-earliest`.
- Fixed-period research scheduler: all `--rr-*` options.
- DuckDB capture: all `--capturer-*` options. Set
  `--capturer-enabled=false` to disable capture.
- Events and observability: `--events-dir`, `--event-bus-size`,
  `--metrics-addr`, and `--log-level`.

Every flag has a corresponding `RETINA_*` environment default. Command-line
flags take precedence over environment variables.

## Agent wire protocol

Each agent maintains one bidirectional TCP connection. Authentication is the
existing newline-delimited JSON exchange. After successful authentication, the
connection switches to headerless CSV and every record ends in `\n`.

PD, orchestrator to agent:

```text
probing_directive_id,"destination_address",near_ttl,protocol_number,first_half_word,second_half_word
```

The final two values are ICMP/ICMPv6 correlation half-words or UDP source and
destination ports.

FIE, agent to orchestrator:

```text
probing_directive_id,unix_capture_timestamp,"near_address",near_capture_delta,"far_address",far_capture_delta
```

Addresses are always quoted. A missing near or far observation is encoded as
`"",0`. Deltas are non-negative whole seconds from the FIE production timestamp
to the corresponding received timestamp. Because the compact representation
does not carry a sent timestamp, the orchestrator reconstructs sent and received
timestamps as equal (the zero-RTT approximation).

The data-phase protocol is a coordinated cutover: it does not negotiate JSON
versus CSV. Agent and orchestrator versions must therefore be upgraded together.

## HTTP API

- `POST /api/v1/pds` bulk-admits PDs using JSON:
  `{"probing_directives":[...]}`. IDs supplied by clients are overwritten by
  scheduler-assigned IDs.
- `GET /api/v1/stream` streams sequenced FIEs as NDJSON.
- `GET /api/v1/sse` streams scheduler events as NDJSON. Despite the historical
  route name, it does not use Server-Sent Events framing.
- `GET /api/v1/swagger/` serves Swagger UI.

`scripts/bulk_push.sh` batches a PD JSONL file into calls to `POST /api/v1/pds`.
JSONL here is an HTTP input-file format and is unrelated to the CSV agent wire
protocol.

## Behavior and backpressure

- The research scheduler controls admission and reissuance periods.
- Each connected agent has a queue sized by `--pd-queue-size`.
- When that queue is full, dispatch waits up to `--pd-push-timeout`. A timeout,
  missing agent, or disconnect is counted by `pds_dropped_total` with a reason.
- `--fie-filter-policy=any|one|both` controls which received FIEs proceed to
  capture and HTTP streaming. Every received FIE still updates the scheduler.
- TCP keepalive detects unreachable peers. PD writes have a five-second
  deadline; FIE reads are intentionally unbounded while the connection is live.
- SIGINT and SIGTERM initiate graceful shutdown.

## Observability

Prometheus metrics and Go runtime profiles are exposed on `--metrics-addr`:

- `/metrics`
- `/debug/pprof/`

Metrics cover agent connections, authentication, PD dispatch/drop reasons,
FIE receipt and streaming, queue sizes, scheduler behavior, capture, and stream
lag. See `internal/orchestrator/metrics.go` for the exact definitions.

For implementation details and operational caveats, see [DOCS.md](DOCS.md).

## Development helpers

- `scripts/orch.sh` is an opinionated research configuration and writes capture
  output below `./captures/`.
- `scripts/bulk_push.sh` and `scripts/insert_pds.sh` load PD JSONL through HTTP.
- `scripts/mock_agent.sh` still implements the legacy JSON data phase and is not
  compatible with the current CSV protocol. Use the real Retina agent with
  `--prober-type=mock` for end-to-end testing without network probes.

## License

MIT License — see [LICENSE](LICENSE).
