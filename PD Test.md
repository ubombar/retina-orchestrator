# PD Bulk-Insert Load Test

Investigation into pushing a very large Probing Directive (PD) file into
`retina-orchestrator` via `/api/v1/pds` with `--rr-single-issuance=true`
(`IssueOnlyOnce`).

**Test file:** `test/pd_selected_39agents_20260928.jsonl` — 12,755,405 lines, 2.9 GB.

---

## 1. Static analysis findings

| # | Finding | Location |
|---|---|---|
| 1 | The README's `--pd-path` / `--issuance-rate` flags are stale — they don't exist in this branch. `/api/v1/pds` is the **only** ingestion path. | `README.md` vs `main.go` |
| 2 | `handleBulkInsert` decodes the entire request body into one `[]*api.ProbingDirective` before inserting anything. A single 2.9GB/12M-PD request would need to hold the whole decoded slice in memory at once. | `internal/orchestrator/api_server.go:219-223` |
| 3 | `Insert()` paces each admission via a token bucket at `--rr-admission-rate` (default 1000/s), busy-waiting (`for time.Now().Before(first) {}`) inside the commitment window (`--rr-busy-tolerance`, default 500µs). | `internal/orchestrator/research_scheduler.go:442-475` |
| 4 | Admitted PDs go into `s.insertCh` (buffer `--rr-insert-channel-size`, default 1024), drained by `drain()`, itself only called from `Next()`. | `research_scheduler.go:552-620` |
| 5 | A dedicated goroutine (`runScheduler`) continuously calls `scheduler.Next()` and `pdQueue.TryPush(pd.AgentID, pd)` **regardless of whether any agent is connected** — this is what keeps `insertCh` draining even with zero agents. | `internal/orchestrator/orchestrator.go:286-308` |
| 6 | `TryPush` silently drops a PD if no agent queue exists for its `agent_id` — logged at **DEBUG only** ("PD dropped: no queue for agent"), **no metric incremented**. | `orchestrator.go:302-307` |
| 7 | With `SingleIssuance=true`, `retire()` keeps the PD's record in `s.records` forever after its one issuance (for late-FIE bookkeeping) — this memory is never released for the life of the process. | `research_scheduler.go:812-825` |
| 8 | `insertAfterHanler` emits `PDBulkInsertionEvent` with the **full requested slice**, even if the insert loop broke early after a partial failure — the event overstates what actually succeeded, and (if `--events-dir` is set) re-marshals the whole batch to JSON a second time. | `api_server.go:238`, `orchestrator.go:430-435` |
| 9 | No idempotency: PD IDs are an atomic counter with an explicit "duplicate detection is unnecessary" design comment. A retried/aborted request re-inserts everything as new PDs. | `research_scheduler.go:697-701` |
| 10 | No `ReadTimeout`/`WriteTimeout`/`MaxBytesReader` on the HTTP server — only `ReadHeaderTimeout` (header-only). Go itself won't cut a long-running request, but an external proxy/LB/client almost certainly will. | `api_server.go:60-90` |

---

## 2. Live test setup

```bash
go build -o retina-orchestrator .

RETINA_SECRET=demo-secret RETINA_CAPTURER_ENABLED=false ./retina-orchestrator \
  --api-addr=localhost:8080 --agent-addr=localhost:50050 \
  --log-level=info --rr-single-issuance=true [--rr-admission-rate=50000]
```

`scripts/bulk_push.sh <file> [batch_size=10000] [server_url]` was written to chunk
the file with `split -l` (one pass, avoids O(n²) reseeking) and POST each chunk as
`{"probing_directives": [...]}`, checking `inserted_count`/HTTP status per batch.

No agents were connected for either run (`retina_orchestrator_agents_connected: 0`
throughout).

---

## 3. Run 1 — default admission rate (1000/s), partial (80,000 PDs)

- Every 10,000-line batch took ~10s, exactly matching the 1000/s admission-rate
  token bucket — confirms the admission rate, not agent throughput, is the
  ingestion-side bottleneck.
- Projected full-file time at this rate: **~3.5 hours**.
- CPU stayed near 0% (the 500µs busy-wait window is small relative to a 1ms
  admission interval at this rate).
- A targeted test confirmed finding #6 directly: inserted 1 PD for a
  nonexistent `agent_id`, got `inserted_count: 1` back (200 OK), and ~2s later
  (its issuance time) it was silently dropped —
  `"PD dropped: no queue for agent"` appeared only at `--log-level=debug`;
  `pds_skipped_total` and `pds_total` never moved from 0.

## 4. Run 2 — accelerated admission rate (50,000/s), full file (12,755,405 PDs)

**Result: 12,755,405 / 12,755,405 lines sent, 1,276/1,276 batches HTTP 200,
`inserted_count` matched `sent` on every batch. Total time: 500s (8m20s),
~25,500 PDs/sec average, no slowdown near the end.**

### Throughput

The configured 50,000/s ceiling was never actually reached — observed interval
rate was ~16,000-23,000/s. At this rate the token bucket is no longer the
bottleneck; the per-batch HTTP/JSON round trip (`jq` serialization + request +
server-side decode of 10,000 objects) is. CPU stayed at ~0% average — the
server finishes each batch well inside the gap between client requests, so it
isn't burning cycles in the busy-wait branch at the rate the client could
actually sustain.

### Memory — the number to plan around

| sample | elapsed | PDs inserted | RSS | heap_alloc |
|---|---|---|---|---|
| mid-run 1 | 2:37 | 3,580,000 | 2.88 GB | 3.04 GB |
| mid-run 2 | 4:09 | 5,050,000 | 3.98 GB | 3.49 GB |
| completion | 9:50 | 12,755,405 | 1.06 GB* | 9.43 GB |

Marginal cost from the two mid-run samples: **~751 bytes/PD**, extrapolating to
**~9.6 GB** for the full file. The actual `heap_inuse_bytes` at completion was
**9.85 GB** — a very close match. This confirms finding #7: because
`SingleIssuance=true` never frees a PD's record after it fires, this is a
**permanent floor**, not a transient decode spike (chunking via
`bulk_push.sh` already ruled out the transient one-shot-decode risk from
finding #2).

*\*Anomaly:* `process_resident_memory_bytes` **dropped** to 1.06 GB by
completion despite `heap_alloc`/`heap_inuse` roughly tripling over the same
window. This contradicts the heap trend and is most likely either macOS
memory compression of PD records that are written once and never touched
again (nothing calls `update()` since no FIEs ever arrive with zero agents
connected), or a known limitation of `client_golang`'s process collector on
Darwin. **Treat `heap_alloc`/`heap_inuse` as the reliable figures for capacity
planning on this platform**, not this RSS metric. (Our sandboxed `ps`/`top`
couldn't read RSS at all here — "requires entitlement" — an OS-native
`footprint <pid>` or Activity Monitor check would give a trustworthy
cross-check.)

System had 32 GB total RAM; ~10 GB for this process is comfortable here but
would be tight-to-insufficient on an 8-16 GB host holding this many
single-issuance PDs.

### The result that matters most: nothing was actually delivered

`agents_connected` was 0 for the entire run, and `pds_skipped_total` stayed 0
throughout (consistent with finding #6: that metric doesn't track the
silent-drop path at all). Given what Run 1 already proved directly, essentially
every one of these 12.75M single-issuance PDs will be (or already has been,
for the earliest-admitted ones) popped by the scheduler at its issuance time
and silently discarded, with no visible signal short of `--log-level=debug`.

**"12,755,405/12,755,405 sent" reflects successful admission into the
scheduler only — it is not evidence that any probing happened.**

---

## 5. Recommendations

1. **Never push the whole file as one request.** Chunk it (`scripts/bulk_push.sh`
   does this) — this alone avoids the worst-case single-decode memory spike.
2. **Budget ~750-800 bytes of permanent RAM per PD** when `--rr-single-issuance`
   is on. For 12M+ PDs, plan for ~10GB+ that will not be released until the
   orchestrator restarts.
3. **Raise `--rr-admission-rate`** to avoid the token bucket being the
   bottleneck, but expect the real ceiling to shift to per-batch HTTP/JSON
   overhead once the bucket is fast enough — bigger batches or a persistent
   connection would help more than raising the rate further.
4. **Connect every agent referenced in the file before/while pushing.**
   Otherwise PDs for missing/late agents are silently and permanently dropped
   (especially costly under single-issuance, where there's no second chance).
5. **Run with `--log-level=debug`** (or, better, get a real "PD delivered /
   PD dropped" metric added) if you need to know whether delivery actually
   happened — today there is no INFO-level or Prometheus signal for this at
   all.
6. **Treat a 200 + matching `inserted_count` as "accepted," not "delivered."**
   The API gives no way to confirm actual dispatch to an agent.
