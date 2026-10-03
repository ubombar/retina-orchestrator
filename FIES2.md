# The fies2 capture formats

fies2 is a family of capture formats for the FIEs the Retina orchestrator receives. Every member shares the layout below; the letter after the 2 says which timing information a file holds. Only **fies2a** exists so far; the orchestrator writes it since `research-v1.6.0` (`internal/retina/capturer.go`). It replaces the earlier `fies` format: one DuckDB file per interval in arrival order, with a packed `time_deltas` column, about 13 bytes per FIE against 1.86 for fies2a on a real hour of FIEs.

## What every fies2 format shares

- **One Parquet file per UTC rotation interval** (one hour by default), named `<format>-<interval start>.parquet`, for example `fies2a-20261003T110000Z.parquet`. The interval start is in the name and in the file's metadata.
- **Rows sorted by `(pd_id, capture_second)`.** All rows of one PD for the interval are next to each other. This is what makes the file small: addresses and IDs that repeat from one issuance to the next become runs that compress to almost nothing.
- **zstd compression**, Parquet row groups of about 1M rows (`--capturer-row-group-size`). Each row group's min and max `pd_id` let a reader skip row groups when filtering by PD ID.
- **Reply addresses as BLOBs of 4 bytes (IPv4) or 16 bytes (IPv6)**, in the same column. IPv4-mapped IPv6 addresses are stored as 4 bytes. A missing reply is `NULL`, not an empty BLOB.
- **No row order.** The order in which FIEs arrived is not kept, not even within a second. Time is only what the timing columns say.
- **Self-describing.** The Parquet key-value metadata holds `retina.fies.format` (for example `2a`), `retina.fies.interval_start` (RFC 3339) and `retina.fies.interval_seconds`.
- Fields that the PD implies are not stored: agent, source and destination address, IP version, protocol, ports, near and far TTL. Join with the PDs to get them.

## fies2a

| Column | Type | Meaning |
| --- | --- | --- |
| `pd_id` | UINTEGER | the PD the FIE answers |
| `capture_second` | USMALLINT | seconds from the interval start to when the orchestrator's capturer received the FIE, on the **orchestrator's clock** |
| `near_reply_addr` | BLOB, NULL | address that answered the near probe, 4 or 16 bytes |
| `far_reply_addr` | BLOB, NULL | same for the far probe |
| `near_reply_age_s` | UTINYINT, NULL | seconds from caracal capturing the near reply to the agent building the FIE; NULL when there was no reply; 255 means 255 or more |
| `far_reply_age_s` | UTINYINT, NULL | same for the far reply |
| `fie_transit_s` | UTINYINT, NULL | seconds from the agent building the FIE to the capturer receiving it (agent to orchestrator delay, including backpressure); NULL when the agent's clock is ahead of the orchestrator's; 255 means 255 or more |

Absolute times, to within a few seconds because every term is rounded:

- FIE received by the orchestrator: `interval_start + capture_second`
- FIE built by the agent: that minus `fie_transit_s`
- reply captured by caracal: that minus `near_reply_age_s` (or `far_reply_age_s`)

### Where the values come from

The agent sends one CSV line per FIE: `pd_id,capture_unix,"near",near_delta,"far",far_delta` (retina-agent `research-v1.1.0`, `orchestrator_client.go`). `capture_unix` and both deltas are whole seconds. fies2a stores exactly that, plus the orchestrator's own receive time. Compared with the earlier `fies` format, it drops the two "sent" deltas, which the orchestrator only ever filled with copies of the received ones, and stores the remaining three as separate columns instead of a packed integer.

## Assumptions

- **Seconds are fine enough.** Every time in fies2a is whole seconds. Probes time out after 2 s, so the reply ages are almost always 0 to 3.
- **Agent and orchestrator clocks agree to about a second.** `fie_transit_s` mixes transport delay and clock offset; a negative value (agent ahead) is stored as NULL.
- **A PD ID identifies one PD for the whole interval.** IDs are not reused within an interval.
- **Row order carries no information.** Analysis that needs ordering uses the time columns.
- **An interval is at most 18 hours,** so that `capture_second` fits in 16 bits.
- **The analysis happens elsewhere** (ClickHouse). fies2a is optimised for disk size, for transfer, and for two reads: all rows of a sample of PD IDs, and a time window at interval granularity.

## Limitations

- **No RTT.** The agent does not send probe send times and rounds reply times to seconds, so round-trip times cannot be computed. A future fies2b would need the agent and the orchestrator to change their FIE line (a shared contract) to carry send and reply times in microseconds.
- **Coarse timing.** Reconstructed absolute times are only precise to a few seconds, and the reply ages are measured against the agent's FIE time, not the probe's send time. Ages of 3 s exist although the probe timeout is 2 s, because of rounding and because the agent builds FIEs up to about 300 ms after the timeout.
- **A file only exists once its interval is over.** The capturer stages FIEs in arrival order in `fies2a-<start>.staging.duckdb` (DuckDB, about 13 bytes per FIE) and sorts it into Parquet after rotation. Until then the data sits in the staging file; a reader wanting live data has to read the staging file.
- **Finalizing costs CPU, memory and disk once per interval.** The sort runs in the background with a DuckDB memory limit (`--capturer-finalize-memory-limit`, 2 GB by default) and spills to disk beyond it. During the sort the staging file, the spill files and the new Parquet file exist at the same time, so disk headroom of about three times the staging size of one interval is needed.
- **The staging file needs a memory limit.** DuckDB otherwise sizes its buffers to 80% of the machine's memory; `--capturer-staging-memory-limit` (1 GB by default) caps it.
- **A crash leaves a staging file behind.** It holds every FIE received so far and is finalized when the orchestrator starts again in the same directory with `--capturer-allow-non-empty-capture-dir`. The Parquet file is written under a `.tmp` name and renamed when complete, so a partial Parquet file is never mistaken for a finished one.
- **Time filtering within a file is coarse.** Rows are sorted by PD first, so every row group spans the whole interval and readers cannot skip row groups by time. Filtering by time works at interval granularity through file names.
- **Saturating values.** Ages and transit above 254 s are stored as 255.
- **PD order is ID order.** If PD IDs are assigned so that one agent's PDs are contiguous, a PD ID filter also tends to select by agent; otherwise row groups mix agents, which only affects how many row groups a filter reads.

## Future letters

| Format | Idea |
| --- | --- |
| fies2b | probe send time and reply times in microseconds, so RTTs; needs the wire format change in agent and orchestrator |
| fies2c… | free, for example millisecond timing, or a different sort order (by address) if reads by router matter more than reads by PD |
