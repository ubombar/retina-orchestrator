bash
#!/usr/bin/env bash

set -euo pipefail

INTERVAL_SECONDS="${INTERVAL_SECONDS:-60}"
PID="${1:-$(pgrep -n retina-orchestrator || true)}"

if [[ -z "$PID" ]]; then
	echo "Could not find retina-orchestrator process." >&2
	exit 1
fi

if [[ ! -r "/proc/$PID/status" ]]; then
	echo "Cannot read /proc/$PID/status" >&2
	exit 1
fi

log() {
	printf '[%s] %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*"
}

bytes_to_mib() {
	awk -v bytes="$1" 'BEGIN { printf "%.2f", bytes / 1024 / 1024 }'
}

log "Starting memory monitoring"
log "PID: $PID"
log "Interval: ${INTERVAL_SECONDS}s"

while kill -0 "$PID" 2>/dev/null; do
	timestamp="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

	vmrss_kb="$(awk '/^VmRSS:/ {print $2}' "/proc/$PID/status")"
	vmhwm_kb="$(awk '/^VmHWM:/ {print $2}' "/proc/$PID/status")"
	vmswap_kb="$(awk '/^VmSwap:/ {print $2}' "/proc/$PID/status")"

	anonymous_kb=""
	private_dirty_kb=""
	referenced_kb=""

	if [[ -r "/proc/$PID/smaps_rollup" ]]; then
		anonymous_kb="$(awk '/^Anonymous:/ {print $2}' "/proc/$PID/smaps_rollup")"
		private_dirty_kb="$(awk '/^Private_Dirty:/ {print $2}' "/proc/$PID/smaps_rollup")"
		referenced_kb="$(awk '/^Referenced:/ {print $2}' "/proc/$PID/smaps_rollup")"
	fi

	go_heap_alloc=""
	go_heap_inuse=""
	go_heap_sys=""
	go_sys=""
	go_goroutines=""

	if metrics="$(curl -fsS --max-time 5 http://localhost:9312/metrics 2>/dev/null)"; then
		go_heap_alloc="$(awk '$1 == "go_memstats_heap_alloc_bytes" {print $2}' <<<"$metrics")"
		go_heap_inuse="$(awk '$1 == "go_memstats_heap_inuse_bytes" {print $2}' <<<"$metrics")"
		go_heap_sys="$(awk '$1 == "go_memstats_heap_sys_bytes" {print $2}' <<<"$metrics")"
		go_sys="$(awk '$1 == "go_memstats_sys_bytes" {print $2}' <<<"$metrics")"
		go_goroutines="$(awk '$1 == "go_goroutines" {print $2}' <<<"$metrics")"
	fi

	printf '%s' "$timestamp"
	printf ' rss_mib=%.2f' "$(awk -v kb="$vmrss_kb" 'BEGIN {print kb / 1024}')"
	printf ' hwm_mib=%.2f' "$(awk -v kb="$vmhwm_kb" 'BEGIN {print kb / 1024}')"
	printf ' swap_mib=%.2f' "$(awk -v kb="$vmswap_kb" 'BEGIN {print kb / 1024}')"

	if [[ -n "$anonymous_kb" ]]; then
		printf ' anonymous_mib=%.2f' "$(awk -v kb="$anonymous_kb" 'BEGIN {print kb / 1024}')"
		printf ' private_dirty_mib=%.2f' "$(awk -v kb="$private_dirty_kb" 'BEGIN {print kb / 1024}')"
		printf ' referenced_mib=%.2f' "$(awk -v kb="$referenced_kb" 'BEGIN {print kb / 1024}')"
	fi

	if [[ -n "$go_heap_alloc" ]]; then
		printf ' go_heap_alloc_mib=%s' "$(bytes_to_mib "$go_heap_alloc")"
		printf ' go_heap_inuse_mib=%s' "$(bytes_to_mib "$go_heap_inuse")"
		printf ' go_heap_sys_mib=%s' "$(bytes_to_mib "$go_heap_sys")"
		printf ' go_sys_mib=%s' "$(bytes_to_mib "$go_sys")"
		printf ' goroutines=%s' "$go_goroutines"
	fi

	printf '\n'

	sleep "$INTERVAL_SECONDS"
done

log "Process $PID exited; stopping memory monitoring"
