#!/usr/bin/env bash
#
# Retina event stream capture.
#
# Connects only to the event stream. Events are appended verbatim into JSONL,
# rotated every ROTATE_EVERY records.
#
# The stream does not reconnect. If it ends, the capture shuts down explicitly.
#
#   ./capture.sh [output_root]
#
# Environment:
#   RETINA_SERVER_URL   base URL           (default: http://localhost:8080)
#   ROTATE_EVERY        event records/file (default: 1000000)
#
# Layout:
#   YYYYMMDD__HHMMSS/
#     events/events_000001.jsonl
#     capture.log

set -uo pipefail

SERVER_URL="${RETINA_SERVER_URL:-http://localhost:8080}"
ROTATE_EVERY="${ROTATE_EVERY:-1000000}"

OUTPUT_ROOT="${1:-./data/streams}"
RUN_DIR="${OUTPUT_ROOT}/$(date -u +'%Y%m%d__%H%M%S')"
EVENTS_DIR="${RUN_DIR}/events"
LOG_FILE="${RUN_DIR}/capture.log"

SSE_ENDPOINT="${SERVER_URL}/api/v1/sse"

log() {
	printf '%s [capture] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" |
		tee -a "$LOG_FILE" >&2
}

die() {
	printf '%s [capture] error: %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2
	exit 1
}

command -v curl >/dev/null 2>&1 || die "required tool not found: curl"

mkdir -p "$EVENTS_DIR" || die "cannot create ${EVENTS_DIR}"
: >"$LOG_FILE"

log "run directory ${RUN_DIR}"
log "event stream ${SSE_ENDPOINT}"
log "event rotate every ${ROTATE_EVERY} records"

EVENT_PID=""
SHUTTING_DOWN=0

capture_events() {
	trap 'exit 0' TERM INT

	local index=1
	local count=0
	local file

	file=$(printf '%s/events_%06d.jsonl' "$EVENTS_DIR" "$index")

	log "event stream connecting"

	while IFS= read -r line; do
		[[ -n "$line" ]] || continue

		printf '%s\n' "$line" >>"$file"
		count=$((count + 1))

		if [[ "$count" -ge "$ROTATE_EVERY" ]]; then
			index=$((index + 1))
			count=0
			file=$(printf '%s/events_%06d.jsonl' "$EVENTS_DIR" "$index")
		fi
	done < <(
		curl -sN --no-buffer "$SSE_ENDPOINT" 2>/dev/null
	)

	log "event stream ended"
}

shutdown() {
	[[ "$SHUTTING_DOWN" -eq 0 ]] || return 0
	SHUTTING_DOWN=1
	trap - INT TERM HUP EXIT

	log "stopping: ${1:-signal}"

	if [[ -n "$EVENT_PID" ]]; then
		kill -TERM "$EVENT_PID" 2>/dev/null || true
	fi

	local waited=0
	while [[ "$waited" -lt 200 ]]; do
		if [[ -z "$EVENT_PID" ]] || ! kill -0 "$EVENT_PID" 2>/dev/null; then
			break
		fi

		sleep 0.1
		waited=$((waited + 1))
	done

	[[ "$waited" -lt 200 ]] ||
		log "event stream did not exit within 20s, continuing"

	# curl is a child/grandchild of the capture process because of process
	# substitution, so terminate anything still attached to it.
	if [[ -n "$EVENT_PID" ]]; then
		pkill -TERM -P "$EVENT_PID" 2>/dev/null || true
	fi

	local event_rows event_files

	event_rows=$(
		cat "$EVENTS_DIR"/*.jsonl 2>/dev/null |
			wc -l
	)

	event_files=$(
		find "$EVENTS_DIR" -name '*.jsonl' -type f |
			wc -l
	)

	log "captured ${event_rows} events in ${event_files} file(s)"
	log "run directory ${RUN_DIR}"

	exit 0
}

trap shutdown INT TERM HUP

capture_events &
EVENT_PID=$!

log "capturing events, interrupt to stop"

while true; do
	if ! kill -0 "$EVENT_PID" 2>/dev/null; then
		shutdown "event stream ended"
	fi

	sleep 1
done
