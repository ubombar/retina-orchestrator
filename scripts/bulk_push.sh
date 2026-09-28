#!/usr/bin/env bash
set -uo pipefail

# Pushes a large JSONL file of Probing Directives into retina-orchestrator's
# /api/v1/pds bulk-insert endpoint in fixed-size batches, so a single request
# never has to hold the entire file in memory or in flight at once.
#
# Usage: ./bulk_push.sh <jsonl_file> [batch_size=10000] [server_url=http://localhost:8080]
#
# Env overrides:
#   MAX_TIME   per-batch curl timeout in seconds (default: 60)

FILE="${1:?Usage: $0 <jsonl_file> [batch_size=10000] [server_url=http://localhost:8080]}"
BATCH_SIZE="${2:-10000}"
SERVER_URL="${3:-http://localhost:8080}"
MAX_TIME="${MAX_TIME:-60}"

[ -f "$FILE" ] || { echo "File not found: $FILE" >&2; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "jq is required" >&2; exit 1; }

TOTAL_LINES=$(wc -l < "$FILE" | tr -d ' ')
TMPDIR=$(mktemp -d "${TMPDIR:-/tmp}/bulk_push.XXXXXX")
cleanup() { rm -rf "$TMPDIR"; }
trap cleanup EXIT

echo "Splitting $FILE ($TOTAL_LINES lines) into batches of $BATCH_SIZE lines..."
# Pre-splitting to disk (one pass) avoids re-scanning the file from the start
# for every batch, which is what naive `sed -n 'a,bp'`/`tail +N` loops would do.
split -l "$BATCH_SIZE" -a 5 "$FILE" "$TMPDIR/chunk_"

CHUNKS=("$TMPDIR"/chunk_*)
TOTAL_CHUNKS=${#CHUNKS[@]}
SENT=0
CHUNK_NUM=0
START_TIME=$(date +%s)

for chunk in "${CHUNKS[@]}"; do
	CHUNK_NUM=$((CHUNK_NUM + 1))
	LINES_IN_CHUNK=$(wc -l < "$chunk" | tr -d ' ')

	BATCH_START=$(date +%s)
	RESPONSE=$(jq -s '{probing_directives: .}' "$chunk" | \
		curl -sS -X POST "$SERVER_URL/api/v1/pds" \
			-H "Content-Type: application/json" \
			--data-binary @- \
			--max-time "$MAX_TIME" \
			-w $'\n%{http_code}' 2>&1)
	CURL_EXIT=$?
	BATCH_ELAPSED=$(( $(date +%s) - BATCH_START ))

	HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
	BODY=$(echo "$RESPONSE" | sed '$d')

	if [ "$CURL_EXIT" -eq 28 ]; then
		echo "[chunk $CHUNK_NUM/$TOTAL_CHUNKS] TIMED OUT after ${MAX_TIME}s (curl exit 28)." >&2
		echo "  This means the request did not complete within --max-time. With no agents" >&2
		echo "  connected (or agents slower than the insertion rate), the scheduler's" >&2
		echo "  internal insert channel fills up and Insert() blocks indefinitely -- the" >&2
		echo "  server never returns a response, successful or not." >&2
		echo "  Sent so far (across completed batches): $SENT/$TOTAL_LINES" >&2
		exit 28
	fi

	if [ "$CURL_EXIT" -ne 0 ]; then
		echo "[chunk $CHUNK_NUM/$TOTAL_CHUNKS] curl failed (exit $CURL_EXIT): $RESPONSE" >&2
		exit "$CURL_EXIT"
	fi

	INSERTED_COUNT=$(echo "$BODY" | jq -r '.inserted_count // "?"' 2>/dev/null)
	SENT=$((SENT + LINES_IN_CHUNK))

	echo "[chunk $CHUNK_NUM/$TOTAL_CHUNKS] sent=$LINES_IN_CHUNK inserted=$INSERTED_COUNT http=$HTTP_CODE elapsed=${BATCH_ELAPSED}s total_sent=$SENT/$TOTAL_LINES"

	if [ "$HTTP_CODE" != "200" ]; then
		echo "Stopping: batch $CHUNK_NUM returned http $HTTP_CODE: $BODY" >&2
		exit 1
	fi

	rm -f "$chunk"
done

TOTAL_ELAPSED=$(( $(date +%s) - START_TIME ))
echo "Done. Sent $SENT/$TOTAL_LINES lines across $TOTAL_CHUNKS batches in ${TOTAL_ELAPSED}s."
