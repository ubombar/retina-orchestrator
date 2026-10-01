#!/usr/bin/env bash
set -euo pipefail

# Sends a whole PD JSONL file to the orchestrator in one request.
# Usage: ./insert_pds.sh path/to/pds.jsonl [server_url]
JSONL_FILE="${1:?Usage: $0 <jsonl_file> [server_url]}"
SERVER_URL="${2:-http://localhost:8080}"

if [ ! -f "$JSONL_FILE" ]; then
	echo "File not found: $JSONL_FILE" >&2
	exit 1
fi

echo "Sending $(wc -l <"$JSONL_FILE") entries from $JSONL_FILE ..."

jq -s '{probing_directives: .}' "$JSONL_FILE" |
	curl -X POST "$SERVER_URL/api/v1/pds" \
		-H "Content-Type: application/json" \
		--data-binary @- \
		--max-time 300 \
		-w "\nHTTP status: %{http_code}, time: %{time_total}s\n"
