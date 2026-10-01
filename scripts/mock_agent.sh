#!/usr/bin/env bash
#
# Legacy mock Retina agent.
#
# This script speaks the former all-JSON protocol and is not compatible with
# the current compact-CSV data phase. Use the real retina-agent binary with
# --prober-type=mock for current end-to-end testing.
#
# Environment:
#   RETINA_SECRET        agent secret (required)
#   RETINA_AGENT_ID           agent identifier          (default: agent_1)
#   RETINA_SERVER_HOST        orchestrator host         (default: localhost)
#   RETINA_SERVER_PORT        orchestrator port         (default: 50050)
#   RETINA_SOURCE_ADDRESS     agent egress address      (default: 192.0.2.1)
#   RETINA_NEAR_REPLY_ADDRESS reply address at NearTTL  (default: 203.0.113.1)
#   RETINA_FAR_REPLY_ADDRESS  reply address at NearTTL+1 (default: 203.0.113.2)

set -euo pipefail

AGENT_ID="${RETINA_AGENT_ID:-agent_1}"
AGENT_SECRET="${RETINA_SECRET:-}"
SERVER_HOST="${RETINA_SERVER_HOST:-localhost}"
SERVER_PORT="${RETINA_SERVER_PORT:-50050}"
SOURCE_ADDRESS="${RETINA_SOURCE_ADDRESS:-192.0.2.1}"
NEAR_REPLY_ADDRESS="${RETINA_NEAR_REPLY_ADDRESS:-203.0.113.1}"
FAR_REPLY_ADDRESS="${RETINA_FAR_REPLY_ADDRESS:-203.0.113.2}"

readonly HANDSHAKE_TIMEOUT=5

log() {
	printf '%s [mock-agent] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2
}

die() {
	log "error: $*"
	exit 1
}

# ---------------------------------------------------------------------------
# Minimal JSON field extraction.
#
# The orchestrator emits directives with Go's encoding/json, which produces
# compact, deterministic output with no interior whitespace, so a targeted
# match on each field is sufficient here. This avoids a jq dependency on the
# probing agents.
# ---------------------------------------------------------------------------

# json_number <json> <field> <fallback>
json_number() {
	local json="$1" field="$2" fallback="$3"
	if [[ "$json" =~ \"$field\"[[:space:]]*:[[:space:]]*([0-9]+) ]]; then
		printf '%s' "${BASH_REMATCH[1]}"
	else
		printf '%s' "$fallback"
	fi
}

# json_string <json> <field> <fallback>
json_string() {
	local json="$1" field="$2" fallback="$3"
	if [[ "$json" =~ \"$field\"[[:space:]]*:[[:space:]]*\"([^\"]*)\" ]]; then
		printf '%s' "${BASH_REMATCH[1]}"
	else
		printf '%s' "$fallback"
	fi
}

# json_bool_true <json> <field> -> exit 0 if the field is literally true
json_bool_true() {
	local json="$1" field="$2"
	[[ "$json" =~ \"$field\"[[:space:]]*:[[:space:]]*true ]]
}

# ---------------------------------------------------------------------------
# Timestamps
#
# Resolved once at startup so the per-directive path does not pay for flavour
# detection. Both GNU coreutils and BSD/macOS date are supported.
# ---------------------------------------------------------------------------

DATE_FLAVOUR=""
detect_date_flavour() {
	if date -u -d @0 +%s >/dev/null 2>&1; then
		DATE_FLAVOUR="gnu"
	elif date -u -r 0 +%s >/dev/null 2>&1; then
		DATE_FLAVOUR="bsd"
	else
		die "no supported date(1) implementation found"
	fi
}

# rfc3339 <epoch_seconds>
rfc3339() {
	local epoch="$1"
	if [[ "$DATE_FLAVOUR" == "gnu" ]]; then
		date -u -d "@$epoch" +"%Y-%m-%dT%H:%M:%SZ"
	else
		date -u -r "$epoch" +"%Y-%m-%dT%H:%M:%SZ"
	fi
}

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

[[ -n "$AGENT_SECRET" ]] || die "RETINA_SECRET is not set"
detect_date_flavour

# A failed redirection on a bare `exec` returns non-zero rather than exiting,
# so the guard is enough. Do not probe the connection first: the orchestrator
# accepts one connection per agent and a throwaway connect consumes it.
if ! exec 3<>"/dev/tcp/${SERVER_HOST}/${SERVER_PORT}"; then
	die "cannot connect to ${SERVER_HOST}:${SERVER_PORT}"
fi

cleanup() {
	exec 3<&- || true
	exec 3>&- || true
}
trap cleanup EXIT
trap 'log "interrupted"; exit 130' INT TERM

log "connected to ${SERVER_HOST}:${SERVER_PORT}"

# ---------------------------------------------------------------------------
# Handshake
# ---------------------------------------------------------------------------

printf '{"agent_id":"%s","secret":"%s"}\n' "$AGENT_ID" "$AGENT_SECRET" >&3
log "sent auth request as ${AGENT_ID}"

auth_response=""
if ! IFS= read -r -t "$HANDSHAKE_TIMEOUT" -u 3 auth_response; then
	die "no auth response within ${HANDSHAKE_TIMEOUT}s"
fi

if json_bool_true "$auth_response" "authenticated"; then
	log "authenticated"
else
	die "rejected: $(json_string "$auth_response" "message" "no message")"
fi

# ---------------------------------------------------------------------------
# Main loop
#
# The read is intentionally unbounded: the orchestrator paces directives and a
# read timeout here would tear the connection down during any quiet period.
# The loop ends on EOF, i.e. when the orchestrator closes the connection.
# ---------------------------------------------------------------------------

log "waiting for probing directives"

directive_count=0

while IFS= read -r -u 3 directive; do
	[[ -n "$directive" ]] || continue

	directive_count=$((directive_count + 1))

	directive_id=$(json_number "$directive" "probing_directive_id" 0)
	ip_version=$(json_number "$directive" "ip_version" 4)
	protocol=$(json_number "$directive" "protocol" 17)
	near_ttl=$(json_number "$directive" "near_ttl" 1)
	far_ttl=$((near_ttl + 1))
	destination=$(json_string "$directive" "destination_address" "0.0.0.0")

	# production < sent < received, one second apart.
	now=$(date -u +%s)
	production_ts=$(rfc3339 $((now - 2)))
	sent_ts=$(rfc3339 $((now - 1)))
	received_ts=$(rfc3339 "$now")

	printf '{"agent":{"agent_id":"%s"},"probing_directive_id":%s,"ip_version":%s,"protocol":%s,"source_address":"%s","destination_address":"%s","near_info":{"probe_ttl":%s,"reply_address":"%s","sent_timestamp":"%s","received_timestamp":"%s"},"far_info":{"probe_ttl":%s,"reply_address":"%s","sent_timestamp":"%s","received_timestamp":"%s"},"production_timestamp":"%s"}\n' \
		"$AGENT_ID" \
		"$directive_id" \
		"$ip_version" \
		"$protocol" \
		"$SOURCE_ADDRESS" \
		"$destination" \
		"$near_ttl" "$NEAR_REPLY_ADDRESS" "$sent_ts" "$received_ts" \
		"$far_ttl" "$FAR_REPLY_ADDRESS" "$sent_ts" "$received_ts" \
		"$production_ts" >&3

	log "pd ${directive_id} ttl ${near_ttl}/${far_ttl} dst ${destination} -> fie sent"
done

log "orchestrator closed the connection after ${directive_count} directive(s)"
