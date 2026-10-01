#!/usr/bin/env bash
#
# Starts retina-orchestrator for a research campaign. Each start captures into
# a new timestamped directory, because the capturer refuses a non-empty one.

set -euo pipefail

log() {
	printf '[orch] %s\n' "$*" >&2
}

usage() {
	cat >&2 <<USAGE
Usage: $0

Env:
  RETINA_SECRET          shared secret agents authenticate with (required)
  STARTING_PERIOD        issuance period of every PD (default: 10s)
  MAX_ISSUANCE_COUNT     issuances per PD, 0 for indefinitely (default: 0)
USAGE
	exit 1
}

[[ $# -eq 0 ]] || usage

if [[ -z "${RETINA_SECRET:-}" ]]; then
	log "RETINA_SECRET is not set"
	usage
fi

STARTING_PERIOD="${STARTING_PERIOD:-10s}"
MAX_ISSUANCE_COUNT="${MAX_ISSUANCE_COUNT:-0}"
CAPTURE_DIR="./captures/$(date -u +%Y%m%d_%H%M%S)"

log "capturing into ${CAPTURE_DIR}"

exec ./retina-orchestrator \
	--agent-addr=":50050" \
	--agent-handshake-timeout=5s \
	--agent-keepalive-idle=30s \
	--agent-keepalive-interval=10s \
	--agent-keepalive-count=3 \
	--agent-write-buffer-size=65536 \
	--agent-flush-period=100ms \
	--api-addr=":8080" \
	--api-read-header-timeout=5s \
	--scheduler-starting-period="${STARTING_PERIOD}" \
	--scheduler-max-issuance-count="${MAX_ISSUANCE_COUNT}" \
	--scheduler-event-queue-size=1024 \
	--capturer-capture-dir="${CAPTURE_DIR}/fies" \
	--capturer-allow-non-empty-capture-dir=false \
	--capturer-rotation-interval=1h \
	--capturer-batch-size=100000 \
	--capturer-queue-size=200000 \
	--capturer-flush-period=1s
