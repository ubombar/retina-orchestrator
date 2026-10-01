#!/usr/bin/env bash

if [[ -z "${RETINA_SECRET:-}" ]]; then
	echo "Error: RETINA_SECRET is not declared." >&2
	exit 1
fi

CAPTURE_DIR="./captures/$(date -u +%Y%m%d_%H%M%S)"

# Research scheduler configuration
RR_STARTING_PERIOD=10s
RR_MAX_ISSUANCE_COUNT=0

./retina-orchestrator \
	--api-addr=":8080" \
	--agent-addr=":50050" \
	--agent-buffer-length=8192 \
	--pd-queue-size=100 \
	--ring-buffer-size=5000 \
	--event-bus-size=1000 \
	--events-dir="${CAPTURE_DIR}/events" \
	--api-read-header-timeout=5s \
	--fie-filter-policy="any" \
	--log-level="info" \
	--metrics-addr=":9312" \
	--stream-start-from-earliest=true \
	--capturer-enabled=true \
	--capturer-allow-non-empty-capture-dir=false \
	--capturer-batch-size=100000 \
	--capturer-capture-dir="${CAPTURE_DIR}/fies" \
	--capturer-rotation-interval=1h \
	--capturer-channel-size=10000 \
	--capturer-flush-period=1s \
	--rr-starting-period="${RR_STARTING_PERIOD}" \
	--rr-max-issuance-count="${RR_MAX_ISSUANCE_COUNT}" \
	--rr-max-events-per-pass=64 \
	--rr-event-channel-size=1024
