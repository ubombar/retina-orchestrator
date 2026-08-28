#!/usr/bin/env bash

if [[ -z "${RETINA_SECRET:-}" ]]; then
	echo "Error: RETINA_SECRET is not declared." >&2
	exit 1
fi

CAPTURE_DIR="./captures/$(date -u +%Y%m%d_%H%M%S)"

# RR configuration
RR_LEARNING_RATE=0.5
RR_SAMPLING_WIDTH=0.0
RR_MIN_ISSUANCE_PERIOD=1s
RR_MAX_ISSUANCE_PERIOD=12h
RR_STARTING_ISSUANCE_PERIOD=10s
RR_DISABLE_RESPONSIBLE_PROBING=true
RR_DISABLE_STALENESS=true

./retina-orchestrator \
	--api-addr=":8080" \
	--agent-addr=":50050" \
	--agent-buffer-length=8192 \
	--pd-queue-size=100 \
	--ring-buffer-size=10000 \
	--event-bus-size=1048576 \
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
	--capturer-rotation-interval=6h \
	--capturer-channel-size=200000 \
	--capturer-flush-period=1s \
	--rr-seed=42 \
	--rr-learning-rate="${RR_LEARNING_RATE}" \
	--rr-sampling-width="${RR_SAMPLING_WIDTH}" \
	--rr-impact-threshold=1.0 \
	--rr-fie-history-capacity=6 \
	--rr-min-issuance-period="${RR_MIN_ISSUANCE_PERIOD}" \
	--rr-max-issuance-period="${RR_MAX_ISSUANCE_PERIOD}" \
	--rr-admission-rate=1000 \
	--rr-starting-issuance-period="${RR_STARTING_ISSUANCE_PERIOD}" \
	--rr-status-interval=1m \
	--rr-period-dump-interval=10m \
	--rr-insert-channel-size=1024 \
	--rr-update-channel-size=1024 \
	--rr-lateness-tolerance=25ms \
	--rr-busy-tolerance=500µs \
	--rr-wait-tolerance=1ms \
	--rr-initial-queue-size=1000 \
	--rr-max-update-drain-per-issuance=5 \
	--rr-max-insert-drain-per-issuance=5 \
	--rr-default-impact-delay=1s \
	--rr-disable-responsible-probing="${RR_DISABLE_RESPONSIBLE_PROBING}" \
	--rr-disable-staleness="${RR_DISABLE_STALENESS}" \
	--rr-disable-period-adjustment-events=true \
	--rr-disable-pd-inserted-events=true \
	--rr-disable-period-dump-events=true \
	--rr-disable-scheduler-late-events=true
