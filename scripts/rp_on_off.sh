#!/usr/bin/env bash
#
# Runs the full responsible probing experiment: one block with RP on, a pause,
# then one block with RP off, both with the same PDs (see rp_experiment.sh).
# Stops if the first block fails. Meant to run unattended from the repository
# root, e.g.:
#
#   RETINA_SECRET=... nohup ./scripts/rp_on_off.sh --pds pds/file.jsonl > logs/rp_on_off.out 2>&1 &

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log() {
	printf '%s [rp-on-off] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >&2
}

usage() {
	cat >&2 <<USAGE
Usage: $0 --pds <file.jsonl> [--pause <minutes>] [--duration <d>]

  --pds <file>        JSONL file of PDs, pushed in both blocks (required)
  --pause <minutes>   pause between the RP on and the RP off block (default: 10)
  --duration <d>      length of each block after its push, e.g. 3h (default: 3h)

Env:
  RETINA_SECRET       shared secret agents authenticate with (required)
USAGE
	exit 1
}

PDS=""
PAUSE_MIN=10
DURATION=3h
while [[ $# -gt 0 ]]; do
	case "$1" in
	--pds | --pause | --duration)
		[[ $# -ge 2 ]] || usage
		case "$1" in
		--pds) PDS=$2 ;;
		--pause) PAUSE_MIN=$2 ;;
		--duration) DURATION=$2 ;;
		esac
		shift 2
		;;
	*) usage ;;
	esac
done
[[ -n "$PDS" ]] || usage
[[ "$PAUSE_MIN" =~ ^[0-9]+$ ]] || { echo "--pause must be a whole number of minutes" >&2; exit 1; }
[[ -n "${RETINA_SECRET:-}" ]] || { echo "RETINA_SECRET is not set" >&2; exit 1; }

log "block 1/2: RP on, pds=${PDS} duration=${DURATION}"
"$SCRIPT_DIR/rp_experiment.sh" --rp on --pds "$PDS" --duration "$DURATION"

log "pausing ${PAUSE_MIN} min"
sleep $((PAUSE_MIN * 60))

log "block 2/2: RP off, pds=${PDS} duration=${DURATION}"
"$SCRIPT_DIR/rp_experiment.sh" --rp off --pds "$PDS" --duration "$DURATION"

log "experiment done"
