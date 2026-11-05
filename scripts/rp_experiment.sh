#!/usr/bin/env bash
#
# Runs one block of the responsible probing (RP) on/off experiment: starts the
# orchestrator through orch.sh with RP on or off, waits until the expected
# number of agents is connected, pushes the PDs, lets it run for the given
# duration after the push, then stops it with SIGTERM and waits for a graceful
# shutdown. Run it once with --rp off and once with --rp on, with the same PDs.
#
# Run it from the repository root, like orch.sh, inside tmux or screen.
# Logs, in ./logs (created if needed):
#   logs/rp_experiment_<time>_rp-<on|off>.log       events of this script
#   logs/rp_experiment_<time>_rp-<on|off>.orch.log  output of the orchestrator

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log() {
	printf '%s [rp-experiment] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" | tee -a "${LOG:-/dev/null}" >&2
}

usage() {
	cat >&2 <<USAGE
Usage: $0 --rp on|off --pds <file.jsonl> [flags]

Required:
  --rp on|off            run with responsible probing on or off
  --pds <file>           JSONL file of PDs, pushed with bulk_push.sh

Flags:
  --agents <n>           distinct agents that must be connected before the PDs
                         are pushed (default: 38)
  --agent-timeout <d>    give up if they are not all connected after this long
                         (default: 15m)
  --duration <d>         how long to run after the push ends (default: 3h)
  --lambda <x>           RP threshold in hits per second per address, used with
                         --rp on (default: 1)

Durations are a number followed by s, m or h (e.g. 90s, 15m, 3h).

Env:
  RETINA_SECRET          shared secret agents authenticate with (required)
USAGE
	exit 1
}

# to_seconds converts a duration such as 90s, 15m or 3h to seconds.
to_seconds() {
	local value=$1
	if [[ "$value" =~ ^([0-9]+)([smh]?)$ ]]; then
		local n=${BASH_REMATCH[1]}
		case "${BASH_REMATCH[2]}" in
		h) echo $((n * 3600)) ;;
		m) echo $((n * 60)) ;;
		*) echo "$n" ;;
		esac
	else
		echo "invalid duration: $value" >&2
		return 1
	fi
}

RP=""
PDS=""
AGENTS=38
AGENT_TIMEOUT=15m
DURATION=3h
LAMBDA=1

while [[ $# -gt 0 ]]; do
	case "$1" in
	--rp | --pds | --agents | --agent-timeout | --duration | --lambda)
		[[ $# -ge 2 ]] || { echo "$1 needs a value" >&2; usage; }
		case "$1" in
		--rp) RP=$2 ;;
		--pds) PDS=$2 ;;
		--agents) AGENTS=$2 ;;
		--agent-timeout) AGENT_TIMEOUT=$2 ;;
		--duration) DURATION=$2 ;;
		--lambda) LAMBDA=$2 ;;
		esac
		shift 2
		;;
	-h | --help) usage ;;
	*)
		echo "unknown argument: $1" >&2
		usage
		;;
	esac
done

case "$RP" in
on) RP_THRESHOLD=$LAMBDA ;;
off) RP_THRESHOLD=0 ;;
*)
	echo "--rp must be on or off" >&2
	usage
	;;
esac
[[ -n "$PDS" ]] || { echo "--pds is required" >&2; usage; }
[[ -f "$PDS" ]] || { echo "PD file not found: $PDS" >&2; exit 1; }
[[ "$AGENTS" =~ ^[1-9][0-9]*$ ]] || { echo "--agents must be a positive integer" >&2; exit 1; }
[[ "$LAMBDA" =~ ^[0-9]*\.?[0-9]+$ ]] && [[ ! "$LAMBDA" =~ ^0*\.?0*$ ]] || { echo "--lambda must be a positive number" >&2; exit 1; }
[[ -n "${RETINA_SECRET:-}" ]] || { echo "RETINA_SECRET is not set" >&2; exit 1; }
command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }
[[ -x ./retina-orchestrator ]] || { echo "./retina-orchestrator not found: run from the repository root" >&2; exit 1; }
AGENT_TIMEOUT_S=$(to_seconds "$AGENT_TIMEOUT")
DURATION_S=$(to_seconds "$DURATION")

STAMP=$(date -u +%Y%m%d_%H%M%S)
LOG_DIR=./logs
mkdir -p "$LOG_DIR"
LOG="${LOG_DIR}/rp_experiment_${STAMP}_rp-${RP}.log"
ORCH_LOG="${LOG_DIR}/rp_experiment_${STAMP}_rp-${RP}.orch.log"
ORCH_PID=""

# alive reports whether the orchestrator is still running.
alive() {
	[[ -n "$ORCH_PID" ]] && kill -0 "$ORCH_PID" 2>/dev/null
}

# pause sleeps in the background, so that a signal interrupts it at once.
pause() {
	sleep "$1" &
	wait $! || true
}

# stop_orchestrator sends SIGTERM and waits for a graceful shutdown. It never
# sends SIGKILL, so the capture files are always finalized.
stop_orchestrator() {
	alive || return 0
	log "sending SIGTERM to the orchestrator (pid ${ORCH_PID})"
	kill -TERM "$ORCH_PID" 2>/dev/null || true
	local waited=0
	while alive; do
		pause 5
		waited=$((waited + 5))
		if ((waited % 30 == 0)); then
			log "waiting for the orchestrator to shut down (${waited}s)"
		fi
	done
	local status=0
	wait "$ORCH_PID" || status=$?
	log "orchestrator exited with status ${status} after ${waited}s"
	ORCH_PID=""
}

cleanup() {
	local status=$?
	trap - EXIT INT TERM
	stop_orchestrator
	if [[ $status -eq 0 ]]; then
		log "done"
	else
		log "stopped with status ${status}"
	fi
	exit "$status"
}
trap cleanup EXIT
trap 'log "interrupted"; exit 130' INT
trap 'log "terminated"; exit 143' TERM

# fail_if_dead exits with an error if the orchestrator stopped by itself.
fail_if_dead() {
	if ! alive; then
		local status=0
		wait "$ORCH_PID" || status=$?
		ORCH_PID=""
		log "the orchestrator stopped by itself with status ${status}: see ${ORCH_LOG}"
		exit 1
	fi
}

# connected_agents prints how many distinct agents are connected now, from the
# "Agent connected" and "Agent disconnected" lines of the orchestrator log.
connected_agents() {
	jq -R -n '
		reduce (inputs | fromjson? | select(.msg == "Agent connected" or .msg == "Agent disconnected")) as $e
			({}; if $e.msg == "Agent connected" then .[$e.agent_id] = true else del(.[$e.agent_id]) end)
		| length' "$ORCH_LOG"
}

log "rp=${RP} threshold=${RP_THRESHOLD} pds=${PDS} agents=${AGENTS} agent_timeout=${AGENT_TIMEOUT} duration=${DURATION}"
log "orchestrator log: ${ORCH_LOG}"

RP_THRESHOLD="$RP_THRESHOLD" "$SCRIPT_DIR/orch.sh" >"$ORCH_LOG" 2>&1 &
ORCH_PID=$!
log "orchestrator started (pid ${ORCH_PID})"
pause 2
fail_if_dead
log "$(grep -m1 'capturing into' "$ORCH_LOG" || echo 'capture directory not found in the orchestrator log')"

log "waiting for ${AGENTS} agents"
start=$(date +%s)
last=-1
while :; do
	fail_if_dead
	count=$(connected_agents)
	if [[ "$count" != "$last" ]]; then
		log "${count}/${AGENTS} agents connected"
		last=$count
	fi
	((count >= AGENTS)) && break
	if (($(date +%s) - start >= AGENT_TIMEOUT_S)); then
		log "only ${count}/${AGENTS} agents connected after ${AGENT_TIMEOUT}"
		exit 1
	fi
	pause 5
done

log "push started"
"$SCRIPT_DIR/bulk_push.sh" "$PDS" >>"$LOG" 2>&1 || {
	log "push failed: see ${LOG}"
	exit 1
}
log "push ended"
fail_if_dead

log "running for ${DURATION}"
end=$(($(date +%s) + DURATION_S))
next_report=$(($(date +%s) + 1800))
while (($(date +%s) < end)); do
	fail_if_dead
	now=$(date +%s)
	if ((now >= next_report)); then
		log "$(((end - now) / 60)) min left, $(connected_agents)/${AGENTS} agents connected"
		next_report=$((now + 1800))
	fi
	pause $((end - now < 30 ? end - now : 30))
done
log "run ended"

stop_orchestrator
