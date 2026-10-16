#!/usr/bin/env bash
# Arm-orchestrator lifecycle manager (HYDRA/ACHILLES) - build hosts first.
# Load the light orchestrator model when idle, unload while the box is building.
#
# Runs from arm-orchestrator.timer every 60s. Idempotent.
# Fix (2026-10-15): decide "is it up?" from the LISTENING PORT (`ss -ltn`),
# not `pgrep -f`, which self-matched and made the manager respawn every tick
# (flapping). Also clear stale instances before (re)starting, and log the result.
set -uo pipefail
PORT="${ARM_PORT:-8080}"
LOG=/tmp/arm-orchestrator.log
MODELLOG=$HOME/arm-orchestrator-model.log
BIN="$HOME/llama-build/llama.cpp/build/bin/llama-server"
MODEL="$HOME/models/gemma4/gemma-4-E2B_q4_0-it.gguf"
ALIAS="${ARM_ALIAS:-gemma4-e2b}"
log(){ echo "$(date -Is) $*" >>"$LOG"; }

# authoritative: is our port actually being listened on?
listening(){ ss -ltnH "( sport = :$PORT )" 2>/dev/null | grep -q .; }
# our server processes for this port (bracket trick avoids self-match)
srvs(){ pgrep -f "[l]lama-server .*--port $PORT" || true; }

busy=0; why=""
pgrep -x cc1plus  >/dev/null && { busy=1; why="cc1plus"; }
pgrep -x rustc    >/dev/null && { busy=1; why="rustc"; }
pgrep -f "[b]itbake|[n]inja|[c]argo build|[g]o build" >/dev/null && { busy=1; why="build"; }
la=$(awk "{print \$1}" /proc/loadavg); cores=$(nproc)
awk -v a="$la" -v c="$cores" "BEGIN{exit !(a > c*1.0)}" && { busy=1; why="load=$la"; }
mavail=$(awk "/MemAvailable/{print int(\$2/1048576)}" /proc/meminfo)
[ "${mavail:-0}" -lt 8 ] && { busy=1; why="mem=${mavail}G"; }

if [ "$busy" = 1 ]; then
  p=$(srvs); [ -n "$p" ] && { kill $p 2>/dev/null; log "unloaded ($why)"; }
else
  if listening; then
    : # already serving, nothing to do
  else
    p=$(srvs); [ -n "$p" ] && { kill $p 2>/dev/null; log "cleared stale pid(s) [$p]"; sleep 1; }
    nohup env HSA_OVERRIDE_GFX_VERSION=11.0.2 "$BIN" \
      -m "$MODEL" -ngl 99 --host 0.0.0.0 --port "$PORT" \
      -c 8192 --alias "$ALIAS" --jinja >"$MODELLOG" 2>&1 &
    pid=$!
    for _ in $(seq 1 30); do
      sleep 2
      listening && { log "loaded (idle) pid=$pid"; break; }
      kill -0 "$pid" 2>/dev/null || { log "load FAILED pid=$pid (died)"; break; }
    done
  fi
fi
