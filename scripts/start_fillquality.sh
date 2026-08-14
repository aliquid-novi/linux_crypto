#!/usr/bin/env bash
set -euo pipefail
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="$BASE_DIR/runtime/fillquality.pid"
LOG_FILE="$BASE_DIR/logs/fillquality.log"
PYTHON="$BASE_DIR/.venv/bin/python"
SCRIPT="$BASE_DIR/fillquality_v3.py"
mkdir -p "$BASE_DIR/runtime" "$BASE_DIR/logs" "$BASE_DIR/data"
if [[ ! -x "$PYTHON" ]]; then
  echo "Missing venv Python at $PYTHON. Run ./scripts/setup_venv.sh first." >&2
  exit 1
fi
if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "fillquality already running with PID $(cat "$PID_FILE")"
  exit 0
fi
RUN_MINUTES="${RUN_MINUTES:-480}"
TAKER_Z="${TAKER_Z:-0.9}"
PROBE_SECS="${PROBE_SECS:-45}"
QUOTE_REFRESH="${QUOTE_REFRESH:-2.0}"
SIM_LATENCY_MS="${SIM_LATENCY_MS:-40}"
MAX_GROSS_USD="${MAX_GROSS_USD:-5000}"
METRICS_PORT="${METRICS_PORT:-9108}"
NO_MARKET_EVENTS_FLAG="${NO_MARKET_EVENTS_FLAG:-}"
CMD=("$PYTHON" -u "$SCRIPT" \
  --minutes "$RUN_MINUTES" \
  --taker-z "$TAKER_Z" \
  --probe-secs "$PROBE_SECS" \
  --quote-refresh "$QUOTE_REFRESH" \
  --sim-latency-ms "$SIM_LATENCY_MS" \
  --max-gross-usd "$MAX_GROSS_USD" \
  --metrics-port "$METRICS_PORT")
if [[ -n "$NO_MARKET_EVENTS_FLAG" ]]; then
  CMD+=(--no-market-events)
fi
{
  echo "===== starting $(date -Is) ====="
  echo "BASE_DIR=$BASE_DIR"
  echo "CMD=${CMD[*]}"
} >> "$LOG_FILE"
nohup "${CMD[@]}" >> "$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"
echo "Started fillquality PID $(cat "$PID_FILE")"
echo "Log: $LOG_FILE"
echo "Heartbeat: $BASE_DIR/runtime/heartbeat.json"
echo "Metrics: http://localhost:${METRICS_PORT}/metrics"
