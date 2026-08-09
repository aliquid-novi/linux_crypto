#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
PID_FILE="$BASE_DIR/runtime/strategy.pid"
LOG_FILE="$BASE_DIR/logs/strategy.log"
PYTHON="$BASE_DIR/.venv/bin/python"
SCRIPT="$BASE_DIR/delta_neut_strat.py"

mkdir -p "$(dirname "$LOG_FILE")" "$(dirname "$PID_FILE")"

echo "$LOG_FILE"

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
 echo "Strategy is already running."
 exit 1
fi

nohup "$PYTHON" -u "$SCRIPT" >> "$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"
echo "Started strategy with PID $(cat "$PID_FILE")."

