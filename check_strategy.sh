#!/usr/bin/env bash
set -euo pipefail
BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
PID_FILE="$BASE_DIR/runtime/strategy.pid"
if [[ ! -f "$PID_FILE" ]]; then
 echo "CRITICAL: PID file missing."
 exit 2
fi
PID="$(cat "$PID_FILE")"
if kill -0 "$PID" 2>/dev/null; then
 echo "OK: strategy is running with PID $PID."
 exit 0
else
 echo "CRITICAL: strategy process is not running."
 exit 2
fi
