#!/usr/bin/env bash
set -euo pipefail
BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
PID_FILE="$BASE_DIR/runtime/strategy.pid"
if [[ ! -f "$PID_FILE" ]]; then
 echo "No PID file found."
 exit 1
fi
PID="$(cat "$PID_FILE")"
if kill -0 "$PID" 2>/dev/null; then
 kill "$PID"
 echo "Stopped strategy PID $PID."
else
 echo "PID file existed, but process was not running."
fi
rm -f "$PID_FILE"
