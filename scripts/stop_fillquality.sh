#!/usr/bin/env bash
set -euo pipefail
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="$BASE_DIR/runtime/fillquality.pid"
if [[ ! -f "$PID_FILE" ]]; then
  echo "No PID file found."
  exit 0
fi
PID="$(cat "$PID_FILE")"
if kill -0 "$PID" 2>/dev/null; then
  echo "Stopping fillquality PID $PID"
  kill "$PID"
  for _ in {1..15}; do
    if ! kill -0 "$PID" 2>/dev/null; then
      rm -f "$PID_FILE"
      echo "Stopped."
      exit 0
    fi
    sleep 1
  done
  echo "Process still alive after SIGTERM; sending SIGKILL."
  kill -9 "$PID" 2>/dev/null || true
fi
rm -f "$PID_FILE"
echo "Stopped."
