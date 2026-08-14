#!/usr/bin/env bash
set -euo pipefail
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="$BASE_DIR/runtime/fillquality.pid"
HEARTBEAT="$BASE_DIR/runtime/heartbeat.json"
LOG_FILE="$BASE_DIR/logs/fillquality.log"
METRICS_PORT="${METRICS_PORT:-9108}"
echo "=== fillquality status $(date -Is) ==="
if [[ -f "$PID_FILE" ]]; then
  PID="$(cat "$PID_FILE")"
  if kill -0 "$PID" 2>/dev/null; then
    echo "process: RUNNING pid=$PID"
    ps -p "$PID" -o pid,etimes,%cpu,%mem,rss,cmd || true
  else
    echo "process: PID file exists but process is not running pid=$PID"
  fi
else
  echo "process: no PID file"
fi
if [[ -f "$HEARTBEAT" ]]; then
  echo
  echo "=== heartbeat ==="
  python3 - "$HEARTBEAT" <<'PY'
import json, sys, time
p=sys.argv[1]
h=json.load(open(p))
age=time.time()-float(h.get('timestamp',0))
print(f"heartbeat_age_seconds={age:.1f}")
for k in ["spot_age_seconds","futures_age_seconds","net_delta_usd","gross_exposure_usd","fills","pending_orders","resting_orders","seq_gaps","reconnects","decode_errors"]:
    print(f"{k}={h.get(k)}")
PY
else
  echo "heartbeat: missing"
fi
echo
echo "=== recent log ==="
tail -40 "$LOG_FILE" 2>/dev/null || true
echo
echo "=== metrics endpoint sample ==="
if command -v curl >/dev/null 2>&1; then
  curl -fsS "http://localhost:${METRICS_PORT}/metrics" 2>/dev/null | grep -E 'fillquality_(feed_age_seconds|net_delta_usd|gross_exposure_usd|pending_orders|resting_orders|reconnects|seq_gaps|decode_errors)' | head -50 || true
else
  echo "curl not installed"
fi
