#!/usr/bin/env bash
set -euo pipefail
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="$BASE_DIR/incidents/$STAMP"
PID_FILE="$BASE_DIR/runtime/fillquality.pid"
HEARTBEAT="$BASE_DIR/runtime/heartbeat.json"
LOG_FILE="$BASE_DIR/logs/fillquality.log"
METRICS_PORT="${METRICS_PORT:-9108}"
mkdir -p "$OUT"
{
  echo "incident_snapshot=$STAMP"
  echo "host=$(hostname)"
  echo "cwd=$BASE_DIR"
  date -Is
} > "$OUT/meta.txt"
if [[ -f "$PID_FILE" ]]; then
  PID="$(cat "$PID_FILE")"
  echo "$PID" > "$OUT/pid.txt"
  ps -p "$PID" -o pid,ppid,etimes,%cpu,%mem,rss,vsz,stat,cmd > "$OUT/process.txt" 2>&1 || true
  if command -v lsof >/dev/null 2>&1; then lsof -p "$PID" > "$OUT/lsof.txt" 2>&1 || true; fi
else
  echo "missing PID file" > "$OUT/pid.txt"
fi
cp "$HEARTBEAT" "$OUT/heartbeat.json" 2>/dev/null || true
tail -300 "$LOG_FILE" > "$OUT/recent_log.txt" 2>/dev/null || true
if command -v curl >/dev/null 2>&1; then
  curl -fsS "http://localhost:${METRICS_PORT}/metrics" > "$OUT/metrics.prom" 2>/dev/null || true
fi
free -h > "$OUT/free_h.txt" 2>&1 || true
df -h > "$OUT/df_h.txt" 2>&1 || true
ps aux --sort=-%cpu | head -20 > "$OUT/top_cpu_processes.txt" 2>&1 || true
ps aux --sort=-%mem | head -20 > "$OUT/top_mem_processes.txt" 2>&1 || true
ss -tanp > "$OUT/ss_tanp.txt" 2>&1 || true
vmstat 1 5 > "$OUT/vmstat.txt" 2>&1 || true
# Copy the last chunk of research data so the incident can be analysed even if files rotate later.
mkdir -p "$OUT/data_tail"
for f in fills.jsonl markouts.jsonl events.jsonl latency.jsonl health.jsonl; do
  if [[ -f "$BASE_DIR/data/$f" ]]; then
    tail -1000 "$BASE_DIR/data/$f" > "$OUT/data_tail/$f" || true
  fi
done
echo "Incident snapshot written to $OUT"
