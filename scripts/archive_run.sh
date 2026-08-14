#!/usr/bin/env bash
set -euo pipefail
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LABEL="${1:-run}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_DIR="$BASE_DIR/runs/${STAMP}_${LABEL}"
mkdir -p "$RUN_DIR"
shopt -s nullglob
for f in "$BASE_DIR"/data/*.jsonl "$BASE_DIR"/runtime/heartbeat.json "$BASE_DIR"/logs/fillquality.log; do
  cp "$f" "$RUN_DIR/" || true
done
if [[ -x "$BASE_DIR/.venv/bin/python" ]]; then
  "$BASE_DIR/.venv/bin/python" "$BASE_DIR/build_fillquality_dataset.py" --data-dir "$RUN_DIR" --out-dir "$RUN_DIR/derived" || true
else
  python3 "$BASE_DIR/build_fillquality_dataset.py" --data-dir "$RUN_DIR" --out-dir "$RUN_DIR/derived" || true
fi
echo "Archived run to $RUN_DIR"
echo "Derived notebook-friendly files should be under $RUN_DIR/derived if conversion succeeded."
