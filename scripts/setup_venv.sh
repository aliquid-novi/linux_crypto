#!/usr/bin/env bash
set -euo pipefail
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$BASE_DIR"
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install websockets prometheus_client pandas pyarrow
mkdir -p data runtime logs runs incidents
python test_fillquality_v2.py
printf '\nSetup complete. Start with: ./scripts/start_fillquality.sh\n'
