#!/usr/bin/env python3
"""health_check.py — operational health for the fillquality process.

Exit codes (Nagios-style, cron/systemd-timer friendly):
    0  OK        1  WARNING        2  CRITICAL

Layers checked, in order:
    1. process   — does the PID in runtime/fillquality.pid exist?
    2. heartbeat — is runtime/heartbeat.json itself fresh?
    3. data      — are BOTH feeds fresh according to the heartbeat?
    4. errors    — reconnects / sequence gaps / crossed books accumulating?
    5. disk      — is the data directory filling the disk?

A process can pass (1) and fail (3): alive but not healthy. That distinction
is the entire point of the checker.
"""

import json
import shutil
import sys
import time
from pathlib import Path

BASE = Path(__file__).resolve().parent
PID_FILE = BASE / "runtime" / "fillquality.pid"
HEARTBEAT = BASE / "runtime" / "heartbeat.json"

HEARTBEAT_WARN, HEARTBEAT_CRIT = 10.0, 30.0     # seconds
FEED_WARN, FEED_CRIT = 10.0, 30.0               # seconds
DISK_WARN, DISK_CRIT = 0.80, 0.90               # fraction used

OK, WARNING, CRITICAL = 0, 1, 2


def pid_running(pid: int) -> bool:
    """True if a process with this PID exists (signal 0 probe)."""
    try:
        import os
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True     # exists, owned by someone else


def main() -> int:
    problems: list[tuple[int, str]] = []
    facts: list[str] = []

    # -- 1. process ------------------------------------------------------
    if not PID_FILE.exists():
        print("CRITICAL: no PID file — strategy not started or crashed before writing it")
        return CRITICAL
    try:
        pid = int(PID_FILE.read_text().strip())
    except ValueError:
        print("CRITICAL: PID file is corrupt")
        return CRITICAL
    if not pid_running(pid):
        print(f"CRITICAL: process {pid} is not running (stale PID file)")
        return CRITICAL
    facts.append(f"pid={pid}")

    # -- 2. heartbeat freshness -----------------------------------------
    if not HEARTBEAT.exists():
        print(f"CRITICAL: process {pid} alive but no heartbeat file — "
              "started too recently, or the telemetry task is dead")
        return CRITICAL
    try:
        hb = json.loads(HEARTBEAT.read_text())
    except json.JSONDecodeError:
        # atomic writes should make this impossible; seeing it is itself a bug
        print("CRITICAL: heartbeat file is not valid JSON")
        return CRITICAL

    hb_age = time.time() - float(hb.get("timestamp", 0))
    facts.append(f"heartbeat={hb_age:.1f}s")
    if hb_age > HEARTBEAT_CRIT:
        problems.append((CRITICAL, f"heartbeat is {hb_age:.0f}s old — "
                                   "process alive but event loop stalled"))
    elif hb_age > HEARTBEAT_WARN:
        problems.append((WARNING, f"heartbeat is {hb_age:.0f}s old"))

    # -- 3. feed freshness (the alive-vs-healthy distinction) ------------
    for label, key in (("spot", "spot_age_seconds"),
                       ("futures", "futures_age_seconds")):
        age = hb.get(key)
        if age is None:                    # null = never connected this run
            problems.append((CRITICAL, f"{label} feed has never delivered data"))
            continue
        facts.append(f"{label}={age:.1f}s")
        if age > FEED_CRIT:
            problems.append((CRITICAL, f"{label} feed is {age:.0f}s stale"))
        elif age > FEED_WARN:
            problems.append((WARNING, f"{label} feed is {age:.0f}s stale"))

    # -- 4. accumulated errors -------------------------------------------
    for key, warn_at in (("seq_gaps", 1), ("reconnects", 5),
                         ("decode_errors", 1), ("crossed_books", 1)):
        n = hb.get(key, 0) or 0
        if n >= warn_at:
            problems.append((WARNING, f"{key}={n} this run"))
        facts.append(f"{key}={n}")

    # -- 5. disk ----------------------------------------------------------
    usage = shutil.disk_usage(BASE)
    frac = usage.used / usage.total
    facts.append(f"disk={frac:.0%}")
    if frac > DISK_CRIT:
        problems.append((CRITICAL, f"disk {frac:.0%} used"))
    elif frac > DISK_WARN:
        problems.append((WARNING, f"disk {frac:.0%} used"))

    # -- verdict -----------------------------------------------------------
    severity = max((s for s, _ in problems), default=OK)
    tag = {OK: "OK", WARNING: "WARNING", CRITICAL: "CRITICAL"}[severity]
    detail = "; ".join(msg for _, msg in problems) or "all checks passed"
    print(f"{tag}: {detail} | {' '.join(facts)}")
    return severity


if __name__ == "__main__":
    sys.exit(main())
