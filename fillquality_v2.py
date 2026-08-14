#!/usr/bin/env python3
"""
fillquality.py — an instrumented paper trading harness for execution-quality research.

WHAT THIS IS
------------
A loose delta-neutral BTC strategy across Kraken spot (BTC/USD) and Kraken
perpetual futures (PF_XBTUSD), running on live WebSocket data, designed to
trade often so that it generates a usable sample of fills.

The strategy exists to produce fills. The point is the telemetry.

WHAT IS REAL AND WHAT IS SIMULATED
----------------------------------
REAL:
  - all market data (books, trades, exchange timestamps)
  - every latency measurement of this process's own pipeline
  - taker slippage: an order decided at t is filled against the book as it
    actually exists at t + latency. The price difference is real market
    movement over a real time window.
  - maker fills: a resting quote fills only when real trades actually consume
    the volume queued ahead of it at that price level.
  - adverse selection: mark-outs use the real mid at t_fill + horizon.

SIMULATED:
  - the fill itself. No orders are sent anywhere. There is no exchange RTT,
    no matching engine, no rejects, no partial-fill policy, no fees beyond a
    flat assumption.
  - queue position, which is estimated from displayed depth at join time and
    is therefore optimistic (it ignores hidden orders, amend-in-place, and
    queue jumping).
  - PnL, which is consequently fiction. Do not read it as a backtest.

The measurements that matter for execution-quality research are in the first
list. The strategy's profitability is in the second. Keep them separate.

USAGE
-----
    pip install websockets
    python3 fillquality.py --minutes 120

    # more aggressive, more fills, more data:
    python3 fillquality.py --taker-z 0.8 --probe-secs 45 --quote-refresh 2.0

OUTPUTS (under ./data/)
-----------------------
    fills.jsonl      every fill, written immediately: timestamp chain,
                     slippage decomposition, decision-state, expected band
    markouts.jsonl   one later row per fill/horizon (0.5s ... 60s)
    events.jsonl     append-only forensic timeline: book tops, trades, orders,
                     fills, reconnects, gaps, stale-feed events
    latency.jsonl    per-second bucketed histograms of pipeline/fill latency,
                     ready to render as a time x latency heat map
    health.jsonl     per-second USE/application health metrics
    heartbeat.json   current liveness/state for an external shell monitor

PROMETHEUS
----------
If prometheus_client is installed, live health/latency metrics are exposed on
:9108/metrics by default.  Prometheus is for monitoring/correlation; JSONL is
the source of truth for fill-level research in pandas/Jupyter.
"""

from __future__ import annotations

import argparse
import asyncio
import bisect
import json
import math
import os
import random
import signal
import statistics
import sys
import time
import uuid
from collections import deque
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Deque, Dict, List, Optional, Tuple

try:
    import websockets
except ImportError:
    sys.exit("pip install websockets")

try:
    from prometheus_client import CollectorRegistry, Gauge, Histogram, start_http_server
except ImportError:  # Prometheus is optional for offline/unit-test use.
    CollectorRegistry = Gauge = Histogram = start_http_server = None

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------

SPOT_WS = "wss://ws.kraken.com/v2"
FUT_WS = "wss://futures.kraken.com/ws/v1"
SPOT_SYMBOL = "BTC/USD"
FUT_SYMBOL = "PF_XBTUSD"

BASE = Path(__file__).resolve().parent
DATA = BASE / "data"
RUNTIME = BASE / "runtime"


@dataclass
class Config:
    # --- strategy ---
    basis_window: int = 600          # samples for the rolling basis z-score
    taker_z: float = 1.2             # |z| above this -> cross the spread
    quote_offset_bps: float = 1.5    # passive quote distance from mid
    quote_refresh: float = 3.0       # seconds between requotes
    clip_usd: float = 250.0          # notional per trade
    max_net_delta_usd: float = 2500.0  # "loose" delta band
    max_gross_usd: float = 5000.0      # cap gross notional, not just net delta
    cooldown: float = 1.5              # min seconds between taker trades

    # --- experimental design ---
    probe_secs: float = 90.0         # mean interval between unconditional
                                     # probe trades (see note in README below)
    probe_enabled: bool = True

    # --- execution simulation ---
    sim_latency_ms: float = 40.0     # decision -> arrival at matching engine
    sim_latency_jitter_ms: float = 25.0
    fee_taker_bps: float = 2.6
    fee_maker_bps: float = -0.2      # rebate

    # --- measurement ---
    markout_horizons: Tuple[float, ...] = (0.5, 1.0, 5.0, 30.0, 60.0)
    stale_threshold: float = 10.0
    ewma_alpha: float = 0.97          # online short-horizon variance-rate EWMA
    expected_band_sigmas: float = 1.96

    # --- monitoring ---
    metrics_port: int = 9108          # node_exporter normally uses 9100
    prometheus_enabled: bool = True
    record_market_events: bool = True

    # --- run control ---
    minutes: float = 60.0
    seed: Optional[int] = None


# --------------------------------------------------------------------------
# Order book
# --------------------------------------------------------------------------

class Book:
    """Price-level book with sequence-gap detection and staleness tracking."""

    def __init__(self, name: str):
        self.name = name
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.seq: Optional[int] = None
        self.last_update: float = 0.0        # monotonic
        self.last_exch_ts: Optional[float] = None  # exchange wall clock
        self.gaps = 0
        self.updates = 0

    # -- mutation -------------------------------------------------------
    def apply_level(self, side: str, price: float, qty: float) -> None:
        book = self.bids if side == "bid" else self.asks
        if qty <= 0:
            book.pop(price, None)
        else:
            book[price] = qty

    def replace(self, bids: List[Tuple[float, float]],
                asks: List[Tuple[float, float]]) -> None:
        self.bids = {p: q for p, q in bids if q > 0}
        self.asks = {p: q for p, q in asks if q > 0}

    def touch(self, mono: float, exch_ts: Optional[float]) -> None:
        self.last_update = mono
        self.last_exch_ts = exch_ts
        self.updates += 1

    def check_seq(self, seq: Optional[int]) -> bool:
        """Returns True if a gap was detected."""
        if seq is None:
            return False
        if self.seq is not None and seq != self.seq + 1:
            self.gaps += 1
            self.seq = seq
            return True
        self.seq = seq
        return False

    # -- reads ----------------------------------------------------------
    @property
    def best_bid(self) -> Optional[Tuple[float, float]]:
        if not self.bids:
            return None
        p = max(self.bids)
        return p, self.bids[p]

    @property
    def best_ask(self) -> Optional[Tuple[float, float]]:
        if not self.asks:
            return None
        p = min(self.asks)
        return p, self.asks[p]

    @property
    def mid(self) -> Optional[float]:
        b, a = self.best_bid, self.best_ask
        if not b or not a:
            return None
        return (b[0] + a[0]) / 2.0

    @property
    def spread_bps(self) -> Optional[float]:
        b, a = self.best_bid, self.best_ask
        if not b or not a:
            return None
        m = (b[0] + a[0]) / 2.0
        return (a[0] - b[0]) / m * 1e4 if m else None

    @property
    def imbalance(self) -> Optional[float]:
        """(bidqty - askqty) / (bidqty + askqty) at top of book."""
        b, a = self.best_bid, self.best_ask
        if not b or not a:
            return None
        tot = b[1] + a[1]
        return (b[1] - a[1]) / tot if tot else 0.0

    def microprice(self) -> Optional[float]:
        """Depth-weighted mid. Stoikov's micro-price is more principled;
        this is the cheap version."""
        b, a = self.best_bid, self.best_ask
        if not b or not a:
            return None
        tot = b[1] + a[1]
        if not tot:
            return (b[0] + a[0]) / 2.0
        return (b[0] * a[1] + a[0] * b[1]) / tot

    def depth_at_or_better(self, side: str, price: float) -> float:
        """Total displayed qty at prices at least as aggressive as `price`.
        Used as the queue-ahead estimate when a passive order joins."""
        if side == "buy":
            return sum(q for p, q in self.bids.items() if p >= price)
        return sum(q for p, q in self.asks.items() if p <= price)

    def sweep(self, side: str, qty: float) -> Optional[Tuple[float, float]]:
        """Walk the book for a market order. Returns (vwap, filled_qty)."""
        levels = (sorted(self.asks.items()) if side == "buy"
                  else sorted(self.bids.items(), reverse=True))
        remaining, cost, filled = qty, 0.0, 0.0
        for price, avail in levels:
            take = min(remaining, avail)
            cost += take * price
            filled += take
            remaining -= take
            if remaining <= 1e-12:
                break
        if filled <= 0:
            return None
        return cost / filled, filled

    def age(self, now_mono: float) -> float:
        return now_mono - self.last_update if self.last_update else float("inf")


class EWMAVol:
    """Online variance-rate estimator for short-horizon mid-price movement.

    We update r^2 / dt, which has units of variance per second.  That lets us
    scale an expected one-sigma move to an arbitrary latency horizon h via
    sqrt(var_rate * h).  This is a *benchmark*, not a calibrated fill model.
    """

    def __init__(self, alpha: float = 0.97):
        self.alpha = alpha
        self.var_rate = 0.0
        self.last_mid: Optional[float] = None
        self.last_mono: Optional[float] = None
        self.samples = 0

    def update(self, mid: Optional[float], mono: float) -> None:
        if mid is None or mid <= 0:
            return
        if self.last_mid is not None and self.last_mono is not None:
            dt = mono - self.last_mono
            if dt > 1e-6:
                r = math.log(mid / self.last_mid)
                obs = (r * r) / dt
                self.var_rate = (
                    obs if self.samples == 0
                    else self.alpha * self.var_rate + (1.0 - self.alpha) * obs
                )
                self.samples += 1
        self.last_mid = mid
        self.last_mono = mono

    def sigma_bps(self, horizon_seconds: float) -> float:
        if self.var_rate <= 0 or horizon_seconds <= 0:
            return 0.0
        return math.sqrt(self.var_rate * horizon_seconds) * 1e4


# --------------------------------------------------------------------------
# USE metrics
# --------------------------------------------------------------------------

class Metrics:
    """Utilisation / Saturation / Errors, in Gregg's sense, for this process.

    Utilisation : fraction of wall time the event loop spent doing work
    Saturation  : event-loop lag and inbound queue depth
    Errors      : reconnects, sequence gaps, decode failures, stale periods
    """

    def __init__(self):
        self.reconnects = 0
        self.seq_gaps = 0
        self.decode_errors = 0
        self.stale_events = 0
        self.msgs = 0
        self.busy_ns = 0
        self.loop_lag_samples: Deque[float] = deque(maxlen=4096)
        self.queue_depth_max = 0
        self._window_start = time.monotonic()

    def record_work(self, ns: int) -> None:
        self.busy_ns += ns
        self.msgs += 1

    def snapshot(self) -> dict:
        now = time.monotonic()
        elapsed = max(now - self._window_start, 1e-9)
        lags = list(self.loop_lag_samples)
        snap = {
            "msgs": self.msgs,
            "msg_rate": self.msgs / elapsed,
            "utilisation": min(self.busy_ns / (elapsed * 1e9), 1.0),
            "loop_lag_p50_ms": _pct(lags, 50) * 1e3 if lags else None,
            "loop_lag_p99_ms": _pct(lags, 99) * 1e3 if lags else None,
            "loop_lag_max_ms": max(lags) * 1e3 if lags else None,
            "queue_depth_max": self.queue_depth_max,
            "reconnects": self.reconnects,
            "seq_gaps": self.seq_gaps,
            "decode_errors": self.decode_errors,
            "stale_events": self.stale_events,
        }
        self.msgs = 0
        self.busy_ns = 0
        self.queue_depth_max = 0
        self.loop_lag_samples.clear()
        self._window_start = now
        return snap


def _pct(sorted_or_not: List[float], p: float) -> float:
    if not sorted_or_not:
        return 0.0
    xs = sorted(sorted_or_not)
    k = (len(xs) - 1) * p / 100.0
    lo, hi = math.floor(k), math.ceil(k)
    if lo == hi:
        return xs[int(k)]
    return xs[lo] * (hi - k) + xs[hi] * (k - lo)


# --------------------------------------------------------------------------
# Latency histogram (heat-map source)
# --------------------------------------------------------------------------

class LatencyHistogram:
    """Log-spaced buckets, flushed once per second.

    One JSONL row per second per stage gives you exactly the (time, latency
    bucket, count) triple a Gregg latency heat map needs — no post-processing
    of millions of raw samples required.
    """

    # 10us .. 10s, ~5 buckets per decade
    EDGES = [10e-6 * (10 ** (i / 5.0)) for i in range(31)]

    def __init__(self):
        self.stages: Dict[str, List[int]] = {}

    def add(self, stage: str, seconds: float) -> None:
        counts = self.stages.setdefault(stage, [0] * (len(self.EDGES) + 1))
        counts[bisect.bisect_right(self.EDGES, seconds)] += 1

    def flush(self) -> Dict[str, List[int]]:
        out = self.stages
        self.stages = {}
        return out


# --------------------------------------------------------------------------
# Orders and fills
# --------------------------------------------------------------------------

@dataclass
class Order:
    oid: int
    venue: str
    side: str               # buy | sell
    kind: str               # taker | maker
    qty: float              # in BTC
    limit: Optional[float]
    reason: str             # signal | probe | hedge | flatten

    # timestamp chain (all time.time() wall clock unless noted)
    t_data_exch: Optional[float] = None   # exchange ts of triggering message
    t_data_recv: float = 0.0              # local receipt of that message
    t_decision: float = 0.0               # strategy decided
    t_submit: float = 0.0                 # handed to execution
    t_effective: float = 0.0              # reaches simulated matching engine

    # market state at decision
    arrival_mid: float = 0.0
    arrival_micro: float = 0.0
    arrival_bid: float = 0.0
    arrival_ask: float = 0.0
    arrival_spread_bps: float = 0.0
    arrival_imbalance: float = 0.0
    basis_z: float = 0.0

    # queue model (maker only)
    queue_ahead: float = 0.0
    queue_consumed: float = 0.0
    queue_initialized: bool = False

    # execution state
    remaining_qty: float = 0.0
    effective_logged: bool = False

    # health at decision — the join between ops and execution quality
    loop_lag_ms: float = 0.0
    book_age_ms: float = 0.0
    feed_lag_ms: Optional[float] = None

    # ex-ante benchmark; never used to label a fill good/bad
    expected_move_1sigma_bps: float = 0.0
    expected_slippage_center_bps: Optional[float] = None
    expected_slippage_low_bps: Optional[float] = None
    expected_slippage_high_bps: Optional[float] = None

    state: str = "live"     # live | partially_filled | filled | cancelled


@dataclass
class Fill:
    order: Order
    t_fill: float
    price: float
    qty: float
    fill_mid: float
    markouts: Dict[str, Optional[float]] = field(default_factory=dict)


# --------------------------------------------------------------------------
# Execution simulator
# --------------------------------------------------------------------------

class Simulator:
    def __init__(self, cfg: Config, books: Dict[str, Book], metrics: Metrics,
                 event_sink=None):
        self.cfg = cfg
        self.books = books
        self.metrics = metrics
        self.event_sink = event_sink
        self.pending: List[Order] = []      # taker orders in flight
        self.resting: List[Order] = []      # maker orders awaiting/live on book
        self.fills: List[Fill] = []
        self.pending_markouts: List[Tuple[Fill, float, str]] = []
        self._oid = 0

    def _event(self, event: str, **fields) -> None:
        if self.event_sink is not None:
            self.event_sink(event, **fields)

    def next_oid(self) -> int:
        self._oid += 1
        return self._oid

    def has_pending_reason(self, reason: str) -> bool:
        return any(o.reason == reason and o.state in ("live", "partially_filled")
                   for o in self.pending + self.resting)

    # -- submission -----------------------------------------------------
    def submit(self, order: Order) -> None:
        order.t_submit = time.time()
        order.remaining_qty = order.qty
        lat = max(0.0, random.gauss(self.cfg.sim_latency_ms,
                                    self.cfg.sim_latency_jitter_ms)) / 1e3
        order.t_effective = order.t_submit + lat
        self._event(
            "ORDER_SUBMIT", oid=order.oid, venue=order.venue, side=order.side,
            kind=order.kind, reason=order.reason, qty=order.qty,
            limit=order.limit, t_decision=order.t_decision,
            t_submit=order.t_submit, t_effective=order.t_effective,
            arrival_mid=order.arrival_mid, arrival_bid=order.arrival_bid,
            arrival_ask=order.arrival_ask, spread_bps=order.arrival_spread_bps,
            imbalance=order.arrival_imbalance, basis_z=order.basis_z,
            book_age_ms=order.book_age_ms, feed_lag_ms=order.feed_lag_ms,
            loop_lag_ms=order.loop_lag_ms,
            expected_move_1sigma_bps=order.expected_move_1sigma_bps,
            expected_slippage_center_bps=order.expected_slippage_center_bps,
            expected_slippage_low_bps=order.expected_slippage_low_bps,
            expected_slippage_high_bps=order.expected_slippage_high_bps,
        )
        if order.kind == "taker":
            self.pending.append(order)
        else:
            # Queue position is intentionally NOT measured here.  The order
            # does not exist at the simulated matching engine until t_effective.
            self.resting.append(order)

    def _activate(self, o: Order, now: float) -> None:
        if o.effective_logged or now < o.t_effective:
            return
        o.effective_logged = True
        if o.kind == "maker" and not o.queue_initialized:
            book = self.books[o.venue]
            o.queue_ahead = book.depth_at_or_better(o.side, o.limit)
            o.queue_initialized = True
        self._event(
            "ORDER_EFFECTIVE", oid=o.oid, venue=o.venue, side=o.side,
            kind=o.kind, reason=o.reason, queue_ahead=o.queue_ahead,
            t_effective=o.t_effective, observed_at=now,
        )

    def cancel_all_makers(self, venue: Optional[str] = None) -> None:
        keep = []
        for o in self.resting:
            if venue is None or o.venue == venue:
                o.state = "cancelled"
                self._event("ORDER_CANCEL", oid=o.oid, venue=o.venue,
                            reason="requote", remaining_qty=o.remaining_qty)
            else:
                keep.append(o)
        self.resting = keep

    # -- the tick that makes slippage real ------------------------------
    def on_clock(self, now: float) -> List[Fill]:
        """Activate due makers and fill due takers against the later book.

        Takers behave like an IOC against the displayed depth we have: if the
        visible book cannot satisfy the whole order, the visible quantity fills
        and the unfilled remainder is cancelled rather than retried later.
        """
        for o in self.resting:
            self._activate(o, now)

        out, still = [], []
        for o in self.pending:
            if now < o.t_effective:
                still.append(o)
                continue
            self._activate(o, now)
            book = self.books[o.venue]
            res = book.sweep(o.side, o.remaining_qty)
            if res is None:
                o.state = "cancelled"
                self._event("ORDER_CANCEL", oid=o.oid, venue=o.venue,
                            reason="empty_displayed_book",
                            remaining_qty=o.remaining_qty)
                continue
            vwap, filled = res
            final = filled + 1e-12 >= o.remaining_qty
            fill = self._record(o, now, vwap, filled, final=final)
            out.append(fill)
            if not final:
                o.state = "partially_filled"
                self._event("ORDER_CANCEL", oid=o.oid, venue=o.venue,
                            reason="ioc_remainder",
                            remaining_qty=o.remaining_qty)
        self.pending = still
        return out

    def on_trade(self, venue: str, side: str, price: float,
                 qty: float, now: float) -> List[Fill]:
        """Approximate maker queue consumption from real public trades.

        `side` is the aggressor side.  queue_ahead contains displayed volume at
        better prices plus the displayed volume at our own price when the order
        becomes effective.  Trades at better prices reduce that queue; trades
        through our price imply the order was reached.  This remains an L2
        approximation, not a claim about true exchange queue priority.
        """
        out, still = [], []
        for o in self.resting:
            if o.venue != venue or now < o.t_effective:
                still.append(o)
                continue
            self._activate(o, now)

            correct_aggressor = ((o.side == "buy" and side == "sell") or
                                 (o.side == "sell" and side == "buy"))
            if not correct_aggressor:
                still.append(o)
                continue

            # Price-priority relation to our limit.
            if o.side == "buy":
                better = price > o.limit
                same = abs(price - o.limit) < 1e-12
                through = price < o.limit
            else:
                better = price < o.limit
                same = abs(price - o.limit) < 1e-12
                through = price > o.limit

            if through:
                got = o.remaining_qty
                if got > 1e-12:
                    out.append(self._record(o, now, o.limit, got, final=True))
                continue

            if not (better or same):
                still.append(o)
                continue

            # Better-price and same-price executions consume volume ahead of us.
            previous = o.queue_consumed
            o.queue_consumed += qty
            before_ours = max(o.queue_ahead - previous, 0.0)
            qty_reaching_us = max(qty - before_ours, 0.0)

            if qty_reaching_us > 0 and same:
                got = min(o.remaining_qty, qty_reaching_us)
                final = got + 1e-12 >= o.remaining_qty
                out.append(self._record(o, now, o.limit, got, final=final))
                if not final:
                    o.state = "partially_filled"
                    still.append(o)
            else:
                still.append(o)

        self.resting = still
        return out

    def _record(self, o: Order, now: float, price: float, qty: float,
                final: bool) -> Fill:
        qty = min(qty, o.remaining_qty)
        o.remaining_qty = max(o.remaining_qty - qty, 0.0)
        o.state = "filled" if final or o.remaining_qty <= 1e-12 else "partially_filled"
        book = self.books[o.venue]
        fill = Fill(order=o, t_fill=now, price=price, qty=qty,
                    fill_mid=book.mid or price)
        self.fills.append(fill)
        for h in self.cfg.markout_horizons:
            self.pending_markouts.append((fill, now + h, f"{h}s"))
        return fill

    def resolve_markouts(self, now: float) -> List[Tuple[Fill, str, Optional[float]]]:
        """Resolve every due mark-out immediately. Positive = favourable.

        Each horizon is returned separately so it can be persisted immediately;
        a crash 59 seconds after a fill therefore does not erase the fill itself.
        """
        resolved, still = [], []
        for fill, due, label in self.pending_markouts:
            if now < due:
                still.append((fill, due, label))
                continue
            book = self.books[fill.order.venue]
            mid = book.mid
            if mid is None:
                value = None
            else:
                sign = 1.0 if fill.order.side == "buy" else -1.0
                value = sign * (mid - fill.price) / fill.price * 1e4
            fill.markouts[label] = value
            resolved.append((fill, label, value))
        self.pending_markouts = still
        return resolved


# --------------------------------------------------------------------------
# Strategy
# --------------------------------------------------------------------------

class Strategy:
    """Loose delta-neutral basis trader.

    Long spot / short perp when the perp is rich, and the reverse when cheap,
    sized small and rebalanced often. The delta band is deliberately wide —
    'loose' — so the book breathes and we generate hedge trades as well as
    signal trades.
    """

    def __init__(self, cfg: Config, books: Dict[str, Book], sim: Simulator,
                 metrics: Metrics, vols: Optional[Dict[str, EWMAVol]] = None):
        self.cfg = cfg
        self.books = books
        self.sim = sim
        self.metrics = metrics
        self.vols = vols or {k: EWMAVol(cfg.ewma_alpha) for k in books}
        self.basis_hist: Deque[float] = deque(maxlen=cfg.basis_window)
        self.pos: Dict[str, float] = {"spot": 0.0, "fut": 0.0}
        self.last_taker = 0.0
        self.last_quote = 0.0
        self.next_probe = time.time() + random.expovariate(1.0 / cfg.probe_secs)

    # -- state ----------------------------------------------------------
    def basis_bps(self) -> Optional[float]:
        s, f = self.books["spot"].mid, self.books["fut"].mid
        if not s or not f:
            return None
        return (f - s) / s * 1e4

    def basis_z(self) -> Optional[float]:
        if len(self.basis_hist) < 60:
            return None
        mu = statistics.fmean(self.basis_hist)
        sd = statistics.pstdev(self.basis_hist)
        b = self.basis_bps()
        if b is None or sd < 1e-9:
            return None
        return (b - mu) / sd

    def net_delta_usd(self) -> float:
        s, f = self.books["spot"].mid, self.books["fut"].mid
        if not s or not f:
            return 0.0
        return self.pos["spot"] * s + self.pos["fut"] * f

    def gross_exposure_usd(self) -> float:
        s, f = self.books["spot"].mid, self.books["fut"].mid
        if not s or not f:
            return 0.0
        return abs(self.pos["spot"] * s) + abs(self.pos["fut"] * f)

    def feeds_healthy(self) -> bool:
        now_m = time.monotonic()
        return all(b.age(now_m) <= self.cfg.stale_threshold for b in self.books.values())

    # -- decision -------------------------------------------------------
    def evaluate(self, trigger_exch_ts: Optional[float],
                 trigger_recv: float, loop_lag_ms: float) -> None:
        now = time.time()
        b = self.basis_bps()
        if b is not None:
            self.basis_hist.append(b)
        z = self.basis_z()
        z_for_risk = z if z is not None else 0.0

        # Never create new simulated orders from stale market state.
        if not self.feeds_healthy():
            return

        # Risk controls are deliberately independent of whether alpha is warm.
        nd = self.net_delta_usd()
        gross = self.gross_exposure_usd()

        if abs(nd) > self.cfg.max_net_delta_usd and not self.sim.has_pending_reason("flatten"):
            side = "sell" if nd > 0 else "buy"
            self._send("fut", side, "taker", "flatten", z_for_risk,
                       trigger_exch_ts, trigger_recv, loop_lag_ms)

        if gross > self.cfg.max_gross_usd and not self.sim.has_pending_reason("gross_reduce"):
            # Reduce each existing leg toward zero rather than assuming net delta
            # is a sufficient description of risk.
            for venue, key in (("spot", "spot"), ("fut", "fut")):
                if abs(self.pos[key]) <= 1e-12:
                    continue
                side = "sell" if self.pos[key] > 0 else "buy"
                self._send(venue, side, "taker", "gross_reduce", z_for_risk,
                           trigger_exch_ts, trigger_recv, loop_lag_ms)
            self.sim.cancel_all_makers()

        # Signal-dependent logic begins only after the z-score is usable.
        if z is None:
            return

        # 1. signal-driven paired taker trades
        projected_gross = gross + 2.0 * self.cfg.clip_usd
        if (abs(z) >= self.cfg.taker_z and
                now - self.last_taker >= self.cfg.cooldown and
                abs(nd) < self.cfg.max_net_delta_usd and
                projected_gross <= self.cfg.max_gross_usd):
            # perp rich (z>0): sell perp, buy spot
            fut_side = "sell" if z > 0 else "buy"
            spot_side = "buy" if z > 0 else "sell"
            self._send("fut", fut_side, "taker", "signal", z,
                       trigger_exch_ts, trigger_recv, loop_lag_ms)
            self._send("spot", spot_side, "taker", "signal", z,
                       trigger_exch_ts, trigger_recv, loop_lag_ms)
            self.last_taker = now

        # 2. unconditional probe trades — useful as a control group.
        if self.cfg.probe_enabled and now >= self.next_probe:
            venue = random.choice(["spot", "fut"])
            side = random.choice(["buy", "sell"])
            self._send(venue, side, "taker", "probe", z,
                       trigger_exch_ts, trigger_recv, loop_lag_ms)
            self.next_probe = now + random.expovariate(1.0 / self.cfg.probe_secs)

        # 3. passive quotes. Do not add more passive gross risk once at cap.
        if now - self.last_quote >= self.cfg.quote_refresh:
            self.sim.cancel_all_makers()
            if gross < self.cfg.max_gross_usd:
                for venue in ("spot", "fut"):
                    book = self.books[venue]
                    mid = book.mid
                    if not mid:
                        continue
                    off = mid * self.cfg.quote_offset_bps / 1e4
                    # Tick sizes are config/venue knowledge in a real gateway;
                    # keep the simulator price simple but do not pretend this is
                    # an exchange-valid order price.
                    for side, px in (("buy", mid - off), ("sell", mid + off)):
                        self._send(venue, side, "maker", "quote", z,
                                   trigger_exch_ts, trigger_recv, loop_lag_ms,
                                   limit=px)
            self.last_quote = now

    def _send(self, venue: str, side: str, kind: str, reason: str,
              z: float, exch_ts: Optional[float], recv: float,
              loop_lag_ms: float, limit: Optional[float] = None) -> None:
        book = self.books[venue]
        mid = book.mid
        if not mid:
            return
        bb, ba = book.best_bid, book.best_ask
        if not bb or not ba:
            return
        now = time.time()
        qty = self.cfg.clip_usd / mid
        sign = 1.0 if side == "buy" else -1.0

        # Ex-ante benchmark only. For takers, the centre is the cost we would
        # see if we crossed the CURRENT displayed book immediately.  The band
        # adds a short-horizon volatility allowance over expected latency.
        sweep = book.sweep(side, qty)
        if kind == "taker" and sweep is not None:
            current_vwap, _ = sweep
            centre = sign * (current_vwap - mid) / mid * 1e4
        elif kind == "maker" and limit is not None:
            centre = sign * (limit - mid) / mid * 1e4
        else:
            centre = None

        horizon = max(self.cfg.sim_latency_ms, 0.0) / 1e3
        sigma_bps = self.vols[venue].sigma_bps(horizon)
        width = self.cfg.expected_band_sigmas * sigma_bps
        low = centre - width if centre is not None else None
        high = centre + width if centre is not None else None

        o = Order(
            oid=self.sim.next_oid(), venue=venue, side=side, kind=kind,
            qty=qty, limit=limit, reason=reason,
            t_data_exch=exch_ts, t_data_recv=recv, t_decision=now,
            arrival_mid=mid, arrival_micro=book.microprice() or mid,
            arrival_bid=bb[0], arrival_ask=ba[0],
            arrival_spread_bps=book.spread_bps or 0.0,
            arrival_imbalance=book.imbalance or 0.0,
            basis_z=z, loop_lag_ms=loop_lag_ms,
            book_age_ms=book.age(time.monotonic()) * 1e3,
            feed_lag_ms=((recv - exch_ts) * 1e3) if exch_ts else None,
            expected_move_1sigma_bps=sigma_bps,
            expected_slippage_center_bps=centre,
            expected_slippage_low_bps=low,
            expected_slippage_high_bps=high,
        )
        self.sim.submit(o)

    def on_fill(self, fill: Fill) -> None:
        signed = fill.qty if fill.order.side == "buy" else -fill.qty
        key = "spot" if fill.order.venue == "spot" else "fut"
        self.pos[key] += signed


# --------------------------------------------------------------------------
# Slippage decomposition
# --------------------------------------------------------------------------

def decompose(fill: Fill, cfg: Config) -> dict:
    """Implementation-shortfall style decomposition, in basis points.

    total = fill price vs the mid at the moment of the decision
          = delay  (mid moved between decision and arrival at the engine)
          + spread (half-spread paid to cross)
          + impact (walked past the touch)

    Positive numbers are costs.
    """
    o = fill.order
    sign = 1.0 if o.side == "buy" else -1.0
    ref = o.arrival_mid

    total = sign * (fill.price - ref) / ref * 1e4
    half_spread = o.arrival_spread_bps / 2.0
    touch = o.arrival_ask if o.side == "buy" else o.arrival_bid
    # what the touch price implied at decision time
    touch_cost = sign * (touch - ref) / ref * 1e4
    # everything not explained by the touch we saw is delay + impact
    residual = total - touch_cost

    fee = cfg.fee_taker_bps if o.kind == "taker" else cfg.fee_maker_bps

    return {
        "slippage_total_bps": round(total, 4),
        "half_spread_bps": round(half_spread, 4),
        "touch_cost_bps": round(touch_cost, 4),
        "delay_plus_impact_bps": round(residual, 4),
        "fee_bps": fee,
        "all_in_bps": round(total + fee, 4),
        "latency_ms": round((o.t_effective - o.t_decision) * 1e3, 3),
        "decision_to_fill_ms": round((fill.t_fill - o.t_decision) * 1e3, 3),
        "expected_slippage_center_bps": (
            round(o.expected_slippage_center_bps, 4)
            if o.expected_slippage_center_bps is not None else None
        ),
        "expected_slippage_low_bps": (
            round(o.expected_slippage_low_bps, 4)
            if o.expected_slippage_low_bps is not None else None
        ),
        "expected_slippage_high_bps": (
            round(o.expected_slippage_high_bps, 4)
            if o.expected_slippage_high_bps is not None else None
        ),
        "expected_move_1sigma_bps": round(o.expected_move_1sigma_bps, 4),
        "slippage_surprise_bps": (
            round(total - o.expected_slippage_center_bps, 4)
            if o.expected_slippage_center_bps is not None else None
        ),
    }


# --------------------------------------------------------------------------
# Writers
# --------------------------------------------------------------------------

class Writers:
    """Durable append-only research data.

    fills.jsonl is written at fill time.  markouts.jsonl is appended later one
    horizon at a time.  events.jsonl is the forensic timeline used to reconstruct
    what surrounded a fill.  Prometheus is intentionally not the raw research
    store.
    """

    def __init__(self):
        DATA.mkdir(parents=True, exist_ok=True)
        RUNTIME.mkdir(parents=True, exist_ok=True)
        self.run_id = uuid.uuid4().hex[:12]
        self.fills = open(DATA / "fills.jsonl", "a", buffering=1)
        self.markouts = open(DATA / "markouts.jsonl", "a", buffering=1)
        self.events = open(DATA / "events.jsonl", "a", buffering=1)
        self.latency = open(DATA / "latency.jsonl", "a", buffering=1)
        self.health = open(DATA / "health.jsonl", "a", buffering=1)
        self.event("RUN_START", run_id=self.run_id, pid=os.getpid())

    def _write(self, fh, row: dict) -> None:
        row.setdefault("run_id", self.run_id)
        fh.write(json.dumps(row, separators=(",", ":")) + "\n")

    def event(self, event: str, **fields) -> None:
        row = {"ts_local": time.time(), "event": event}
        row.update(fields)
        self._write(self.events, row)

    def fill(self, fill: Fill, cfg: Config) -> None:
        """Persist every fill immediately; do not wait for future mark-outs."""
        o = fill.order
        row = {
            "ts": fill.t_fill,
            "oid": o.oid, "venue": o.venue, "side": o.side, "kind": o.kind,
            "reason": o.reason, "order_qty": round(o.qty, 8),
            "fill_qty": round(fill.qty, 8),
            "remaining_qty": round(o.remaining_qty, 8),
            "order_state": o.state,
            "fill_price": fill.price, "fill_mid": fill.fill_mid,
            "arrival_mid": o.arrival_mid, "arrival_micro": o.arrival_micro,
            "arrival_bid": o.arrival_bid, "arrival_ask": o.arrival_ask,
            "arrival_spread_bps": round(o.arrival_spread_bps, 4),
            "arrival_imbalance": round(o.arrival_imbalance, 4),
            "basis_z": round(o.basis_z, 4),
            "queue_ahead": round(o.queue_ahead, 6),
            "queue_consumed": round(o.queue_consumed, 6),
            "t_data_exch": o.t_data_exch, "t_data_recv": o.t_data_recv,
            "t_decision": o.t_decision, "t_submit": o.t_submit,
            "t_effective": o.t_effective,
            "feed_lag_ms": o.feed_lag_ms,
            "book_age_ms": round(o.book_age_ms, 3),
            "loop_lag_ms": round(o.loop_lag_ms, 4),
        }
        row.update(decompose(fill, cfg))
        self._write(self.fills, row)
        self.event(
            "FILL", oid=o.oid, venue=o.venue, side=o.side, kind=o.kind,
            reason=o.reason, qty=fill.qty, price=fill.price, fill_mid=fill.fill_mid,
            decision_to_fill_ms=row["decision_to_fill_ms"],
            slippage_total_bps=row["slippage_total_bps"],
            slippage_surprise_bps=row["slippage_surprise_bps"],
        )

    def markout(self, fill: Fill, label: str, value: Optional[float]) -> None:
        row = {
            "ts": time.time(), "oid": fill.order.oid,
            "venue": fill.order.venue, "side": fill.order.side,
            "kind": fill.order.kind, "reason": fill.order.reason,
            "fill_ts": fill.t_fill, "horizon": label, "markout_bps": value,
        }
        self._write(self.markouts, row)
        self.event("MARKOUT", oid=fill.order.oid, venue=fill.order.venue,
                   horizon=label, markout_bps=value)

    def latency_row(self, hist: Dict[str, List[int]], edges: List[float]) -> None:
        if not hist:
            return
        self._write(self.latency, {
            "ts": time.time(), "edges": edges, "stages": hist})

    def health_row(self, snap: dict) -> None:
        self._write(self.health, snap)

    def close(self):
        self.event("RUN_STOP", run_id=self.run_id)
        for f in (self.fills, self.markouts, self.events, self.latency, self.health):
            try:
                f.close()
            except Exception:
                pass



class PrometheusBridge:
    """Small live-monitoring surface; raw fill/tick research stays in JSONL."""

    def __init__(self, cfg: Config):
        self.enabled = bool(cfg.prometheus_enabled and CollectorRegistry is not None)
        if not self.enabled:
            if cfg.prometheus_enabled and CollectorRegistry is None:
                print("[metrics] prometheus_client not installed; metrics endpoint disabled",
                      file=sys.stderr, flush=True)
            return
        self.registry = CollectorRegistry()
        self.feed_age = Gauge(
            "fillquality_feed_age_seconds", "Age of latest market-data update",
            ["venue"], registry=self.registry)
        self.feed_lag = Gauge(
            "fillquality_feed_lag_ms", "Latest exchange timestamp to local receive lag",
            ["venue"], registry=self.registry)
        self.loop_lag_p99 = Gauge(
            "fillquality_event_loop_lag_p99_ms", "Event-loop lag p99 over last window",
            registry=self.registry)
        self.net_delta = Gauge(
            "fillquality_net_delta_usd", "Current simulated net delta in USD",
            registry=self.registry)
        self.gross = Gauge(
            "fillquality_gross_exposure_usd", "Current simulated gross exposure in USD",
            registry=self.registry)
        self.pending = Gauge(
            "fillquality_pending_orders", "Current simulated pending taker orders",
            registry=self.registry)
        self.resting = Gauge(
            "fillquality_resting_orders", "Current simulated resting maker orders",
            registry=self.registry)
        self.reconnects = Gauge(
            "fillquality_reconnects", "Cumulative WebSocket reconnects",
            registry=self.registry)
        self.seq_gaps = Gauge(
            "fillquality_sequence_gaps", "Cumulative detected sequence gaps",
            registry=self.registry)
        self.decode_errors = Gauge(
            "fillquality_decode_errors", "Cumulative decode failures",
            registry=self.registry)
        self.decision_to_fill = Histogram(
            "fillquality_decision_to_fill_seconds",
            "Decision-to-fill latency distribution", ["venue", "kind"],
            buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25,
                     0.5, 1.0, 2.5, 5.0, 10.0), registry=self.registry)
        start_http_server(cfg.metrics_port, registry=self.registry)
        print(f"[metrics] Prometheus endpoint on :{cfg.metrics_port}/metrics", flush=True)

    def observe_fill(self, fill: Fill) -> None:
        if not self.enabled:
            return
        self.decision_to_fill.labels(fill.order.venue, fill.order.kind).observe(
            max(fill.t_fill - fill.order.t_decision, 0.0))

    def update(self, books: Dict[str, Book], strat: Strategy, metrics: Metrics,
               sim: Simulator, snap: dict) -> None:
        if not self.enabled:
            return
        now_m = time.monotonic()
        for venue, book in books.items():
            self.feed_age.labels(venue).set(book.age(now_m))
        self.loop_lag_p99.set(snap.get("loop_lag_p99_ms") or 0.0)
        self.net_delta.set(strat.net_delta_usd())
        self.gross.set(strat.gross_exposure_usd())
        self.pending.set(len(sim.pending))
        self.resting.set(len(sim.resting))
        self.reconnects.set(metrics.reconnects)
        self.seq_gaps.set(metrics.seq_gaps)
        self.decode_errors.set(metrics.decode_errors)


def write_heartbeat(books, strat, metrics, cfg) -> None:
    now_m = time.monotonic()
    payload = {
        "timestamp": time.time(),
        "spot_age_seconds": books["spot"].age(now_m),
        "futures_age_seconds": books["fut"].age(now_m),
        "position_open": abs(strat.net_delta_usd()) > 1.0,
        "net_delta_usd": round(strat.net_delta_usd(), 2),
        "gross_exposure_usd": round(strat.gross_exposure_usd(), 2),
        "fills": len(strat.sim.fills),
        "resting_orders": len(strat.sim.resting),
        "pending_orders": len(strat.sim.pending),
        "seq_gaps": metrics.seq_gaps,
        "reconnects": metrics.reconnects,
        "decode_errors": metrics.decode_errors,
    }
    tmp = RUNTIME / "heartbeat.tmp"
    tmp.write_text(json.dumps(payload, indent=2))
    tmp.replace(RUNTIME / "heartbeat.json")


# --------------------------------------------------------------------------
# Feed handlers
# --------------------------------------------------------------------------

def parse_iso(ts: str) -> Optional[float]:
    """Kraken v2 sends RFC3339 with microseconds and a Z."""
    try:
        from datetime import datetime, timezone
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        return datetime.fromisoformat(ts).replace(tzinfo=timezone.utc).timestamp()
    except Exception:
        return None


class Engine:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.books = {"spot": Book("spot"), "fut": Book("fut")}
        self.vols = {k: EWMAVol(cfg.ewma_alpha) for k in self.books}
        self.metrics = Metrics()
        self.hist = LatencyHistogram()
        self.writers = Writers()
        self.sim = Simulator(cfg, self.books, self.metrics, self.writers.event)
        self.strat = Strategy(cfg, self.books, self.sim, self.metrics, self.vols)
        self.prom = PrometheusBridge(cfg)
        self.stop = asyncio.Event()
        self.completed: List[Fill] = []
        self._last_top: Dict[str, Tuple] = {}
        self._stale_state = {"spot": False, "fut": False}

    def _record_book_top(self, venue: str, exch_ts: Optional[float]) -> None:
        if not self.cfg.record_market_events:
            return
        book = self.books[venue]
        bb, ba = book.best_bid, book.best_ask
        if not bb or not ba:
            return
        top = (bb[0], bb[1], ba[0], ba[1])
        if self._last_top.get(venue) == top:
            return
        self._last_top[venue] = top
        self.writers.event(
            "BOOK_TOP", venue=venue, t_exchange=exch_ts,
            bid=bb[0], bid_qty=bb[1], ask=ba[0], ask_qty=ba[1],
            mid=book.mid, spread_bps=book.spread_bps,
            imbalance=book.imbalance, microprice=book.microprice(),
        )

    def _record_trade(self, venue: str, t: dict, t_exchange: Optional[float] = None) -> None:
        if not self.cfg.record_market_events:
            return
        self.writers.event(
            "TRADE", venue=venue, t_exchange=t_exchange, side=t.get("side"),
            price=t.get("price"), qty=t.get("qty"),
        )

    def _handle_fill(self, fill: Fill) -> None:
        self.strat.on_fill(fill)
        self.writers.fill(fill, self.cfg)
        self.completed.append(fill)
        self.hist.add("decision_to_fill",
                      max(fill.t_fill - fill.order.t_decision, 0.0))
        self.hist.add("decision_to_effective",
                      max(fill.order.t_effective - fill.order.t_decision, 0.0))
        self.prom.observe_fill(fill)

    # -- spot -----------------------------------------------------------
    async def run_spot(self):
        sub = {"method": "subscribe",
               "params": {"channel": "book", "symbol": [SPOT_SYMBOL], "depth": 25}}
        subt = {"method": "subscribe",
                "params": {"channel": "trade", "symbol": [SPOT_SYMBOL]}}
        while not self.stop.is_set():
            try:
                async with websockets.connect(SPOT_WS, ping_interval=20) as ws:
                    await ws.send(json.dumps(sub))
                    await ws.send(json.dumps(subt))
                    async for raw in ws:
                        if self.stop.is_set():
                            break
                        self._handle_spot(raw)
            except Exception as e:
                self.metrics.reconnects += 1
                self.writers.event("RECONNECT", venue="spot", error=str(e))
                print(f"[spot] reconnect: {e}", file=sys.stderr, flush=True)
                await asyncio.sleep(2)

    def _handle_spot(self, raw: str) -> None:
        t_recv = time.time()
        t0 = time.perf_counter_ns()
        try:
            msg = json.loads(raw)
        except Exception:
            self.metrics.decode_errors += 1
            self.writers.event("DECODE_ERROR", venue="spot")
            return
        t_parsed = time.perf_counter_ns()
        self.hist.add("decode", (t_parsed - t0) / 1e9)

        ch = msg.get("channel")
        if ch == "book":
            book = self.books["spot"]
            for d in msg.get("data", []):
                exch_ts = parse_iso(d["timestamp"]) if d.get("timestamp") else None
                if msg.get("type") == "snapshot":
                    book.replace([(b["price"], b["qty"]) for b in d.get("bids", [])],
                                 [(a["price"], a["qty"]) for a in d.get("asks", [])])
                else:
                    for b in d.get("bids", []):
                        book.apply_level("bid", b["price"], b["qty"])
                    for a in d.get("asks", []):
                        book.apply_level("ask", a["price"], a["qty"])
                mono = time.monotonic()
                book.touch(mono, exch_ts)
                self.vols["spot"].update(book.mid, mono)
                self._record_book_top("spot", exch_ts)
                if exch_ts:
                    lag = max(t_recv - exch_ts, 0.0)
                    self.hist.add("feed_lag_spot", lag)
                    if self.prom.enabled:
                        self.prom.feed_lag.labels("spot").set(lag * 1e3)
                self._decide(exch_ts, t_recv)
        elif ch == "trade":
            now = time.time()
            for t in msg.get("data", []):
                t_ex = parse_iso(t["timestamp"]) if t.get("timestamp") else None
                self._record_trade("spot", t, t_ex)
                fills = self.sim.on_trade("spot", t["side"], t["price"],
                                          t["qty"], now)
                for f in fills:
                    self._handle_fill(f)
        self.metrics.record_work(time.perf_counter_ns() - t0)
        self.hist.add("handler_total", (time.perf_counter_ns() - t0) / 1e9)

    # -- futures --------------------------------------------------------
    async def run_fut(self):
        subs = [
            {"event": "subscribe", "feed": "book", "product_ids": [FUT_SYMBOL]},
            {"event": "subscribe", "feed": "trade", "product_ids": [FUT_SYMBOL]},
        ]
        while not self.stop.is_set():
            try:
                async with websockets.connect(FUT_WS, ping_interval=20) as ws:
                    for sub in subs:
                        await ws.send(json.dumps(sub))
                    async for raw in ws:
                        if self.stop.is_set():
                            break
                        if self._handle_fut(raw):
                            # A delta gap means our local book can no longer be
                            # trusted. Reconnect so the next snapshot rebuilds it.
                            raise RuntimeError("futures sequence gap; resnapshot required")
            except Exception as e:
                self.metrics.reconnects += 1
                self.writers.event("RECONNECT", venue="fut", error=str(e))
                print(f"[fut] reconnect: {e}", file=sys.stderr, flush=True)
                await asyncio.sleep(2)

    def _handle_fut(self, raw: str) -> bool:
        t_recv = time.time()
        t0 = time.perf_counter_ns()
        try:
            msg = json.loads(raw)
        except Exception:
            self.metrics.decode_errors += 1
            self.writers.event("DECODE_ERROR", venue="fut")
            return False
        self.hist.add("decode", (time.perf_counter_ns() - t0) / 1e9)

        # Subscription acknowledgements also include a ``feed`` field (for
        # example {"event": "subscribed", "feed": "book", ...}) but are
        # not market-data deltas and therefore have no price/qty fields.
        # Ignore/control-handle them before dispatching on ``feed``.
        event = msg.get("event")
        if event is not None:
            self.writers.event("FUT_WS_CONTROL", venue="fut",
                               ws_event=event, feed=msg.get("feed"),
                               message=msg.get("message"))
            if event in ("error", "subscribed_failed"):
                print(f"[fut] websocket control error: {msg}",
                      file=sys.stderr, flush=True)
            return False

        feed = msg.get("feed")
        book = self.books["fut"]
        gap = False

        if feed == "book_snapshot":
            book.replace([(b["price"], b["qty"]) for b in msg.get("bids", [])],
                         [(a["price"], a["qty"]) for a in msg.get("asks", [])])
            book.seq = msg.get("seq")
            exch_ts = msg.get("timestamp", 0) / 1000.0 or None
            mono = time.monotonic()
            book.touch(mono, exch_ts)
            self.vols["fut"].update(book.mid, mono)
            self._record_book_top("fut", exch_ts)
            self._decide(exch_ts, t_recv)

        elif feed == "book":
            if book.check_seq(msg.get("seq")):
                self.metrics.seq_gaps += 1
                gap = True
                self.writers.event("SEQ_GAP", venue="fut", seq=msg.get("seq"))
                print("[fut] SEQUENCE GAP -> invalidate/resnapshot",
                      file=sys.stderr, flush=True)
            if not gap:
                side = "bid" if msg.get("side") == "buy" else "ask"
                book.apply_level(side, msg["price"], msg["qty"])
                exch_ts = msg.get("timestamp", 0) / 1000.0 or None
                mono = time.monotonic()
                book.touch(mono, exch_ts)
                self.vols["fut"].update(book.mid, mono)
                self._record_book_top("fut", exch_ts)
                if exch_ts:
                    lag = max(t_recv - exch_ts, 0.0)
                    self.hist.add("feed_lag_fut", lag)
                    if self.prom.enabled:
                        self.prom.feed_lag.labels("fut").set(lag * 1e3)
                self._decide(exch_ts, t_recv)

        elif feed == "trade_snapshot":
            # Kraken sends an initial snapshot as a wrapper containing a
            # ``trades`` array before subsequent single ``trade`` deltas.
            for trade in msg.get("trades", []):
                t_ex = trade.get("time") or trade.get("timestamp")
                if isinstance(t_ex, (int, float)) and t_ex > 1e12:
                    t_ex = t_ex / 1000.0
                self._record_trade("fut", trade,
                                   t_ex if isinstance(t_ex, (int, float)) else None)
                if all(k in trade for k in ("price", "qty")):
                    fills = self.sim.on_trade(
                        "fut", trade.get("side", "buy"),
                        trade["price"], trade["qty"], time.time())
                    for f in fills:
                        self._handle_fill(f)

        elif feed == "trade":
            # Defensive field check: malformed/control messages should be
            # logged, not crash the entire futures WebSocket task.
            if not all(k in msg for k in ("price", "qty")):
                self.metrics.decode_errors += 1
                self.writers.event("FUT_TRADE_MALFORMED", venue="fut", raw=msg)
            else:
                t_ex = msg.get("time") or msg.get("timestamp")
                if isinstance(t_ex, (int, float)) and t_ex > 1e12:
                    t_ex = t_ex / 1000.0
                self._record_trade("fut", msg,
                                   t_ex if isinstance(t_ex, (int, float)) else None)
                fills = self.sim.on_trade("fut", msg.get("side", "buy"),
                                          msg["price"], msg["qty"], time.time())
                for f in fills:
                    self._handle_fill(f)

        self.metrics.record_work(time.perf_counter_ns() - t0)
        self.hist.add("handler_total", (time.perf_counter_ns() - t0) / 1e9)
        return gap

    def _decide(self, exch_ts: Optional[float], t_recv: float) -> None:
        t0 = time.perf_counter_ns()
        lag = (self.metrics.loop_lag_samples[-1] * 1e3
               if self.metrics.loop_lag_samples else 0.0)
        self.strat.evaluate(exch_ts, t_recv, lag)
        self.hist.add("strategy", (time.perf_counter_ns() - t0) / 1e9)

    # -- clocks ---------------------------------------------------------
    async def run_clock(self):
        """Drive simulated arrival/fills/mark-outs and measure event-loop lag."""
        interval = 0.01
        while not self.stop.is_set():
            target = time.monotonic() + interval
            await asyncio.sleep(interval)
            self.metrics.loop_lag_samples.append(max(time.monotonic() - target, 0.0))

            now = time.time()
            for f in self.sim.on_clock(now):
                self._handle_fill(f)
            for fill, label, value in self.sim.resolve_markouts(now):
                self.writers.markout(fill, label, value)

    async def run_telemetry(self):
        while not self.stop.is_set():
            await asyncio.sleep(1.0)
            self.writers.latency_row(self.hist.flush(), LatencyHistogram.EDGES)
            snap = self.metrics.snapshot()
            snap["ts"] = time.time()
            for name, b in self.books.items():
                age = b.age(time.monotonic())
                snap[f"{name}_age_s"] = round(age, 3)
                stale = age > self.cfg.stale_threshold
                if stale and not self._stale_state[name]:
                    self.metrics.stale_events += 1
                    self.writers.event("STALE_FEED", venue=name, age_seconds=age)
                elif not stale and self._stale_state[name]:
                    self.writers.event("FEED_RECOVERED", venue=name, age_seconds=age)
                self._stale_state[name] = stale
            snap["net_delta_usd"] = round(self.strat.net_delta_usd(), 2)
            snap["gross_exposure_usd"] = round(self.strat.gross_exposure_usd(), 2)
            snap["fills_total"] = len(self.sim.fills)
            snap["pending_orders"] = len(self.sim.pending)
            snap["resting_orders"] = len(self.sim.resting)
            self.writers.health_row(snap)
            write_heartbeat(self.books, self.strat, self.metrics, self.cfg)
            self.prom.update(self.books, self.strat, self.metrics, self.sim, snap)

    async def run_console(self):
        while not self.stop.is_set():
            await asyncio.sleep(15.0)
            b = self.strat.basis_bps()
            z = self.strat.basis_z()
            print(f"[{time.strftime('%H:%M:%S')}] "
                  f"basis={b:+.2f}bps " if b is not None else "[--] basis=n/a ",
                  end="")
            print(f"z={z:+.2f} " if z is not None else "z=n/a ", end="")
            print(f"fills={len(self.sim.fills)} "
                  f"delta=${self.strat.net_delta_usd():+.0f} "
                  f"gross=${self.strat.gross_exposure_usd():.0f} "
                  f"gaps={self.metrics.seq_gaps} "
                  f"reconn={self.metrics.reconnects}", flush=True)

    async def run(self):
        tasks = [asyncio.create_task(c) for c in (
            self.run_spot(), self.run_fut(), self.run_clock(),
            self.run_telemetry(), self.run_console())]
        try:
            await asyncio.wait_for(self.stop.wait(),
                                   timeout=self.cfg.minutes * 60)
        except asyncio.TimeoutError:
            pass
        self.stop.set()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.writers.close()
        self.summary()

    def summary(self) -> None:
        rows = self.completed
        print(f"\n=== {len(rows)} fills persisted immediately ===")
        if not rows:
            print("No fills. Loosen taker_z / probe interval or lengthen the run.")
            return
        for kind in ("taker", "maker"):
            sub = [f for f in rows if f.order.kind == kind]
            if not sub:
                continue
            slip = [decompose(f, self.cfg)["slippage_total_bps"] for f in sub]
            print(f"{kind:6s} n={len(sub):4d} "
                  f"slip_med={statistics.median(slip):+.3f}bps "
                  f"slip_p90={_pct(slip, 90):+.3f}bps")
        print("\nRaw research data: ./data/fills.jsonl, markouts.jsonl, "
              "events.jsonl, latency.jsonl, health.jsonl")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--minutes", type=float, default=60.0)
    p.add_argument("--taker-z", type=float, default=1.2)
    p.add_argument("--clip-usd", type=float, default=250.0)
    p.add_argument("--probe-secs", type=float, default=90.0)
    p.add_argument("--quote-refresh", type=float, default=3.0)
    p.add_argument("--sim-latency-ms", type=float, default=40.0)
    p.add_argument("--max-gross-usd", type=float, default=5000.0)
    p.add_argument("--metrics-port", type=int, default=9108)
    p.add_argument("--no-prometheus", action="store_true")
    p.add_argument("--no-market-events", action="store_true")
    p.add_argument("--seed", type=int, default=None)
    a = p.parse_args()

    cfg = Config(minutes=a.minutes, taker_z=a.taker_z, clip_usd=a.clip_usd,
                 probe_secs=a.probe_secs, quote_refresh=a.quote_refresh,
                 sim_latency_ms=a.sim_latency_ms, max_gross_usd=a.max_gross_usd,
                 metrics_port=a.metrics_port, prometheus_enabled=not a.no_prometheus,
                 record_market_events=not a.no_market_events, seed=a.seed)
    if cfg.seed is not None:
        random.seed(cfg.seed)

    eng = Engine(cfg)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, eng.stop.set)
        except NotImplementedError:
            pass
    try:
        loop.run_until_complete(eng.run())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
