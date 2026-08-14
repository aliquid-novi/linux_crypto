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
    fills.jsonl      one row per fill: full timestamp chain, slippage
                     decomposition, book state at decision, mark-outs
    latency.jsonl    per-second bucketed histograms of pipeline latency,
                     ready to render as a Gregg-style heat map
    health.jsonl     per-second USE metrics: loop lag, queue depth, drops,
                     reconnects, sequence gaps, decode errors
    heartbeat.json   liveness for an external monitor (same shape as the
                     linux_crypto project's checker expects)
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
from collections import deque
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Deque, Dict, List, Optional, Tuple

try:
    import websockets
except ImportError:
    sys.exit("pip install websockets")

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
    cooldown: float = 1.5            # min seconds between taker trades

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

    # health at decision — the join between ops and execution quality
    loop_lag_ms: float = 0.0
    book_age_ms: float = 0.0
    feed_lag_ms: Optional[float] = None

    state: str = "live"     # live | filled | cancelled


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
    def __init__(self, cfg: Config, books: Dict[str, Book], metrics: Metrics):
        self.cfg = cfg
        self.books = books
        self.metrics = metrics
        self.pending: List[Order] = []      # taker orders in flight
        self.resting: List[Order] = []      # maker orders on the book
        self.fills: List[Fill] = []
        self.pending_markouts: List[Tuple[Fill, float, str]] = []
        self._oid = 0

    def next_oid(self) -> int:
        self._oid += 1
        return self._oid

    # -- submission -----------------------------------------------------
    def submit(self, order: Order) -> None:
        order.t_submit = time.time()
        lat = max(0.0, random.gauss(self.cfg.sim_latency_ms,
                                    self.cfg.sim_latency_jitter_ms)) / 1e3
        order.t_effective = order.t_submit + lat
        if order.kind == "taker":
            self.pending.append(order)
        else:
            book = self.books[order.venue]
            order.queue_ahead = book.depth_at_or_better(order.side, order.limit)
            self.resting.append(order)

    def cancel_all_makers(self, venue: Optional[str] = None) -> None:
        keep = []
        for o in self.resting:
            if venue is None or o.venue == venue:
                o.state = "cancelled"
            else:
                keep.append(o)
        self.resting = keep

    # -- the tick that makes slippage real ------------------------------
    def on_clock(self, now: float) -> List[Fill]:
        """Fill any taker order whose latency window has elapsed, against the
        book AS IT IS NOW — not as it was when the decision was made."""
        out = []
        still = []
        for o in self.pending:
            if now < o.t_effective:
                still.append(o)
                continue
            book = self.books[o.venue]
            res = book.sweep(o.side, o.qty)
            if res is None:
                still.append(o)     # book empty; try again next tick
                continue
            vwap, filled = res
            fill = self._record(o, now, vwap, filled)
            out.append(fill)
        self.pending = still
        return out

    def on_trade(self, venue: str, side: str, price: float,
                 qty: float, now: float) -> List[Fill]:
        """Real market trades consume the queue ahead of our resting orders.

        `side` is the aggressor side. A market sell (aggressor sell) consumes
        resting bids; a market buy consumes resting asks.
        """
        out, still = [], []
        for o in self.resting:
            if o.venue != venue or now < o.t_effective:
                still.append(o)
                continue
            consumes = (o.side == "buy" and side == "sell" and price <= o.limit) or \
                       (o.side == "sell" and side == "buy" and price >= o.limit)
            if not consumes:
                still.append(o)
                continue
            o.queue_consumed += qty
            if o.queue_consumed >= o.queue_ahead + o.qty:
                out.append(self._record(o, now, o.limit, o.qty))
            elif o.queue_consumed > o.queue_ahead:
                # partial: fill what got through
                got = min(o.qty, o.queue_consumed - o.queue_ahead)
                out.append(self._record(o, now, o.limit, got))
                o.qty -= got
                if o.qty > 1e-9:
                    still.append(o)
            else:
                still.append(o)
        self.resting = still
        return out

    def _record(self, o: Order, now: float, price: float, qty: float) -> Fill:
        o.state = "filled"
        book = self.books[o.venue]
        fill = Fill(order=o, t_fill=now, price=price, qty=qty,
                    fill_mid=book.mid or price)
        self.fills.append(fill)
        for h in self.cfg.markout_horizons:
            self.pending_markouts.append((fill, now + h, f"{h}s"))
        return fill

    def resolve_markouts(self, now: float) -> List[Fill]:
        """Adverse selection: where did the mid go after we traded?
        Signed so that positive = the fill looks good in hindsight."""
        done, still = [], []
        for fill, due, label in self.pending_markouts:
            if now < due:
                still.append((fill, due, label))
                continue
            book = self.books[fill.order.venue]
            mid = book.mid
            if mid is None:
                fill.markouts[label] = None
            else:
                sign = 1.0 if fill.order.side == "buy" else -1.0
                fill.markouts[label] = sign * (mid - fill.price) / fill.price * 1e4
            if len(fill.markouts) == len(self.cfg.markout_horizons):
                done.append(fill)
        self.pending_markouts = still
        return done


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
                 metrics: Metrics):
        self.cfg = cfg
        self.books = books
        self.sim = sim
        self.metrics = metrics
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

    # -- decision -------------------------------------------------------
    def evaluate(self, trigger_exch_ts: Optional[float],
                 trigger_recv: float, loop_lag_ms: float) -> None:
        now = time.time()
        b = self.basis_bps()
        if b is not None:
            self.basis_hist.append(b)

        z = self.basis_z()
        if z is None:
            return

        # 1. signal-driven taker trades
        if abs(z) >= self.cfg.taker_z and now - self.last_taker >= self.cfg.cooldown:
            if abs(self.net_delta_usd()) < self.cfg.max_net_delta_usd:
                # perp rich (z>0): sell perp, buy spot
                fut_side = "sell" if z > 0 else "buy"
                spot_side = "buy" if z > 0 else "sell"
                self._send("fut", fut_side, "taker", "signal", z,
                           trigger_exch_ts, trigger_recv, loop_lag_ms)
                self._send("spot", spot_side, "taker", "signal", z,
                           trigger_exch_ts, trigger_recv, loop_lag_ms)
                self.last_taker = now

        # 2. unconditional probe trades — the control group
        if self.cfg.probe_enabled and now >= self.next_probe:
            venue = random.choice(["spot", "fut"])
            side = random.choice(["buy", "sell"])
            self._send(venue, side, "taker", "probe", z,
                       trigger_exch_ts, trigger_recv, loop_lag_ms)
            self.next_probe = now + random.expovariate(1.0 / self.cfg.probe_secs)

        # 3. delta band breach -> flatten toward neutral
        nd = self.net_delta_usd()
        if abs(nd) > self.cfg.max_net_delta_usd:
            venue = "fut"
            side = "sell" if nd > 0 else "buy"
            self._send(venue, side, "taker", "flatten", z,
                       trigger_exch_ts, trigger_recv, loop_lag_ms)

        # 4. passive quotes, refreshed on a timer
        if now - self.last_quote >= self.cfg.quote_refresh:
            self.sim.cancel_all_makers()
            for venue in ("spot", "fut"):
                book = self.books[venue]
                mid = book.mid
                if not mid:
                    continue
                off = mid * self.cfg.quote_offset_bps / 1e4
                for side, px in (("buy", mid - off), ("sell", mid + off)):
                    self._send(venue, side, "maker", "quote", z,
                               trigger_exch_ts, trigger_recv, loop_lag_ms,
                               limit=round(px, 1))
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
        o = Order(
            oid=self.sim.next_oid(), venue=venue, side=side, kind=kind,
            qty=self.cfg.clip_usd / mid, limit=limit, reason=reason,
            t_data_exch=exch_ts, t_data_recv=recv, t_decision=now,
            arrival_mid=mid, arrival_micro=book.microprice() or mid,
            arrival_bid=bb[0], arrival_ask=ba[0],
            arrival_spread_bps=book.spread_bps or 0.0,
            arrival_imbalance=book.imbalance or 0.0,
            basis_z=z, loop_lag_ms=loop_lag_ms,
            book_age_ms=book.age(time.monotonic()) * 1e3,
            feed_lag_ms=((recv - exch_ts) * 1e3) if exch_ts else None,
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
    }


# --------------------------------------------------------------------------
# Writers
# --------------------------------------------------------------------------

class Writers:
    def __init__(self):
        DATA.mkdir(parents=True, exist_ok=True)
        RUNTIME.mkdir(parents=True, exist_ok=True)
        self.fills = open(DATA / "fills.jsonl", "a", buffering=1)
        self.latency = open(DATA / "latency.jsonl", "a", buffering=1)
        self.health = open(DATA / "health.jsonl", "a", buffering=1)

    def fill(self, fill: Fill, cfg: Config) -> None:
        o = fill.order
        row = {
            "ts": fill.t_fill,
            "oid": o.oid, "venue": o.venue, "side": o.side, "kind": o.kind,
            "reason": o.reason, "qty": round(o.qty, 8),
            "fill_price": fill.price, "fill_mid": fill.fill_mid,
            "arrival_mid": o.arrival_mid, "arrival_micro": o.arrival_micro,
            "arrival_spread_bps": round(o.arrival_spread_bps, 4),
            "arrival_imbalance": round(o.arrival_imbalance, 4),
            "basis_z": round(o.basis_z, 4),
            "queue_ahead": round(o.queue_ahead, 6),
            "t_data_exch": o.t_data_exch, "t_data_recv": o.t_data_recv,
            "t_decision": o.t_decision, "t_submit": o.t_submit,
            "t_effective": o.t_effective,
            "feed_lag_ms": o.feed_lag_ms,
            "book_age_ms": round(o.book_age_ms, 3),
            "loop_lag_ms": round(o.loop_lag_ms, 4),
            "markouts_bps": fill.markouts,
        }
        row.update(decompose(fill, cfg))
        self.fills.write(json.dumps(row) + "\n")

    def latency_row(self, hist: Dict[str, List[int]], edges: List[float]) -> None:
        if not hist:
            return
        self.latency.write(json.dumps({
            "ts": time.time(), "edges": edges, "stages": hist}) + "\n")

    def health_row(self, snap: dict) -> None:
        self.health.write(json.dumps(snap) + "\n")

    def close(self):
        for f in (self.fills, self.latency, self.health):
            try:
                f.close()
            except Exception:
                pass


def write_heartbeat(books, strat, metrics, cfg) -> None:
    now_m = time.monotonic()
    payload = {
        "timestamp": time.time(),
        "spot_age_seconds": books["spot"].age(now_m),
        "futures_age_seconds": books["fut"].age(now_m),
        "position_open": abs(strat.net_delta_usd()) > 1.0,
        "net_delta_usd": round(strat.net_delta_usd(), 2),
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
        self.metrics = Metrics()
        self.hist = LatencyHistogram()
        self.sim = Simulator(cfg, self.books, self.metrics)
        self.strat = Strategy(cfg, self.books, self.sim, self.metrics)
        self.writers = Writers()
        self.stop = asyncio.Event()
        self.completed: List[Fill] = []

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
                print(f"[spot] reconnect: {e}", file=sys.stderr, flush=True)
                await asyncio.sleep(2)

    def _handle_spot(self, raw: str) -> None:
        t_recv = time.time()
        t0 = time.perf_counter_ns()
        try:
            msg = json.loads(raw)
        except Exception:
            self.metrics.decode_errors += 1
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
                book.touch(time.monotonic(), exch_ts)
                if exch_ts:
                    self.hist.add("feed_lag_spot", max(t_recv - exch_ts, 0.0))
                self._decide(exch_ts, t_recv)
        elif ch == "trade":
            now = time.time()
            for t in msg.get("data", []):
                fills = self.sim.on_trade("spot", t["side"], t["price"],
                                          t["qty"], now)
                for f in fills:
                    self.strat.on_fill(f)
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
                    for s in subs:
                        await ws.send(json.dumps(s))
                    async for raw in ws:
                        if self.stop.is_set():
                            break
                        self._handle_fut(raw)
            except Exception as e:
                self.metrics.reconnects += 1
                print(f"[fut] reconnect: {e}", file=sys.stderr, flush=True)
                await asyncio.sleep(2)

    def _handle_fut(self, raw: str) -> None:
        t_recv = time.time()
        t0 = time.perf_counter_ns()
        try:
            msg = json.loads(raw)
        except Exception:
            self.metrics.decode_errors += 1
            return
        self.hist.add("decode", (time.perf_counter_ns() - t0) / 1e9)

        feed = msg.get("feed")
        book = self.books["fut"]

        if feed == "book_snapshot":
            book.replace([(b["price"], b["qty"]) for b in msg.get("bids", [])],
                         [(a["price"], a["qty"]) for a in msg.get("asks", [])])
            book.seq = msg.get("seq")
            exch_ts = msg.get("timestamp", 0) / 1000.0 or None
            book.touch(time.monotonic(), exch_ts)
            self._decide(exch_ts, t_recv)

        elif feed == "book":
            if book.check_seq(msg.get("seq")):
                self.metrics.seq_gaps += 1
                print("[fut] SEQUENCE GAP", file=sys.stderr, flush=True)
            side = "bid" if msg.get("side") == "buy" else "ask"
            book.apply_level(side, msg["price"], msg["qty"])
            exch_ts = msg.get("timestamp", 0) / 1000.0 or None
            book.touch(time.monotonic(), exch_ts)
            if exch_ts:
                self.hist.add("feed_lag_fut", max(t_recv - exch_ts, 0.0))
            self._decide(exch_ts, t_recv)

        elif feed == "trade":
            fills = self.sim.on_trade("fut", msg.get("side", "buy"),
                                      msg["price"], msg["qty"], time.time())
            for f in fills:
                self.strat.on_fill(f)

        self.metrics.record_work(time.perf_counter_ns() - t0)
        self.hist.add("handler_total", (time.perf_counter_ns() - t0) / 1e9)

    def _decide(self, exch_ts: Optional[float], t_recv: float) -> None:
        t0 = time.perf_counter_ns()
        lag = (self.metrics.loop_lag_samples[-1] * 1e3
               if self.metrics.loop_lag_samples else 0.0)
        self.strat.evaluate(exch_ts, t_recv, lag)
        self.hist.add("strategy", (time.perf_counter_ns() - t0) / 1e9)

    # -- clocks ---------------------------------------------------------
    async def run_clock(self):
        """Drives taker fills and mark-outs. Also measures event-loop lag:
        the gap between when we asked to be woken and when we actually were.
        This is the saturation metric."""
        interval = 0.01
        while not self.stop.is_set():
            target = time.monotonic() + interval
            await asyncio.sleep(interval)
            self.metrics.loop_lag_samples.append(max(time.monotonic() - target, 0.0))

            now = time.time()
            for f in self.sim.on_clock(now):
                self.strat.on_fill(f)
            for f in self.sim.resolve_markouts(now):
                self.writers.fill(f, self.cfg)
                self.completed.append(f)

    async def run_telemetry(self):
        while not self.stop.is_set():
            await asyncio.sleep(1.0)
            self.writers.latency_row(self.hist.flush(), LatencyHistogram.EDGES)
            snap = self.metrics.snapshot()
            snap["ts"] = time.time()
            for name, b in self.books.items():
                age = b.age(time.monotonic())
                snap[f"{name}_age_s"] = round(age, 3)
                if age > self.cfg.stale_threshold:
                    self.metrics.stale_events += 1
            snap["net_delta_usd"] = round(self.strat.net_delta_usd(), 2)
            snap["fills_total"] = len(self.sim.fills)
            self.writers.health_row(snap)
            write_heartbeat(self.books, self.strat, self.metrics, self.cfg)

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
                  f"complete={len(self.completed)} "
                  f"delta=${self.strat.net_delta_usd():+.0f} "
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
        done = self.completed
        print(f"\n=== {len(done)} fills with complete mark-outs ===")
        if not done:
            print("No completed fills. Loosen taker_z or lengthen the run.")
            return
        for kind in ("taker", "maker"):
            rows = [f for f in done if f.order.kind == kind]
            if not rows:
                continue
            slip = [decompose(f, self.cfg)["slippage_total_bps"] for f in rows]
            print(f"{kind:6s} n={len(rows):4d} "
                  f"slip_med={statistics.median(slip):+.3f}bps "
                  f"slip_p90={_pct(slip, 90):+.3f}bps")
        for h in self.cfg.markout_horizons:
            lbl = f"{h}s"
            vals = [f.markouts[lbl] for f in done
                    if f.markouts.get(lbl) is not None]
            if vals:
                print(f"  markout {lbl:>5s}: median {statistics.median(vals):+.3f} bps "
                      f"(n={len(vals)})")
        print("\nRaw data in ./data/ — the analysis is the actual project.")


# --------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--minutes", type=float, default=60.0)
    p.add_argument("--taker-z", type=float, default=1.2)
    p.add_argument("--clip-usd", type=float, default=250.0)
    p.add_argument("--probe-secs", type=float, default=90.0)
    p.add_argument("--quote-refresh", type=float, default=3.0)
    p.add_argument("--sim-latency-ms", type=float, default=40.0)
    p.add_argument("--seed", type=int, default=None)
    a = p.parse_args()

    cfg = Config(minutes=a.minutes, taker_z=a.taker_z, clip_usd=a.clip_usd,
                 probe_secs=a.probe_secs, quote_refresh=a.quote_refresh,
                 sim_latency_ms=a.sim_latency_ms, seed=a.seed)
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
