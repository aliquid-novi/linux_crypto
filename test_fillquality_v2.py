import sys
import time
import random
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import fillquality_v2 as fq

random.seed(7)


def ok(condition, message):
    if not condition:
        raise AssertionError(message)
    print("PASS", message)


def fresh_book(name, bids, asks):
    b = fq.Book(name)
    b.replace(bids, asks)
    b.touch(time.monotonic(), time.time())
    return b


# ---- Book basics ---------------------------------------------------------
b = fresh_book("t", [(100.0, 2.0), (99.0, 5.0)], [(101.0, 3.0), (102.0, 4.0)])
ok(b.best_bid == (100.0, 2.0), "best_bid")
ok(b.best_ask == (101.0, 3.0), "best_ask")
ok(b.mid == 100.5, "mid")
ok(abs(b.spread_bps - (1.0 / 100.5 * 1e4)) < 1e-9, "spread_bps")
ok(abs(b.imbalance - ((2 - 3) / 5)) < 1e-12, "imbalance")
mp = b.microprice()
ok(99.0 < mp < 102.0 and mp < b.mid, "microprice leans to thin bid side")

# ---- sweep across levels ------------------------------------------------
vwap, filled = b.sweep("buy", 4.0)  # 3 @101 + 1 @102
ok(abs(filled - 4.0) < 1e-12, "sweep fills full visible size")
ok(abs(vwap - (3 * 101 + 1 * 102) / 4) < 1e-9, "sweep VWAP walks book")
vwap2, f2 = b.sweep("buy", 100.0)
ok(abs(f2 - 7.0) < 1e-12, "sweep caps at displayed depth")

# ---- sequence gaps ------------------------------------------------------
g = fq.Book("g")
g.check_seq(10)
ok(not g.check_seq(11), "in-order sequence")
ok(g.check_seq(15), "sequence gap flagged")
ok(g.gaps == 1, "sequence-gap counter")

# ---- online volatility / ex-ante movement band -------------------------
v = fq.EWMAVol(alpha=0.5)
t0 = time.monotonic()
v.update(100.0, t0)
v.update(100.1, t0 + 0.1)
v.update(100.05, t0 + 0.2)
ok(v.sigma_bps(0.05) > 0, "EWMA short-horizon movement estimate")

# ---- Simulator: taker fills against later book -------------------------
cfg = fq.Config(sim_latency_ms=50.0, sim_latency_jitter_ms=0.0,
                prometheus_enabled=False)
books = {
    "spot": fresh_book("spot", [(100.0, 10.0)], [(101.0, 10.0)]),
    "fut": fresh_book("fut", [(100.0, 10.0)], [(101.0, 10.0)]),
}
m = fq.Metrics()
sim = fq.Simulator(cfg, books, m)

o = fq.Order(
    oid=1, venue="spot", side="buy", kind="taker", qty=1.0,
    limit=None, reason="test", arrival_mid=100.5,
    arrival_bid=100.0, arrival_ask=101.0,
    arrival_spread_bps=books["spot"].spread_bps or 0.0,
    t_decision=time.time(),
)
sim.submit(o)
ok(sim.on_clock(time.time()) == [], "no taker fill before simulated latency")
books["spot"].replace([(103.0, 10.0)], [(104.0, 10.0)])
fills = sim.on_clock(o.t_effective + 0.001)
ok(len(fills) == 1, "taker fills after latency window")
f = fills[0]
ok(abs(f.price - 104.0) < 1e-9, "taker fills against later ask")
d = fq.decompose(f, cfg)
ok(d["slippage_total_bps"] > 300, "adverse move appears as positive slippage")
ok(abs(d["latency_ms"] - 50.0) < 1.0, "simulated latency recorded")

# displayed-depth shortage is an IOC-style partial, not silently retried
books["spot"].replace([(100.0, 1.0)], [(101.0, 0.25)])
o_small = fq.Order(
    oid=2, venue="spot", side="buy", kind="taker", qty=1.0,
    limit=None, reason="test", arrival_mid=100.5,
    arrival_bid=100.0, arrival_ask=101.0,
    arrival_spread_bps=books["spot"].spread_bps or 0.0,
    t_decision=time.time(),
)
sim.submit(o_small)
f_partial = sim.on_clock(o_small.t_effective + 0.001)[0]
ok(abs(f_partial.qty - 0.25) < 1e-12, "taker partial uses visible depth only")
ok(o_small.remaining_qty > 0, "unfilled taker remainder retained in state")
ok(o_small not in sim.pending, "IOC remainder is not retried on a later book")

# ---- maker queue model --------------------------------------------------
books["spot"] = fresh_book("spot", [(100.0, 5.0)], [(101.0, 5.0)])
sim.books = books
om = fq.Order(
    oid=3, venue="spot", side="buy", kind="maker", qty=1.0,
    limit=100.0, reason="quote", arrival_mid=100.5,
    arrival_bid=100.0, arrival_ask=101.0, t_decision=time.time(),
)
sim.submit(om)
ok(not om.queue_initialized, "maker queue not measured before order is effective")
t = om.t_effective + 0.001
ok(sim.on_trade("spot", "sell", 100.0, 2.0, t) == [], "maker remains queued")
ok(om.queue_initialized and abs(om.queue_ahead - 5.0) < 1e-9,
   "maker queue measured at simulated effective time")
ok(sim.on_trade("spot", "sell", 100.0, 2.5, t) == [], "still queued after 4.5")
got = sim.on_trade("spot", "sell", 100.0, 2.0, t)
ok(len(got) == 1 and abs(got[0].qty - 1.0) < 1e-12, "queue exhaustion fills maker")
ok(abs(got[0].price - 100.0) < 1e-9, "maker fills at limit")

# better-price trades are volume ahead of our lower bid
books["spot"] = fresh_book("spot", [(100.0, 2.0), (99.0, 2.0)], [(101.0, 5.0)])
sim.books = books
om2 = fq.Order(
    oid=4, venue="spot", side="buy", kind="maker", qty=1.0,
    limit=99.0, reason="quote", arrival_mid=100.0,
    arrival_bid=100.0, arrival_ask=101.0, t_decision=time.time(),
)
sim.submit(om2)
t2 = om2.t_effective + 0.001
sim.on_trade("spot", "sell", 100.0, 2.0, t2)
ok(om2.queue_consumed >= 2.0, "better-price trade consumes priority ahead")

# ---- mark-outs resolve one horizon at a time ---------------------------
books["spot"].replace([(110.0, 5.0)], [(111.0, 5.0)])
sim.pending_markouts = [(got[0], time.time() - 1, "1.0s")]
resolved = sim.resolve_markouts(time.time())
ok(len(resolved) == 1, "markout resolved immediately at due horizon")
_, label, mo = resolved[0]
ok(label == "1.0s" and mo is not None and mo > 0, "buy + price up -> positive markout")

# ---- latency histogram -------------------------------------------------
h = fq.LatencyHistogram()
for val in (5e-6, 50e-6, 1e-3, 0.5, 30.0):
    h.add("x", val)
buckets = h.flush()["x"]
ok(sum(buckets) == 5, "all latency samples bucketed")
ok(buckets[0] == 1, "underflow latency bucket")
ok(buckets[-1] == 1, "overflow latency bucket")

# ---- strategy risk state ----------------------------------------------
books = {
    "spot": fresh_book("spot", [(100.0, 5.0)], [(101.0, 5.0)]),
    "fut": fresh_book("fut", [(100.0, 5.0)], [(101.0, 5.0)]),
}
cfg_risk = fq.Config(sim_latency_ms=10.0, sim_latency_jitter_ms=0.0,
                     max_net_delta_usd=50.0, max_gross_usd=10_000.0,
                     prometheus_enabled=False, probe_enabled=False)
m2 = fq.Metrics()
sim2 = fq.Simulator(cfg_risk, books, m2)
s2 = fq.Strategy(cfg_risk, books, sim2, m2)
s2.pos["spot"] = 1.0
ok(s2.net_delta_usd() > 50.0, "long spot breaches net-delta limit")
ok(s2.gross_exposure_usd() > 100.0, "gross exposure measured separately")
s2.evaluate(time.time(), time.time(), 0.0)  # z-score is not warm
ok(any(o.reason == "flatten" for o in sim2.pending),
   "risk flatten can fire before alpha z-score is warm")

# ---- expected slippage benchmark --------------------------------------
for venue in ("spot", "fut"):
    vv = s2.vols[venue]
    tm = time.monotonic()
    vv.update(100.0, tm)
    vv.update(100.2, tm + 0.1)
# seed basis history so signal/utility paths can use z without returning None
for _ in range(100):
    s2.basis_hist.append(random.gauss(0.0, 1.0))
pre_n = len(sim2.pending)
s2._send("spot", "buy", "taker", "test_band", 0.0,
         time.time(), time.time(), 0.0)
band_order = sim2.pending[-1]
ok(len(sim2.pending) == pre_n + 1, "manual test order submitted")
ok(band_order.expected_slippage_center_bps is not None,
   "ex-ante slippage centre recorded")
ok(band_order.expected_slippage_high_bps >= band_order.expected_slippage_low_bps,
   "ex-ante uncertainty band recorded without good/bad label")

# ---- ISO parse ---------------------------------------------------------
ts = fq.parse_iso("2026-08-15T03:04:05.123456Z")
ok(ts is not None and 1.7e9 < ts < 2.1e9, "RFC3339 parse")

print("\nall tests passed")
