import sys, time, random, json
sys.path.insert(0, "/mnt/user-data/outputs")
import fillquality as fq

random.seed(7)
ok = lambda c, m: print(("PASS " if c else "FAIL ") + m)

# ---- Book basics
b = fq.Book("t")
b.replace([(100.0, 2.0), (99.0, 5.0)], [(101.0, 3.0), (102.0, 4.0)])
ok(b.best_bid == (100.0, 2.0), "best_bid")
ok(b.best_ask == (101.0, 3.0), "best_ask")
ok(b.mid == 100.5, "mid")
ok(abs(b.spread_bps - (1.0/100.5*1e4)) < 1e-9, "spread_bps")
ok(abs(b.imbalance - ((2-3)/5)) < 1e-12, "imbalance")
mp = b.microprice()
ok(99.0 < mp < 102.0 and mp < b.mid, "microprice leans to the thin side")

# ---- sweep across levels
vwap, filled = b.sweep("buy", 4.0)          # 3 @101 + 1 @102
ok(abs(filled - 4.0) < 1e-12, "sweep fills full size")
ok(abs(vwap - (3*101 + 1*102)/4) < 1e-9, "sweep vwap walks the book")
vwap2, f2 = b.sweep("buy", 100.0)           # more than displayed
ok(abs(f2 - 7.0) < 1e-12, "sweep caps at displayed depth")

# ---- depth_at_or_better (queue-ahead estimate)
ok(b.depth_at_or_better("buy", 99.0) == 7.0, "queue ahead for a bid join")
ok(b.depth_at_or_better("sell", 101.0) == 3.0, "queue ahead for an ask join")

# ---- sequence gap detection
g = fq.Book("g")
g.check_seq(10)
ok(not g.check_seq(11), "in-order seq: no gap")
ok(g.check_seq(15), "skipped seq: gap flagged")
ok(g.gaps == 1, "gap counter")

# ---- Simulator: taker fills against the LATER book, not the earlier one
cfg = fq.Config(sim_latency_ms=50.0, sim_latency_jitter_ms=0.0)
books = {"spot": fq.Book("spot"), "fut": fq.Book("fut")}
books["spot"].replace([(100.0, 10.0)], [(101.0, 10.0)])
books["fut"].replace([(100.0, 10.0)], [(101.0, 10.0)])
m = fq.Metrics()
sim = fq.Simulator(cfg, books, m)

o = fq.Order(oid=1, venue="spot", side="buy", kind="taker", qty=1.0,
             limit=None, reason="test", arrival_mid=100.5,
             arrival_bid=100.0, arrival_ask=101.0,
             arrival_spread_bps=books["spot"].spread_bps or 0.0,
             t_decision=time.time())
sim.submit(o)
ok(sim.on_clock(time.time()) == [], "no fill before latency elapses")

# market moves against us during the latency window
books["spot"].replace([(103.0, 10.0)], [(104.0, 10.0)])
fills = sim.on_clock(o.t_effective + 0.001)
ok(len(fills) == 1, "fill after latency window")
f = fills[0]
ok(abs(f.price - 104.0) < 1e-9, "filled at the NEW ask, not the old one")

d = fq.decompose(f, cfg)
ok(d["slippage_total_bps"] > 300, "adverse move shows as large positive slippage")
ok(d["delay_plus_impact_bps"] > 0, "residual captures the delay component")
ok(abs(d["latency_ms"] - 50.0) < 1.0, "latency recorded")

# a favourable move should give negative (beneficial) slippage
books["fut"].replace([(100.0, 10.0)], [(101.0, 10.0)])
o2 = fq.Order(oid=2, venue="fut", side="buy", kind="taker", qty=1.0, limit=None,
              reason="test", arrival_mid=100.5, arrival_bid=100.0,
              arrival_ask=101.0, arrival_spread_bps=books["fut"].spread_bps,
              t_decision=time.time())
sim.submit(o2)
books["fut"].replace([(98.0, 10.0)], [(99.0, 10.0)])
f2b = sim.on_clock(o2.t_effective + 0.001)[0]
ok(fq.decompose(f2b, cfg)["slippage_total_bps"] < 0, "favourable move -> negative slippage")

# ---- maker queue model
books["spot"].replace([(100.0, 5.0)], [(101.0, 5.0)])
om = fq.Order(oid=3, venue="spot", side="buy", kind="maker", qty=1.0,
              limit=100.0, reason="quote", arrival_mid=100.5,
              arrival_bid=100.0, arrival_ask=101.0, t_decision=time.time())
sim.submit(om)
ok(abs(om.queue_ahead - 5.0) < 1e-9, "queue ahead read from displayed depth")
t = time.time() + 1.0
ok(sim.on_trade("spot", "sell", 100.0, 2.0, t) == [], "small trade doesn't reach us")
ok(sim.on_trade("spot", "sell", 100.0, 2.5, t) == [], "still queued")
got = sim.on_trade("spot", "sell", 100.0, 2.0, t)
ok(len(got) == 1, "queue exhausted -> we fill")
ok(abs(got[0].price - 100.0) < 1e-9, "maker fills at our limit")
ok(sim.on_trade("spot", "buy", 100.0, 5.0, t) == [], "wrong aggressor side ignored")

# ---- mark-outs
books["spot"].replace([(110.0, 5.0)], [(111.0, 5.0)])
sim.pending_markouts = [(got[0], time.time() - 1, "1.0s")]
sim.resolve_markouts(time.time())
mo = got[0].markouts["1.0s"]
ok(mo is not None and mo > 0, "buy + price up -> positive mark-out")

# ---- histogram bucketing
h = fq.LatencyHistogram()
for v in (5e-6, 50e-6, 1e-3, 0.5, 30.0):
    h.add("x", v)
buckets = h.flush()["x"]
ok(sum(buckets) == 5, "all samples bucketed")
ok(buckets[0] == 1, "below-range sample in first bucket")
ok(buckets[-1] == 1, "above-range sample in overflow bucket")

# ---- percentiles
xs = list(range(1, 101))
ok(abs(fq._pct(xs, 50) - 50.5) < 1e-9, "p50")
ok(abs(fq._pct(xs, 99) - 99.01) < 0.05, "p99")

# ---- strategy z-score + probe
s = fq.Strategy(cfg, books, sim, m)
for i in range(200):
    s.basis_hist.append(random.gauss(5.0, 1.0))
books["spot"].replace([(100.0, 5.0)], [(101.0, 5.0)])
books["fut"].replace([(100.0, 5.0)], [(101.0, 5.0)])
ok(s.basis_z() is not None, "z-score computes once warm")
ok(abs(s.net_delta_usd()) < 1e-9, "flat book -> zero delta")
s.pos["spot"] = 0.5
ok(s.net_delta_usd() > 0, "long spot -> positive delta")

# ---- ISO parse
ts = fq.parse_iso("2026-08-15T03:04:05.123456Z")
ok(ts is not None and 1.7e9 < ts < 2.1e9, "RFC3339 parse")

print("\ndone")
