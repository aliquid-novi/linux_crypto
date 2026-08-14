"""Regression tests for the v2 -> v3 fixes specifically."""
import sys, time, json, math, random
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import fillquality_v3 as fq

random.seed(11)

def ok(c, m):
    if not c:
        raise AssertionError(m)
    print("PASS", m)

# ---- 1. fixed-depth truncation prevents the crossed-book failure --------
b = fq.Book("spot", max_depth=3)
b.replace([(100.0, 1), (99.0, 1), (98.0, 1)], [(101.0, 1), (102.0, 1), (103.0, 1)])
# simulate the upstream failure: new best levels arrive, old ones never deleted
b.apply_level("ask", 100.5, 1)   # new best ask pushes 103 out of the window
b.apply_level("ask", 100.2, 1)   # ...and 102
b.truncate()
ok(len(b.asks) == 3, "asks truncated to subscription depth")
ok(103.0 not in b.asks and 102.0 not in b.asks, "stale deep asks dropped")
ok(not b.is_crossed(), "book stays uncrossed after truncation")

# without max_depth (futures) truncate is a no-op
f = fq.Book("fut")
f.replace([(100.0, 1)], [(101.0, 1), (102.0, 1), (103.0, 1), (104.0, 1)])
f.truncate()
ok(len(f.asks) == 4, "truncate is a no-op without max_depth")

# a genuinely crossed book is detected
c = fq.Book("x", max_depth=25)
c.replace([(105.0, 1)], [(101.0, 1)])
ok(c.is_crossed(), "crossed book detected")

# ---- 2. crossed book blocks trading -------------------------------------
cfg = fq.Config(sim_latency_ms=10.0, sim_latency_jitter_ms=0.0,
                max_net_delta_usd=50.0, max_gross_usd=100_000.0,
                prometheus_enabled=False, probe_enabled=False)
books = {"spot": fq.Book("spot", max_depth=25), "fut": fq.Book("fut")}
books["spot"].replace([(105.0, 5.0)], [(101.0, 5.0)])   # crossed
books["fut"].replace([(100.0, 5.0)], [(101.0, 5.0)])
for bk in books.values():
    bk.touch(time.monotonic(), None)
m = fq.Metrics()
sim = fq.Simulator(cfg, books, m)
s = fq.Strategy(cfg, books, sim, m)
s.pos["spot"] = 1.0   # ~$100 delta against a $50 band -> must flatten
s.evaluate(time.time(), time.time(), 0.0)
ok(len(sim.pending) == 0 and len(sim.resting) == 0,
   "no orders created while any book is crossed")

books["spot"].replace([(100.0, 5.0)], [(101.0, 5.0)])
books["spot"].touch(time.monotonic(), None)
s.evaluate(time.time(), time.time(), 0.0)
ok(any(o.reason == "flatten" for o in sim.pending),
   "trading resumes once the book is clean")

# ---- 3. flatten sized to the excess, not one clip ------------------------
fl = [o for o in sim.pending if o.reason == "flatten"][0]
fut_mid = books["fut"].mid
excess_usd = abs(s.net_delta_usd()) - 0.5 * cfg.max_net_delta_usd
ok(abs(fl.qty * fut_mid - excess_usd) / excess_usd < 0.02,
   "flatten qty targets half the delta band, not clip_usd")

# ---- 4. signal trades blocked while risk order pending -------------------
books4 = {"spot": fq.Book("spot", max_depth=25), "fut": fq.Book("fut")}
books4["spot"].replace([(100.0, 5.0)], [(101.0, 5.0)])
books4["fut"].replace([(110.0, 5.0)], [(111.0, 5.0)])   # perp very rich
for bk in books4.values():
    bk.touch(time.monotonic(), None)
m4 = fq.Metrics()
sim4 = fq.Simulator(cfg, books4, m4)
s4 = fq.Strategy(cfg, books4, sim4, m4)
for _ in range(s4._basis_min_samples + 10):
    s4.basis_hist.append(random.gauss(0.0, 0.5))
s4.evaluate(time.time(), time.time(), 0.0)
ok(any(o.reason == "signal" for o in sim4.pending),
   "extreme basis fires a signal trade when no risk order is pending")

sim4.pending = [o for o in sim4.pending if o.reason != "signal"]
fake = fq.Order(oid=999, venue="fut", side="sell", kind="taker", qty=0.001,
                limit=None, reason="flatten", t_decision=time.time())
sim4.pending.append(fake)
s4.last_taker = 0.0
s4.evaluate(time.time(), time.time(), 0.0)
ok(not any(o.reason == "signal" for o in sim4.pending),
   "signal entry suppressed while a flatten is in flight")

# ---- 5. basis sampling is on a time grid ---------------------------------
books2 = {"spot": fq.Book("spot", max_depth=25), "fut": fq.Book("fut")}
books2["spot"].replace([(100.0, 5.0)], [(101.0, 5.0)])
books2["fut"].replace([(100.0, 5.0)], [(101.0, 5.0)])
for bk in books2.values():
    bk.touch(time.monotonic(), None)
m2 = fq.Metrics()
sim2 = fq.Simulator(cfg, books2, m2)
s2 = fq.Strategy(cfg, books2, sim2, m2)
for _ in range(50):                      # 50 rapid-fire messages
    s2.evaluate(time.time(), time.time(), 0.0)
ok(len(s2.basis_hist) == 1,
   "burst of messages adds one basis sample, not fifty")
ok(s2.basis_hist.maxlen == int(cfg.basis_window_secs / cfg.basis_sample_secs),
   "window length derived from seconds, not message count")
ok(s2._basis_min_samples == int(cfg.basis_min_secs / cfg.basis_sample_secs),
   "warm-up threshold derived from seconds")

# ---- 6. mark-out measures mid drift (maker tautology removed) ------------
books3 = {"spot": fq.Book("spot", max_depth=25), "fut": fq.Book("fut")}
books3["spot"].replace([(100.0, 5.0)], [(100.03, 5.0)])
m3 = fq.Metrics()
sim3 = fq.Simulator(cfg, books3, m3)
om = fq.Order(oid=1, venue="spot", side="buy", kind="maker", qty=1.0,
              limit=100.0, reason="quote", arrival_mid=100.015,
              arrival_bid=100.0, arrival_ask=100.03, t_decision=time.time())
sim3.submit(om)
t = om.t_effective + 0.01
sim3.on_trade("spot", "sell", 100.0, 10.0, t)   # queue swept, we fill at 100
fill = sim3.fills[-1]
# mid does NOT move afterwards:
sim3.pending_markouts = [(fill, time.time() - 1, "1.0s")]
_, _, v_mid, v_fill = sim3.resolve_markouts(time.time())[0]
ok(abs(v_mid) < 0.2, "unchanged mid -> ~zero adverse-selection markout")
ok(v_fill > 1.0, "vs-fill markout still shows the earned offset separately")

# ---- 7. strict JSON ------------------------------------------------------
clean = fq._finite({"a": float("inf"), "b": [1.0, float("nan")],
                    "c": {"d": -float("inf")}, "e": 2.5})
ok(clean == {"a": None, "b": [1.0, None], "c": {"d": None}, "e": 2.5},
   "_finite nulls every non-finite float")
ok(json.loads(json.dumps(clean)) == clean, "sanitised row round-trips strictly")

# ---- 8. feed-lag floor ---------------------------------------------------
bb = fq.Book("t")
e1 = bb.note_feed_lag(0.560)
e2 = bb.note_feed_lag(0.548)
e3 = bb.note_feed_lag(0.600)
ok(e2 == 0.0, "new minimum -> zero excess")
ok(abs(e3 - 0.052) < 1e-9, "excess measured over the rolling floor")

# ---- 9. health counters: window + total ----------------------------------
mm = fq.Metrics()
mm.reconnects = 3
s1 = mm.snapshot()
mm.reconnects = 5
s2_ = mm.snapshot()
ok(s1["reconnects_window"] == 3 and s1["reconnects_total"] == 3,
   "first window reports both delta and total")
ok(s2_["reconnects_window"] == 2 and s2_["reconnects_total"] == 5,
   "second window reports the delta since last snapshot")

print("\nall v3 regression tests passed")
