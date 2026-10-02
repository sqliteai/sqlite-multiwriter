#!/usr/bin/env python3
"""Turso 0.8 benchmark shapes, run the same way for Turso (pyturso, BEGIN CONCURRENT, journal_mode=mvcc) and for stock SQLite through Python's sqlite3
(a calibration of the Python driver: the SQLite / Multi-Writer numbers in the comparison come from mw_bench, in C).
  throughput : N connections, each transaction inserts 100 rows on disjoint keys, closed loop (as fast as it can), synchronous=FULL
  latency    : Poisson arrivals at a total rate (default 1000 tps) over N connections; latency = scheduled arrival -> commit
usage: bench_turso.py ENGINE(turso|sqlite3) MODE(throughput|latency) CONNS [duration] [tps]      (prints one JSON line)"""
import sys, os, time, json, random, threading, tempfile, shutil, math

engine, mode, conns = sys.argv[1], sys.argv[2], int(sys.argv[3])
duration = float(sys.argv[4]) if len(sys.argv) > 4 else 3.0
tps = float(sys.argv[5]) if len(sys.argv) > 5 else 1000.0
WARM = 1.0
PAYLOAD = "0123456789012345678901234567890123456789012345678901234567890123"
d = tempfile.mkdtemp(prefix="mw_turso_")
path = os.path.join(d, "bench.db")

def connect():
    if engine == "turso":
        import turso
        return turso.connect(path)
    import sqlite3
    return sqlite3.connect(path, timeout=60, isolation_level=None, check_same_thread=False)

def is_conflict(e):
    m = str(e).lower()
    return "conflict" in m or "busy" in m or "locked" in m or "snapshot" in m

setup = connect()
cur = setup.cursor()
if engine == "turso":
    cur.execute("PRAGMA journal_mode=mvcc"); cur.fetchall()        # (the pragma only takes effect once its result is consumed)
else:
    cur.execute("PRAGMA journal_mode=WAL"); cur.fetchall()
cur.execute("PRAGMA synchronous=FULL")
cur.execute("CREATE TABLE bk(id INTEGER PRIMARY KEY, v TEXT)")
setup.commit()
BEGIN = "BEGIN CONCURRENT" if engine == "turso" else "BEGIN IMMEDIATE"

stop = threading.Event()
measuring = threading.Event()
results = [None] * conns
start_gate = threading.Barrier(conns + 1)

def one_txn(c, cu, base):
    rows = [(base + i, PAYLOAD) for i in range(100)]
    retries = 0
    while True:
        try:
            cu.execute(BEGIN)
            cu.executemany("INSERT INTO bk(id,v) VALUES(?,?)", rows)
            c.commit() if engine == "turso" else cu.execute("COMMIT")
            return retries
        except Exception as e:
            if not is_conflict(e): raise
            retries += 1
            try:
                c.rollback() if engine == "turso" else cu.execute("ROLLBACK")
            except Exception:
                pass
            time.sleep(0.0002)

def worker(k):
    c = connect(); cu = c.cursor()
    if engine == "turso": cu.execute("PRAGMA synchronous=FULL")
    else: cu.execute("PRAGMA synchronous=FULL"); cu.execute("PRAGMA busy_timeout=60000")
    rng = random.Random(k + 1)
    lat, commits, retries, n = [], 0, 0, 0
    start_gate.wait()
    next_t = time.perf_counter() + rng.expovariate(tps / conns) if mode == "latency" else 0
    while not stop.is_set():
        if mode == "latency":
            sched = next_t
            now = time.perf_counter()
            if sched > now: time.sleep(sched - now)
            next_t = sched + rng.expovariate(tps / conns)
        else:
            sched = time.perf_counter()
        r = one_txn(c, cu, (k << 32) + n * 100); n += 1
        end = time.perf_counter()
        if measuring.is_set():
            commits += 1; retries += r; lat.append(end - sched)
    results[k] = (commits, retries, lat)
    c.close()

ths = [threading.Thread(target=worker, args=(k,)) for k in range(conns)]
for t in ths: t.start()
start_gate.wait()
time.sleep(WARM)
measuring.set(); t0 = time.perf_counter()
time.sleep(duration)
measuring.clear(); secs = time.perf_counter() - t0
stop.set()
for t in ths: t.join()
commits = sum(r[0] for r in results); retries = sum(r[1] for r in results)
lat = sorted(x for r in results for x in r[2])
def pct(p): return lat[min(len(lat) - 1, int(p * len(lat)))] * 1e6 if lat else 0.0
# validation: every committed transaction is there, exactly once (rows committed during warm-up and the tail are counted by the table, not by us)
v = connect(); vc = v.cursor()
vc.execute("SELECT count(*) FROM bk"); total_rows = vc.fetchone()[0]
vc.execute("PRAGMA integrity_check"); ic = vc.fetchall()
ic_ok = len(ic) == 1 and str(ic[0][0]).lower() == "ok"
valid = ic_ok and total_rows % 100 == 0 and total_rows >= commits * 100
print("JSON " + json.dumps({"engine": engine, "mode": mode, "conns": conns, "tps_target": tps if mode == "latency" else None,
      "tx_per_s": commits / secs, "commits": commits, "retries": retries, "p50_us": pct(0.5), "p99_us": pct(0.99), "p999_us": pct(0.999), "max_us": lat[-1] * 1e6 if lat else 0,
      "rows_in_table": total_rows, "integrity_ok": ic_ok, "valid": valid}))
shutil.rmtree(d, ignore_errors=True)
