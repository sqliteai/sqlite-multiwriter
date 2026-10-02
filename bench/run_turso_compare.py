#!/usr/bin/env python3
"""Turso 0.8 vs stock SQLite vs Multi-Writer, on the workload shapes of the Turso 0.8 post (written independently: this is NOT Turso's benchmark suite).
   throughput: N connections, each transaction = one INSERT of 100 rows on disjoint keys, closed loop, synchronous=FULL
   latency   : the same transaction, Poisson arrivals at 1000 tps in total over N connections; latency = scheduled arrival -> commit
   python3 run_turso_compare.py TURSO_BENCH_BINARY [outfile.jsonl]      (run from the repo root after `make mw-bench`)
   Turso: native Rust crate (BEGIN CONCURRENT, journal_mode=mvcc); SQLite / Multi-Writer: mw_bench (C), WAL, busy_timeout / retry until commit."""
import json, os, subprocess, sys, statistics
TB = sys.argv[1]; OUT = sys.argv[2] if len(sys.argv) > 2 else "test/multiwriter/bench/results/turso_compare.jsonl"
MW = "dist/mw/mw_bench"; REPS = 3
KINDS = os.environ.get("KINDS", "throughput,latency").split(",")     # e.g. KINDS=latency
ENGINES = os.environ.get("ENGINES", "stock,stock-wal,multiwriter,turso").split(",")   # e.g. ENGINES=stock-wal,multiwriter to re-measure only the SQLite family
open(OUT, "w").close()

def run_c(mode, conns, poisson, dur, seed):
    a = [MW, "--mode", mode, "--workload", "bulk", "--agents", str(conns), "--duration", str(dur), "--warmup", "1", "--seed", str(seed), "--sync", "full"]
    if mode == "multiwriter": a += ["--busy-ms", "0", "--retry", "1000000"]
    else: a += ["--busy-ms", "60000"]
    if poisson: a += ["--poisson-tps", "1000"]
    r = subprocess.run(a, capture_output=True, text=True)
    js = [l for l in r.stdout.split("\n") if l.startswith("JSON ")]
    if r.returncode or not js: return {"valid": 0, "err": (r.stdout + r.stderr)[-300:]}
    d = json.loads(js[0][5:]); d["engine"] = mode; return d

def run_t(conns, poisson, dur, seed):
    a = [TB, "latency" if poisson else "throughput", str(conns), str(dur), "1000"]
    r = subprocess.run(a, capture_output=True, text=True)
    js = [l for l in r.stdout.split("\n") if l.startswith("JSON ")]
    if r.returncode or not js: return {"valid": 0, "err": (r.stdout + r.stderr)[-300:]}
    d = json.loads(js[0][5:]); d["valid"] = 1 if d["valid"] else 0; d["engine"] = "turso"; return d

def emit(kind, engine, conns, runs):
    ok = [r for r in runs if r.get("valid")]
    rec = {"kind": kind, "engine": engine, "conns": conns, "runs": len(runs), "valid_runs": len(ok)}
    if ok:
        for k in ("tx_per_s", "p50_us", "p99_us", "p999_us", "max_us"):
            v = [r[k] for r in ok]
            rec[k] = statistics.mean(v) if k != "max_us" else max(v)
            if k == "tx_per_s": rec["sd"] = statistics.pstdev(v) if len(v) > 1 else 0.0
        rec["retries"] = sum(r.get("busy", r.get("retries", 0)) for r in ok)
    with open(OUT, "a") as f: f.write(json.dumps(rec) + "\n")
    print(kind, engine, conns, {k: (round(v) if isinstance(v, float) else v) for k, v in rec.items() if k in ("tx_per_s", "p99_us", "p999_us", "valid_runs")}, flush=True)

for conns in (1, 2, 4, 8, 16, 32, 64) if "throughput" in KINDS else ():
    for engine in [e for e in ("stock", "stock-wal", "multiwriter", "turso") if e in ENGINES]:
        runs = [run_t(conns, False, 3, s) if engine == "turso" else run_c(engine, conns, False, 3, s) for s in range(1, REPS + 1)]
        emit("throughput", engine, conns, runs)
for conns in (1, 8, 16, 32) if "latency" in KINDS else ():
    for engine in [e for e in ("stock-wal", "multiwriter", "turso") if e in ENGINES]:
        runs = [run_t(conns, True, 10, s) if engine == "turso" else run_c(engine, conns, True, 10, s) for s in range(1, REPS + 1)]
        emit("latency", engine, conns, runs)
