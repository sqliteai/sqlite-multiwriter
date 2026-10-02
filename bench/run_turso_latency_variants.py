#!/usr/bin/env python3
"""Re-runs the Turso open-loop latency points with different ways of waiting for the next scheduled arrival (the wait changes the harness, not the engine):
   block = the connection's task sleeps on its OS thread (same as mw_bench's nanosleep) -- the reference;  spin = busy-wait (only for few connections);
   async = tokio::time::sleep (the first run).  usage: run_turso_latency_variants.py TURSO_BENCH [jsonl]"""
import json, os, subprocess, sys, statistics
TB = sys.argv[1]; OUT = sys.argv[2] if len(sys.argv) > 2 else "test/multiwriter/bench/results/turso_compare.jsonl"
for wait, conns_list in (("block", (1, 8, 16, 32)), ("spin", (1, 8))):
    for conns in conns_list:
        runs = []
        for s in range(3):
            r = subprocess.run([TB, "latency", str(conns), "10", "1000"], capture_output=True, text=True, env=dict(os.environ, TURSO_WAIT=wait))
            js = [l for l in r.stdout.split("\n") if l.startswith("JSON ")]
            if js: runs.append(json.loads(js[0][5:]))
        ok = [r for r in runs if r["valid"]]
        rec = {"kind": "latency", "engine": "turso-" + wait, "conns": conns, "runs": len(runs), "valid_runs": len(ok)}
        for k in ("tx_per_s", "p50_us", "p99_us", "p999_us"): rec[k] = statistics.mean(r[k] for r in ok) if ok else None
        rec["max_us"] = max(r["max_us"] for r in ok) if ok else None
        open(OUT, "a").write(json.dumps(rec) + "\n")
        print(rec["engine"], conns, {k: round(v) for k, v in rec.items() if k in ("p50_us", "p99_us", "p999_us")}, "valid", len(ok), flush=True)
