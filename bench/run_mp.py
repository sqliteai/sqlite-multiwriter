#!/usr/bin/env python3
"""Multi-process benchmark: P processes x A agents on ONE database, stock WAL vs Multi-Writer (mw_mp=1).
   python3 test/multiwriter/bench/run_mp.py [duration] [sync]   (run from the repo root after `make mw-bench`)
Every run is verified once at the end (sum of committed updates == table content, integrity_check)."""
import json, subprocess, sys, os, time
BIN = "dist/mw/mw_bench"
dur = sys.argv[1] if len(sys.argv) > 1 else "3"
sync = sys.argv[2] if len(sys.argv) > 2 else "off"

def run(mode, procs, agents_per_proc, workload="independent", reps=2):
    out = []
    for rep in range(reps):
        path = "/tmp/mw_mpbench_%d.db" % os.getpid()
        total_agents = procs * agents_per_proc
        mp = ["--mp", "1"] if mode == "multiwriter" else []
        r = subprocess.run([BIN, "--mode", mode, "--workload", workload, "--agents", str(total_agents), "--rows", str(total_agents + 1),
                            "--path", path, "--setup-only", "--sync", sync] + mp, capture_output=True, text=True)
        if r.returncode: print(r.stdout, r.stderr); sys.exit(1)
        ps = []
        for p in range(procs):
            ps.append(subprocess.Popen([BIN, "--mode", mode, "--workload", workload, "--agents", str(agents_per_proc), "--rows", str(total_agents + 1),
                                        "--path", path, "--no-setup", "--agent-base", str(p * agents_per_proc), "--duration", dur, "--warmup", "1",
                                        "--seed", str(rep + 1), "--sync", sync] + mp, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True))
        committed, txps, p99, busy = 0, 0.0, 0.0, 0
        for pr in ps:
            so, se = pr.communicate()
            js = [l for l in so.split("\n") if l.startswith("JSON ")]
            if pr.returncode or not js: print("PROCESS FAILED:", so[-600:], se[-600:]); sys.exit(1)
            d = json.loads(js[0][5:])
            committed += d["committed"]; txps += d["tx_per_s"]; p99 = max(p99, d["p99_us"]); busy += d["busy"]
        v = subprocess.run([BIN, "--mode", mode, "--workload", workload, "--agents", "1", "--path", path, "--verify-sum", str(committed), "--sync", sync] + mp, capture_output=True, text=True)
        valid = "VALID" in v.stdout and "INVALID" not in v.stdout
        if not valid: print("VERIFY FAILED", v.stdout, v.stderr); sys.exit(1)
        out.append((txps, p99, busy))
    out.sort()
    return out[len(out) // 2]

print("| workload | processes x agents | stock WAL tx/s | Multi-Writer (mw_mp=1) tx/s | MW / WAL | WAL p99 us | MW p99 us | busy WAL / MW |")
print("|---|---:|---:|---:|---:|---:|---:|---:|")
for wl in ["independent"]:   # (multi-process verification checks sum(t.v); the tracked hot workload is single-process only)
    for procs, apps in [(1, 4), (2, 4), (4, 4), (8, 4), (8, 16)]:
        w = run("stock-wal", procs, apps, wl)
        m = run("multiwriter", procs, apps, wl)
        print("| %s | %d x %d | %.0f | %.0f | %.2fx | %.0f | %.0f | %d / %d |" % (wl, procs, apps, w[0], m[0], m[0] / w[0] if w[0] else 0, w[1], m[1], w[2], m[2]))
        sys.stdout.flush()
