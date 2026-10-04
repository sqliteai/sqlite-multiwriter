#!/usr/bin/env python3
"""Multi-Writer vs stock SQLite (WAL, synchronous=FULL), bulk inserts of 100 rows per transaction, every run on a fresh database.
usage: compare_sqlite.py threads|procs <out.jsonl> [duration] [points...]
Variants: mw (Multi-Writer with the CRDT capture on: every table tracked; the engine of one process for threads, the shared multi-process mode for processes), mw0 (the same engine without the capture), sqlite (stock WAL, busy_timeout 60 s: the library waits inside the call, the application sees no
retry), sqlite0 (stock WAL, busy_timeout 0: the application retries a refused transaction with jittered backoff, retries are counted).
Measures per run: tx/s, retries, transactions that gave up, the time a transaction waits before its write may start (first attempt -> BEGIN IMMEDIATE + first
read done: for SQLite that is the write lock, for Multi-Writer the admission), latency, memory (per-process RSS; for processes also the system-wide growth of
anonymous+wired+compressed memory while the run is going, which does not count shared file mappings N times)."""
import glob, json, os, re, subprocess, sys, tempfile, time

BIN = os.environ.get("BIN", "dist/mw_bench")
kind, out = sys.argv[1], sys.argv[2]
DUR = int(sys.argv[3]) if len(sys.argv) > 3 else 10
points = [int(x) for x in sys.argv[4:]] or ([1, 2, 4, 8, 16, 32, 64] if kind == "threads" else [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1000])
VARIANTS = {"mw": ("multiwriter", ["--tracked", "1"]), "mw0": ("multiwriter", ["--tracked", "0"]), "sqlite": ("stock-wal", []), "sqlite0": ("stock-wal", ["--busy-ms", "0"])}
ONLY = os.environ.get("VARIANTS", "mw,mw0,sqlite,sqlite0").split(",")

def vm():
    if os.path.exists("/proc/meminfo"):                      # Linux: memory in use that is not reclaimable cache
        m = {l.split(":")[0]: int(l.split()[1]) for l in open("/proc/meminfo")}
        return (m["MemTotal"] - m["MemAvailable"]) / 1024.0
    t = subprocess.run(["vm_stat"], capture_output=True, text=True).stdout
    ps = int(re.search(r"page size of (\d+)", t).group(1)); g = lambda k: int(re.search(k + r":\s+(\d+)", t).group(1))
    return (g("Anonymous pages") + g("Pages wired down") + g("Pages occupied by compressor")) * ps / 1048576.0

def clean(db):
    for f in glob.glob(db + "*"): os.unlink(f)

def common(mode, extra, db, n):
    mp = (1 if kind == "procs" or os.environ.get("THREADS_MP") else 0) if mode == "multiwriter" else 0       # (threads: the engine of one process, mw_mp=0; THREADS_MP=1: the shared mode with threads)
    return [BIN, "--mode", mode, "--workload", os.environ.get("WORKLOAD", "bulk"), "--path", db, "--mp", str(mp), "--sync", "full", "--retry", "1000", "--begin-wait"] + (["--think-us", os.environ["THINK_US"]] if os.environ.get("THINK_US") else []) + extra

def run(variant, n):
    mode, extra = VARIANTS[variant]
    db = "/tmp/cmp_%d.db" % os.getpid(); clean(db)
    base = common(mode, extra, db, n)
    subprocess.run(base + ["--agents", str(n), "--setup-only"], capture_output=True, check=True)
    res = {"variant": variant, "kind": kind, "n": n, "duration": DUR}
    if kind == "threads":
        r = subprocess.run(base + ["--agents", str(n), "--threads", str(n), "--no-setup", "--duration", str(DUR), "--warmup", "2", "--seed", "1"], capture_output=True, text=True)
        js = [json.loads(l[5:]) for l in r.stdout.splitlines() if l.startswith("JSON ")]
        if not js: res["error"] = (r.stdout + r.stderr)[-300:]; clean(db); return res
        j = js[0]; res.update(tx_s=j["tx_per_s"], retries=j["busy"], gave_up=j["errors"], committed=j["committed"], wait_mean_us=j["wait_mean_us"], wait_p50_us=j["wait_p50_us"],
                              wait_p99_us=j["wait_p99_us"], wait_max_us=j["wait_max_us"], lat_p50_us=j["p50_us"], lat_p99_us=j["p99_us"], rss_mb=j["rss_mb"], valid=j["valid"], cpu_s=j["cpu_s"])
    else:
        bar = tempfile.mkdtemp(); od = tempfile.mkdtemp(); m0 = vm()
        procs = [subprocess.Popen(base + ["--agents", "1", "--agent-base", str(i), "--no-setup", "--duration", str(DUR), "--warmup", "2", "--seed", "1", "--barrier", bar],
                                  stdout=open("%s/%d.txt" % (od, i), "w"), stderr=subprocess.STDOUT) for i in range(n)]
        for _ in range(3000):
            if len(glob.glob(bar + "/ready.*")) >= n: break
            time.sleep(0.1)
        open(bar + "/go", "w").close()
        time.sleep(2 + DUR / 2); m1 = vm()
        for p in procs: p.wait()
        js = []
        for f in glob.glob(od + "/*.txt"):
            for l in open(f):
                if l.startswith("JSON "): js.append(json.loads(l[5:]))
        if len(js) < n: res["error"] = "only %d of %d processes reported" % (len(js), n)
        if js:
            tot = sum(j["committed"] for j in js); wn = [j["tx_per_s"] * j["duration"] for j in js]
            wm = sum(j["wait_mean_us"] * w for j, w in zip(js, wn)) / max(1, sum(wn))
            rss = sorted(j["rss_mb"] for j in js); p50 = sorted(j["wait_p50_us"] for j in js)
            res.update(tx_s=sum(j["tx_per_s"] for j in js), retries=sum(j["busy"] for j in js), gave_up=sum(j["errors"] for j in js), committed=tot, reporting=len(js),
                       wait_mean_us=wm, wait_p50_us=p50[len(p50) // 2], wait_p99_us=max(j["wait_p99_us"] for j in js), wait_max_us=max(j["wait_max_us"] for j in js),
                       lat_p50_us=sorted(j["p50_us"] for j in js)[len(js) // 2], lat_p99_us=max(j["p99_us"] for j in js), rss_mb_median=rss[len(rss) // 2], rss_mb_sum=sum(rss),
                       sys_mem_growth_mb=m1 - m0, cpu_s=sum(j["cpu_s"] for j in js))
        v = subprocess.run(base + ["--agents", "1", "--verify-sum", str(res.get("committed", 0))], capture_output=True, text=True).stdout
        res["valid"] = 1 if "-> VALID" in v else 0
    clean(db)
    return res

with open(out, "a") as fo:
    for n in points:
        for variant in ONLY:
            r = run(variant, n); fo.write(json.dumps(r) + "\n"); fo.flush()
            print(json.dumps({k: (round(v, 1) if isinstance(v, float) else v) for k, v in r.items()}), flush=True)
            time.sleep(4)
