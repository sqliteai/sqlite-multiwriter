#!/usr/bin/env python3
"""Turns the jsonl of compare_sqlite.py into markdown tables.  usage: summarize_compare.py results.jsonl [threads|procs]"""
import json, sys
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
kind = sys.argv[2] if len(sys.argv) > 2 else rows[0]["kind"]
rows = [r for r in rows if r["kind"] == kind]
variants = []
for r in rows:
    if r["variant"] not in variants: variants.append(r["variant"])
ns = sorted({r["n"] for r in rows})
def get(v, n):
    c = [r for r in rows if r["variant"] == v and r["n"] == n]
    return c[-1] if c else None
def cell(r, k, f="%.0f"):
    return "-" if not r or k not in r else f % r[k]
print("### transactions per second (100-row insert per transaction, synchronous=FULL)\n")
print("| %s | " % ("threads" if kind == "threads" else "processes") + " | ".join(variants) + " |")
print("|---:|" + "---:|" * len(variants))
for n in ns: print("| %d | " % n + " | ".join(cell(get(v, n), "tx_s") for v in variants) + " |")
print("\n### retries (the application re-runs a refused transaction) / transactions that gave up\n")
print("| n | " + " | ".join(variants) + " |"); print("|---:|" + "---:|" * len(variants))
for n in ns: print("| %d | " % n + " | ".join(("%s / %s" % (cell(get(v, n), "retries"), cell(get(v, n), "gave_up"))) for v in variants) + " |")
print("\n### mean wait before the write may start (us; first attempt -> write lock / admission)\n")
print("| n | " + " | ".join(variants) + " |"); print("|---:|" + "---:|" * len(variants))
for n in ns: print("| %d | " % n + " | ".join(cell(get(v, n), "wait_mean_us", "%.1f") for v in variants) + " |")
print("\n### latency p50 / p99 (us)\n")
print("| n | " + " | ".join(variants) + " |"); print("|---:|" + "---:|" * len(variants))
for n in ns: print("| %d | " % n + " | ".join(("%s / %s" % (cell(get(v, n), "lat_p50_us"), cell(get(v, n), "lat_p99_us"))) for v in variants) + " |")
print("\n### memory (MB)\n")
if kind == "threads":
    print("| n | " + " | ".join(variants) + " |"); print("|---:|" + "---:|" * len(variants))
    for n in ns: print("| %d | " % n + " | ".join(cell(get(v, n), "rss_mb") for v in variants) + " |")
else:
    print("| n | " + " | ".join("%s RSS/process (median) · system growth" % v for v in variants) + " |"); print("|---:|" + "---:|" * len(variants))
    for n in ns: print("| %d | " % n + " | ".join(("%s · %s" % (cell(get(v, n), "rss_mb_median"), cell(get(v, n), "sys_mem_growth_mb"))) for v in variants) + " |")
bad = [r for r in rows if r.get("valid") != 1 or r.get("error")]
if bad: print("\nINVALID or failed runs: " + ", ".join("%s n=%s" % (r["variant"], r["n"]) for r in bad))
