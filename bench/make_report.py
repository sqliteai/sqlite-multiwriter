#!/usr/bin/env python3
"""Turns the results of bench/compare_sqlite.py into the plain tables of docs/benchmarks.md.
usage: make_report.py DATE > docs/benchmarks.md        (reads bench/results/simple_<workload>_<threads|procs>_<DATE>.jsonl)"""
import json, os, sys
D = sys.argv[1]
R = "bench/results/simple_%s_%s_" + D + ".jsonl"

def load(f):
    d = {}
    if not os.path.exists(f): return d
    for l in open(f):
        r = json.loads(l); d[(r["n"], r["variant"])] = r
    return d

def per100(r): return 100.0 * r["retries"] / r["committed"] if r.get("committed") else 0.0
def fmt_tx(r): return "{:,.0f}".format(r["tx_s"]) + ("*" if r.get("gave_up") else "")
def fmt_x(a, b): return "%.1f×" % (a / b) if b else "-"
def fmt_r(v): return "0" if v == 0 else ("%.1f" % v if v < 10 else "{:,.0f}".format(v))

def table(wl, kind, label):
    d = load(R % (wl, kind))
    if not d: return ""
    ns = sorted(set(n for n, _ in d))
    out = ["| %s | SQLite tx/s | SQLite retries per 100 tx | Engine tx/s | vs SQLite | Engine retries per 100 tx | Engine **with merge** tx/s | vs SQLite | retries per 100 tx | commits merged | lost replays per merged commit |" % label,
           "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for n in ns:
        a, b, c = d.get((n, "mw")), d.get((n, "mwr")), d.get((n, "sqlite"))
        if not (a and b and c): continue
        merged = 100.0 * b.get("merged", 0) / b["committed"] if b.get("committed") else 0
        lost = b.get("merge_attempts_lost", 0) / b["merged"] if b.get("merged") else 0
        out.append("| %d | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (n, fmt_tx(c), fmt_r(per100(c)), fmt_tx(a), fmt_x(a["tx_s"], c["tx_s"]), fmt_r(per100(a)),
                   fmt_tx(b), fmt_x(b["tx_s"], c["tx_s"]), fmt_r(per100(b)), ("%.0f%%" % merged) if b.get("merged") else "0", ("%.1f" % lost) if b.get("merged") else "-"))
    return "\n".join(out)

SCEN = [
 ("bulk", "Each writer inserts its own rows (100 rows per transaction)", "No two writers touch the same row or page, apart from the growth of the file. The common case of many writers."),
 ("groups", "Each writer updates its own row; the rows of four groups of writers share a page each", "Writers never touch the same **row**, but they do share **pages**: SQLite refuses the second one, the engine can merge them."),
 ("samepage", "Each writer updates its own row; all the rows are on one single page", "The same, in the worst case for page-level conflicts: one page for everybody."),
 ("hot", "Everybody updates the same 4 rows (`a = a + 1`)", "A **real** conflict: two writers change the same row. No engine can merge that: one of them must run again."),
 ("cols", "Writers update different columns of the same rows", "Different columns of rows spread over the table. When the rows coincide it is a real conflict for the merge too (it compares whole rows). Threads only."),
]
print("# Benchmarks\n")
print("""Measured on one Mac with 18 cores, SQLite 3.53.4, `synchronous=FULL` (every commit is on disk when it returns), %s. One run of 8 seconds per number, so differences of a few percent are noise.
The numbers are in `bench/results/simple_*_%s.jsonl`; `bench/make_report.py` makes this page from them.

**How to read the tables**

- **Writers**: threads of one process (first tables) or separate processes (second tables) that all write the same database at once.
- **tx/s**: transactions committed per second, all writers together. More is better.
- **SQLite**: stock SQLite in WAL mode, one writer at a time; a writer that finds the database busy gives up and the application runs the transaction again.
- **Engine**: this project, several writers at once; a commit that conflicts with a commit made meanwhile is refused and the application runs it again.
- **Engine with merge** (`mw_rebase=1`): a commit that conflicts only because it shares a page with another commit, not a row, is not refused: the engine replays its row changes on top of the latest state and commits it.
- **Retries per 100 tx**: how many times, for every 100 committed transactions, the application had to run a transaction again after being refused (`SQLITE_BUSY`). 0 means the application never noticed a conflict.
- **Commits merged**: the share of committed transactions that the engine saved by merging, instead of refusing them.
- **Lost replays per merged commit**: how many times, on average, the engine had to replay a merge again because another commit got in first. It costs time, not retries for the application.
- "vs SQLite": the number of times more transactions per second than SQLite. A `*` after a number: some transactions gave up after 1000 retries.
""" % (D, D))
for wl, title, text in SCEN:
    print("## %s\n" % title)
    print(text + "\n")
    t = table(wl, "threads", "Threads")
    if t: print("**Threads of one process**\n\n" + t + "\n")
    if wl != "cols":
        p = table(wl, "procs", "Processes")
        if p: print("**Processes**\n\n" + p + "\n")
