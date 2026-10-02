#!/usr/bin/env python3
"""Summarise mw_bench CSV output: median over repetitions, scalability tables (markdown)."""
import csv, sys, statistics
from collections import defaultdict
rows = list(csv.DictReader(open(sys.argv[1])))
bad = [r for r in rows if r['valid'] != '1']
if bad:
    print("WARNING: %d INVALID runs were discarded" % len(bad))
rows = [r for r in rows if r['valid'] == '1']
groups = defaultdict(list)
for r in rows:
    groups[(r['workload'], r['sync'], int(r['agents']), r['mode'])].append(r)
def med(key, field):
    g = groups.get(key)
    return statistics.median(float(x[field]) for x in g) if g else None
def fmt(v, p=0):
    return '-' if v is None else ('%.*f' % (p, v))
workloads = sorted({(k[0], k[1]) for k in groups})
for (wl, sync) in workloads:
    agents = sorted({k[2] for k in groups if k[0] == wl and k[1] == sync})
    print("\n### %s (synchronous=%s)\n" % (wl, sync))
    print("| agents | stock tx/s | stock WAL tx/s | MultiWriter tx/s | MW vs WAL | WAL p99 us | MW p99 us | MW rebases/s | MW page conflicts/s | reps |")
    print("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for n in agents:
        s, w, m = (wl, sync, n, 'stock'), (wl, sync, n, 'stock-wal'), (wl, sync, n, 'multiwriter')
        tw, tm = med(w, 'tx_per_s'), med(m, 'tx_per_s')
        ratio = (tm / tw) if tw and tm else None
        dur = med(m, 'tx_per_s') and 1
        reb = None
        if groups.get(m):
            secs = 3.0
            reb = statistics.median(float(x['mw_rebases']) for x in groups[m]) / secs
            pc = statistics.median(float(x['mw_page_conflicts']) for x in groups[m]) / secs
        else:
            pc = None
        reps = len(groups.get(m, [])) or len(groups.get(w, []))
        print("| %d | %s | %s | %s | %s | %s | %s | %s | %s | %d |" % (n, fmt(med(s, 'tx_per_s')), fmt(tw), fmt(tm), ('%.2fx' % ratio) if ratio else '-',
              fmt(med(w, 'p99_us'), 1), fmt(med(m, 'p99_us'), 1), fmt(reb), fmt(pc), reps))
