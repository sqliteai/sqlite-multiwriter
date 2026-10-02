#!/bin/bash
# N separate processes, one connection each, same database file; prints the summed tx/s.
# usage: run_mp_procs.sh <multiwriter|stock-wal> <procs> [sync] [duration]
MODE=$1; N=$2; SYNC=${3:-full}; DUR=${4:-8}
BIN=${BIN:-dist/mw/mw_bench}
DB=/tmp/mw_procs_$$.db
MP=0; [ "$MODE" = multiwriter ] && MP=1
rm -f $DB $DB-*
$BIN --mode $MODE --workload ${WL:-bulk} --agents $N --path $DB --mp $MP --sync $SYNC --setup-only >/dev/null 2>&1
OUT=$(mktemp -d)
BAR=$(mktemp -d)
for i in $(seq 0 $((N-1))); do
  $BIN --mode $MODE --workload ${WL:-bulk} --agents 1 --agent-base $i --no-setup --path $DB --mp $MP --sync $SYNC --retry 1000 $EXTRA --duration $DUR --warmup 1 --seed 1 --barrier $BAR > $OUT/$i.txt 2>&1 &
done
# every process is connected: start them together
for t in $(seq 1 600); do [ $(ls $BAR | grep -c ready) -ge $N ] && break; sleep 0.1; done
touch $BAR/go
wait
rm -rf $BAR
[ -n "$KEEP" ] && cp $OUT/0.txt $KEEP
python3 - $OUT $N <<'PY'
import glob, json, sys
tx = busy = 0; p50 = []; p99 = []; valid = 1
for f in glob.glob(sys.argv[1] + "/*.txt"):
    for line in open(f):
        if line.startswith("JSON "):
            j = json.loads(line[5:]); tx += j["tx_per_s"]; busy += j["busy"]; p50.append(j["p50_us"]); p99.append(j["p99_us"]); valid &= j.get("valid", 1)
p50.sort()
tx_committed = sum(json.loads(l[5:])['committed'] for f in glob.glob(sys.argv[1] + '/*.txt') for l in open(f) if l.startswith('JSON '))
open(sys.argv[1] + '/committed', 'w').write(str(tx_committed))
print("%s processes (%d reported): %d tx/s total, refused+retried=%d, p50=%.0f us (median of processes), p99=%.0f us (worst process)%s" % (sys.argv[2], len(p50), tx, busy, p50[len(p50)//2] if p50 else 0, max(p99) if p99 else 0, "" if valid else "  INVALID"))
PY
VERTXT=$($BIN --mode $MODE --workload ${WL:-bulk} --agents 1 --path $DB --mp $MP --sync $SYNC --verify-sum $(cat $OUT/committed) 2>&1 | grep 'verify:' | tail -1)
VER=$(echo "$VERTXT" | grep -o 'VALID\|INVALID' | tail -1)
[ "$VER" = INVALID ] && echo "   $VERTXT"
echo "   verify: ${VER:-none}"
rm -rf $OUT $DB $DB-*
