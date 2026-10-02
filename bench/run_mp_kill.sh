#!/bin/bash
# N processes insert concurrently (multi-process mode); K of them are killed with SIGKILL at t=3 s, whichever lock, queue position or admission slot they hold.
# The survivors must keep committing; afterwards the database must pass integrity_check and hold a whole number of 100-row transactions, at least the survivors' acknowledged ones.
# usage: run_mp_kill.sh <procs> <kills> [duration]
N=$1; K=$2; DUR=${3:-10}; BIN=${BIN:-dist/mw/mw_bench}; DB=/tmp/mw_kill_$$.db
rm -f $DB $DB-*
$BIN --mode multiwriter --workload bulk --agents $N --path $DB --mp 1 --sync full --setup-only >/dev/null 2>&1
OUT=$(mktemp -d); PIDS=()
for i in $(seq 0 $((N-1))); do $BIN --mode multiwriter --workload bulk --agents 1 --agent-base $i --no-setup --path $DB --mp 1 --sync full --retry 1000 --duration $DUR --warmup 1 --seed 1 > $OUT/$i.txt 2>&1 & PIDS+=($!); done
sleep 4
KILLED=""
for k in $(python3 -c "import random; random.seed(7); print(' '.join(map(str, random.sample(range($N), $K))))"); do kill -9 ${PIDS[$k]} 2>/dev/null; KILLED="$KILLED $k"; done
for i in $(seq 0 $((N-1))); do case " $KILLED " in *" $i "*) wait ${PIDS[$i]} 2>/dev/null;; *) wait ${PIDS[$i]};; esac; done
python3 - $OUT "$KILLED" $DB <<'PY'
import glob, json, sys, sqlite3
out, killed, db = sys.argv[1], set(sys.argv[2].split()), sys.argv[3]
committed = tx = 0; alive = 0
for f in glob.glob(out + "/*.txt"):
    idx = f.rsplit("/", 1)[1][:-4]
    if idx in killed: continue
    for line in open(f):
        if line.startswith("JSON "):
            j = json.loads(line[5:]); committed += j["committed"]; tx += j["tx_per_s"]; alive += 1
c = sqlite3.connect(db)
ic = c.execute("pragma integrity_check").fetchone()[0]
n = c.execute("select count(*) from bk").fetchone()[0]
ok = ic == "ok" and n % 100 == 0 and n >= committed * 100 and alive == len(glob.glob(out + "/*.txt")) - len(killed)
print("killed %d of %d: %d survivors finished, %.0f tx/s together, rows=%d (>= %d acknowledged), integrity_check=%s -> %s" % (len(killed), len(glob.glob(out + "/*.txt")), alive, tx, n, committed * 100, ic, "OK" if ok else "FAILED"))
sys.exit(0 if ok else 1)
PY
RC=$?; rm -rf $OUT $DB $DB-*; exit $RC
