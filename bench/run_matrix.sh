#!/bin/sh
# Runs the benchmark matrix and appends one CSV row per run (see mw_bench --csv).
#   sh test/multiwriter/bench/run_matrix.sh [outfile.csv] [duration] [warmup]
# Every run validates its own data (integrity_check + workload invariant); INVALID runs abort the matrix.
OUT=${1:-bench_results.csv}
DUR=${2:-3}
WARM=${3:-1}
BIN=dist/mw/mw_bench
run () { # mode workload agents sync reps [extra...]
  mode=$1; wl=$2; n=$3; sync=$4; reps=$5; shift 5
  r=1
  while [ $r -le $reps ]; do
    $BIN --mode $mode --workload $wl --agents $n --duration $DUR --warmup $WARM --seed $r --sync $sync --csv $OUT "$@" > /tmp/mw_bench_last.txt 2>&1 || { echo "RUN FAILED: $mode $wl $n"; cat /tmp/mw_bench_last.txt; exit 1; }
    r=$((r+1))
  done
}
# independent writes: all three modes, 1/10/100/1000 agents, 3 repetitions
for n in 1 10 100 1000; do for m in stock stock-wal multiwriter; do run $m independent $n off 3; done; done
# read-only, hot rows and 80/20 mixed: stock WAL vs Multi-Writer
for n in 1 10 100 1000; do for m in stock-wal multiwriter; do run $m readonly $n off 3; done; done
for wl in hot; do for n in 1 10 100 1000; do for m in stock-wal multiwriter; do run $m $wl $n off 2; done; done; done
for n in 1 10 100 1000; do for m in stock-wal multiwriter; do run $m mixed $n off 2 --read-pct 80; done; done
# reduced matrix: stock WAL vs Multi-Writer
for wl in samepage cols samecol crdtinsert longtx insert-uuid insert-int insert-autoinc; do for n in 1 10 100; do for m in stock-wal multiwriter; do run $m $wl $n off 2; done; done; done
for n in 10 100; do for m in stock-wal multiwriter; do run $m mixed $n off 2 --read-pct 50; done; done
for m in stock-wal multiwriter; do run $m longreader 100 off 2; done
# durability: synchronous=FULL (macOS fsync does not flush the drive cache: numbers are relative, see docs)
for n in 1 10 100 1000; do for m in stock-wal multiwriter; do run $m independent $n full 2; done; done
echo "done: $OUT"
