#!/bin/bash
# Load for mw_serial made of mw_serial itself: N copies in parallel (each one starts 6 processes and kills them), R rounds. The output of the runs that print a violation is kept in outdir/.
# usage: test/serial_stress.sh N R outdir [VAR=value ...]       (make dist/mw_serial first)
#   MW_SERIAL_ONLY="processes, medium"   only the scenarios whose name contains it
#   MW_SERIAL_NO_KILL=1 / MW_SERIAL_NO_CRASH=1   no kill from outside / no death at a publication point
#   MW_SERIAL_PT=n   the point of the publication where the processes die: 0 mid-log 1 before-log 2 record-complete 3 installed 4 after-log 5 after-visible 6 in a collection
# On an idle machine a run of mw_serial passes (one in a few hundred fails); with 14 copies at once most of the failures of the shared mode come out (docs/design.md, 'Tests').
cd "$(dirname "$0")/.."
N=$1; R=$2; OUT=$3; shift 3
[ -n "$OUT" ] || { echo "usage: test/serial_stress.sh N R outdir [VAR=value ...]"; exit 1; }
mkdir -p "$OUT"; rm -f "$OUT"/run_*.txt "$OUT"/summary.txt "$OUT"/progress.txt
for r in $(seq 1 $R); do
  for i in $(seq 1 $N); do
    ( env "$@" ./dist/mw_serial > $OUT/run_${r}_$i.txt 2>&1; if grep -qE "^   reads that differ.*(from the serial order [1-9]|UNIQUE broken [1-9]|read-only inconsistent [1-9]|final differs [1-9]|nobody to account for them [1-9]|fit no epoch [1-9])" $OUT/run_${r}_$i.txt; then echo "VIOLATION run_${r}_$i" >> $OUT/summary.txt; else rm -f $OUT/run_${r}_$i.txt; fi ) &
  done
  wait
  echo "round $r done" >> $OUT/progress.txt
done
echo ALLDONE >> $OUT/progress.txt
