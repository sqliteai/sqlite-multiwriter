#!/bin/bash
# The long hunt for failures of mw_serial under load (docs/design.md, 'Tests'): copies of the test at once, in rotation of configurations, until $OUT/STOP exists. A run with a FAIL line (other than the
# one about the number of events, which kills disabled make unavoidable) is kept in $OUT/fail_*.txt and listed in $OUT/fails.log; $OUT/progress.log has one line per round.
# usage: cp dist/mw_serial /tmp/mw_serial_hunt; mkdir -p /tmp/hunt; test/hunt.sh &     (stop: touch /tmp/hunt/STOP)
# long hunt: mw_serial with 24 copies at once, in rotation of configurations, until /tmp/hunt/STOP exists. A run with any FAIL line is kept.
OUT=/tmp/hunt; cd /tmp
cfgs=("X=1" "MW_SERIAL_KILL_MS=60" "MW_TEST_MP=1" "MW_TEST_REBASE=1 MW_SERIAL_KILL_MS=60" "MW_SERIAL_NO_KILL=1 MW_SERIAL_PT=2" "MW_SERIAL_NO_KILL=1 MW_SERIAL_PT=3" "MW_SERIAL_NO_KILL=1 MW_SERIAL_PT=6" "MW_TEST_MP=1 MW_SERIAL_KILL_MS=100")
n=0; total=0; fails=0
while [ ! -e $OUT/STOP ]; do
  cfg=${cfgs[$((n % ${#cfgs[@]}))]}; n=$((n+1))
  for i in $(seq 1 24); do
    ( env $cfg MW_SERIAL_DEBUG=1 /tmp/mw_serial_hunt > $OUT/cur_$i.txt 2>&1
      if grep "^FAIL" $OUT/cur_$i.txt | grep -qv "min_events"; then cp $OUT/cur_$i.txt "$OUT/fail_${n}_$i.txt"; echo "FAIL cfg=[$cfg] round=$n copy=$i $(grep -m1 '^FAIL' $OUT/cur_$i.txt)" >> $OUT/fails.log; fi; rm -f $OUT/cur_$i.txt ) &
  done
  wait; total=$((total+24)); echo "$(date +%H:%M:%S) rounds=$n runs=$total cfg=[$cfg]" >> $OUT/progress.log
done
echo STOPPED >> $OUT/progress.log
