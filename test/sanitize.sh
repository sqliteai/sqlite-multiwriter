#!/bin/bash
# Builds the Multi-Writer tests and the benchmark with a sanitizer and runs them.
# usage: test/multiwriter/sanitize.sh <asan|ubsan|tsan> [test ...]      (run from the repository root; macOS arm64 flags as in the Makefile)
# asan: memory errors; ubsan: undefined behaviour, misaligned access, signed overflow; tsan: data races (races inside the sqlite-sync core, the benchmark harness and
# the test programs themselves are reported too: read the first frames, only the multiwriter_*.c ones concern the engine).
set -e
KIND=${1:-asan}; shift || true
case $KIND in
  asan)  SAN="-fsanitize=address";;
  ubsan) if gcc --version 2>&1 | grep -qi clang; then SAN="-fsanitize=undefined,alignment,integer -fno-sanitize=unsigned-integer-overflow,unsigned-shift-base,implicit-conversion -fno-sanitize-recover=undefined"
         else SAN="-fsanitize=undefined -fno-sanitize-recover=undefined"; fi;;     # (GNU gcc, Linux: no "integer" group)
  tsan)  SAN="-fsanitize=thread";;
  *) echo "usage: $0 asan|ubsan|tsan [tests]"; exit 2;;
esac
OUT=build/$KIND; BIN=dist/$KIND; mkdir -p $OUT $BIN
ARCH=$(uname -m); [ "$ARCH" = arm64 ] && ARCHF="-arch arm64" || ARCHF=""
INC="-Isrc -Isrc/sqlite -Isrc/postgresql -Isrc/network -Isqlite -Icurl/include -Imodules/fractional-indexing -Isrc/multiwriter"
DEFS="-DCLOUDSYNC_MULTIWRITER -DSQLITE_ENABLE_PREUPDATE_HOOK -DSQLITE_DISABLE_PAGECACHE_OVERFLOW_STATS"
CF="$INC $DEFS -O1 -g $SAN -fno-omit-frame-pointer $ARCHF -DSQLITE_CORE -DCLOUDSYNC_UNITTEST -DCLOUDSYNC_OMIT_NETWORK -DCLOUDSYNC_OMIT_PRINT_RESULT"
for f in src/multiwriter/multiwriter_*.c src/cloudsync.c src/dbutils.c src/lz4.c src/pk.c src/utils.c src/network.c src/block.c \
         src/sqlite/cloudsync_changes_sqlite.c src/sqlite/cloudsync_sqlite.c src/sqlite/database_sqlite.c src/sqlite/sql_sqlite.c modules/fractional-indexing/fractional_indexing.c; do
  [ -f $f ] && gcc -w $CF -c $f -o $OUT/$(basename $f .c).o
done
gcc -w -O1 -g $SAN $ARCHF $INC $DEFS -DSQLITE_EXTRA_INIT=mw_extra_init -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_RTREE -DSQLITE_DQS=0 -DSQLITE_CORE -c sqlite/sqlite3.c -o $OUT/sqlite3.o
TESTS=${@:-"mw_lanes mw_reloc mw_stagedlog mw_compact mw_durability mw_structural mw_rebase mw_multiproc mw_shidx mw_shared bench/mw_bench"}
LIBS=$(ls $OUT/*.o | grep -v "/mw_")
EXTRA=""; [ "$(uname)" = Darwin ] && EXTRA="-framework Security"
for t in $TESTS; do
  src=test/multiwriter/$t.c; b=$(basename $t)
  gcc -w $CF -c $src -o $OUT/$b.o && gcc $OUT/$b.o $LIBS -o $BIN/$b $EXTRA $SAN -lm
done
for t in $TESTS; do b=$(basename $t); [ "$b" = mw_bench ] && continue
  ASAN_OPTIONS=detect_leaks=0 TSAN_OPTIONS=halt_on_error=0:detect_deadlocks=0 UBSAN_OPTIONS=print_stacktrace=1 $BIN/$b > $OUT/$b.log 2>&1 && st=ok || st="exit $?"
  echo "$b: $st, reports: $(grep -c -E 'ERROR: AddressSanitizer|WARNING: ThreadSanitizer|runtime error' $OUT/$b.log)"
done
if [ -x $BIN/mw_bench ]; then for w in bulk independent hot samepage mixed; do
  TSAN_OPTIONS=halt_on_error=0:detect_deadlocks=0 UBSAN_OPTIONS=print_stacktrace=1 $BIN/mw_bench --mode multiwriter --workload $w --agents 8 --duration 3 --warmup 1 --retry 1000 --sync full > $OUT/bench_$w.log 2>&1 && st=ok || st="exit $?"
  echo "mw_bench $w: $st, reports: $(grep -c -E 'ERROR: AddressSanitizer|WARNING: ThreadSanitizer|runtime error' $OUT/bench_$w.log)"; done; fi
