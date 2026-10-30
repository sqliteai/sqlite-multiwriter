#!/bin/bash
# Builds tests with a sanitizer and runs them.
#   test/sanitize.sh asan|ubsan|tsan [test ...]        (from the repository root)
# asan: memory errors; ubsan: undefined behaviour; tsan: data races (SQLite itself and the test programs are reported too: read the first frames,
# only the multiwriter_*.c ones concern the engine). Default tests: the transaction ones. A test that runs longer than MW_SAN_TIMEOUT seconds (default 600) is killed and fails.
set -euo pipefail
CC=${CC:-cc}
KIND=${1:-asan}; shift || true
case $KIND in
  asan)  SAN="-fsanitize=address";;
  ubsan) SAN="-fsanitize=undefined,alignment,integer -fno-sanitize=unsigned-integer-overflow,unsigned-shift-base,implicit-conversion -fno-sanitize-recover=undefined";;
  tsan)  SAN="-fsanitize=thread"; export TSAN_OPTIONS="die_after_fork=0 ${TSAN_OPTIONS:-}";;      # (the tests that fork while the compactor thread runs: the child starts threads of its own)
  *) echo "usage: $0 asan|ubsan|tsan [tests]"; exit 2;;
esac
OUT=build/$KIND; BIN=dist/$KIND; mkdir -p $OUT $BIN; rm -f $OUT/*.o
CF="-O1 -g $SAN -fno-omit-frame-pointer -Itest -Isrc -Ithird_party/sqlite -DSQLITE_DISABLE_PAGECACHE_OVERFLOW_STATS"
[ "$(uname)" = Linux ] && CF="$CF -D_GNU_SOURCE"
for f in src/*.c; do "$CC" -w $CF -c $f -o $OUT/$(basename $f .c).o; done
"$CC" -w -O1 -g $SAN -fno-omit-frame-pointer -Ithird_party/sqlite -DSQLITE_EXTRA_INIT=mw_extra_init -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_FTS4 -DSQLITE_ENABLE_RTREE -DSQLITE_CORE -c third_party/sqlite/sqlite3.c -o $OUT/sqlite3.o
LIBS=$(ls $OUT/*.o | grep -v "/t_")
EXTRA=""; [ "$(uname)" = Darwin ] && EXTRA="-framework Security"
TESTS=${@:-"mw_serial mw_relocprep mw_reloc mw_lanes mw_rebaseneg mw_attachsync"}
status=0
for t in $TESTS; do
  b=$(basename $t)
  "$CC" -w $CF -c test/$t.c -o $OUT/t_$b.o
  TEST_LIBS=$LIBS
  SUPP=
  case $b in
    mw_identity|mw_multiproc) SUPP="suppressions=$PWD/test/tsan.supp";;      # (races inside SQLite: test/tsan.supp)
    mw_stagednormal)
      "$CC" -w $CF -Dpwritev=inject_pwritev -c src/multiwriter_log.c -o $OUT/injected.o
      TEST_LIBS="${LIBS//$OUT\/multiwriter_log.o/} $OUT/injected.o";;
    mw_holesync)
      "$CC" -w $CF -Dfsync=inject_fsync -c src/multiwriter_log.c -o $OUT/injected.o
      TEST_LIBS="${LIBS//$OUT\/multiwriter_log.o/} $OUT/injected.o";;
    mw_autoextfail)
      "$CC" -w $CF -Dsqlite3_auto_extension=inject_auto_extension -c src/multiwriter_vfs.c -o $OUT/injected.o
      TEST_LIBS="${LIBS//$OUT\/multiwriter_vfs.o/} $OUT/injected.o";;
    mw_walstatfail)
      TEST_LIBS="${LIBS//$OUT\/multiwriter_lane.o/}";;      # (the test includes multiwriter_lane.c itself)
    mw_admitcap)
      TEST_LIBS="${LIBS//$OUT\/multiwriter_pages.o/}";;      # (the test includes multiwriter_pages.c itself)
    mw_rowdiffoom)
      "$CC" -w $CF -Dmalloc=inject_malloc -Dcalloc=inject_calloc -Drealloc=inject_realloc -c src/multiwriter_rowdiff.c -o $OUT/injected.o
      TEST_LIBS="${LIBS//$OUT\/multiwriter_rowdiff.o/} $OUT/injected.o";;
  esac
  "$CC" $OUT/t_$b.o $TEST_LIBS -o $BIN/$b $EXTRA $SAN -lpthread -lm
  echo "== $KIND: $t"
  r=0; env ${SUPP:+TSAN_OPTIONS="${TSAN_OPTIONS:-} $SUPP"} perl -e 'alarm shift; exec @ARGV' ${MW_SAN_TIMEOUT:-600} ./$BIN/$b 2>&1 | tail -${TAIL:-6} || r=$?        # (pipefail: the status of the test, not of tail; the other tests still run)
  [ $r -eq 0 ] || { echo "== $KIND: $t FAILED ($r)"; [ $status -ne 0 ] || status=$r; }
done
exit $status
