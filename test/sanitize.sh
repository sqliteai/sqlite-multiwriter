#!/bin/bash
# Builds tests with a sanitizer and runs them.
#   test/sanitize.sh asan|ubsan|tsan [test ...]        (from the repository root)
# asan: memory errors; ubsan: undefined behaviour; tsan: data races (SQLite itself and the test programs are reported too: read the first frames,
# only the multiwriter_*.c ones concern the engine). Default tests: the transaction ones.
set -euo pipefail
KIND=${1:-asan}; shift || true
case $KIND in
  asan)  SAN="-fsanitize=address";;
  ubsan) SAN="-fsanitize=undefined,alignment,integer -fno-sanitize=unsigned-integer-overflow,unsigned-shift-base,implicit-conversion -fno-sanitize-recover=undefined";;
  tsan)  SAN="-fsanitize=thread";;
  *) echo "usage: $0 asan|ubsan|tsan [tests]"; exit 2;;
esac
OUT=build/$KIND; BIN=dist/$KIND; mkdir -p $OUT $BIN; rm -f $OUT/*.o
CF="-O1 -g $SAN -fno-omit-frame-pointer -Itest -Isrc -Ithird_party/sqlite -DSQLITE_DISABLE_PAGECACHE_OVERFLOW_STATS"
[ "$(uname)" = Linux ] && CF="$CF -D_GNU_SOURCE"
for f in src/*.c; do cc -w $CF -c $f -o $OUT/$(basename $f .c).o; done
cc -w -O1 -g $SAN -fno-omit-frame-pointer -Ithird_party/sqlite -DSQLITE_EXTRA_INIT=mw_extra_init -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_RTREE -DSQLITE_CORE -c third_party/sqlite/sqlite3.c -o $OUT/sqlite3.o
LIBS=$(ls $OUT/*.o | grep -v "/t_")
EXTRA=""; [ "$(uname)" = Darwin ] && EXTRA="-framework Security"
TESTS=${@:-"mw_serial mw_relocprep mw_reloc mw_lanes mw_rebaseneg mw_attachsync"}
status=0
for t in $TESTS; do
  b=$(basename $t)
  cc -w $CF -c test/$t.c -o $OUT/t_$b.o
  TEST_LIBS=$LIBS
  case $b in
    mw_stagednormal)
      cc -w $CF -Dpwritev=inject_pwritev -c src/multiwriter_log.c -o $OUT/injected.o
      TEST_LIBS="${LIBS//$OUT\/multiwriter_log.o/} $OUT/injected.o";;
    mw_holesync)
      cc -w $CF -Dfsync=inject_fsync -c src/multiwriter_log.c -o $OUT/injected.o
      TEST_LIBS="${LIBS//$OUT\/multiwriter_log.o/} $OUT/injected.o";;
    mw_autoextfail)
      cc -w $CF -Dsqlite3_auto_extension=inject_auto_extension -c src/multiwriter_vfs.c -o $OUT/injected.o
      TEST_LIBS="${LIBS//$OUT\/multiwriter_vfs.o/} $OUT/injected.o";;
    mw_rowdiffoom)
      cc -w $CF -Dmalloc=inject_malloc -Dcalloc=inject_calloc -Drealloc=inject_realloc -c src/multiwriter_rowdiff.c -o $OUT/injected.o
      TEST_LIBS="${LIBS//$OUT\/multiwriter_rowdiff.o/} $OUT/injected.o";;
  esac
  cc $OUT/t_$b.o $TEST_LIBS -o $BIN/$b $EXTRA $SAN -lpthread -lm
  echo "== $KIND: $t"
  r=0; ./$BIN/$b 2>&1 | tail -${TAIL:-6} || r=$?        # (pipefail: the status of the test, not of tail; the other tests still run)
  [ $r -eq 0 ] || { echo "== $KIND: $t FAILED ($r)"; [ $status -ne 0 ] || status=$r; }
done
exit $status
