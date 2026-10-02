#!/bin/bash
# Builds tests with a sanitizer and runs them.
#   test/sanitize.sh asan|ubsan|tsan [test ...]        (from the repository root)
# asan: memory errors; ubsan: undefined behaviour; tsan: data races (SQLite itself and the test programs are reported too: read the first frames,
# only the multiwriter_*.c ones concern the engine). Default tests: the metadata ones. Oracle tests (oracle_*) link sqlite-sync's sources too.
set -e
KIND=${1:-asan}; shift || true
case $KIND in
  asan)  SAN="-fsanitize=address";;
  ubsan) SAN="-fsanitize=undefined,alignment,integer -fno-sanitize=unsigned-integer-overflow,unsigned-shift-base,implicit-conversion -fno-sanitize-recover=undefined";;
  tsan)  SAN="-fsanitize=thread";;
  *) echo "usage: $0 asan|ubsan|tsan [tests]"; exit 2;;
esac
OUT=build/$KIND; BIN=dist/$KIND; mkdir -p $OUT/oracle $BIN
SS=deps/sqlite-sync
CF="-O1 -g $SAN -fno-omit-frame-pointer -Isrc -Isrc/crdt -I$SS/sqlite -I$SS/src -DSQLITE_DISABLE_PAGECACHE_OVERFLOW_STATS"
for f in src/*.c src/crdt/*.c; do cc -w $CF -c $f -o $OUT/$(basename $f .c).o; done
cc -w -O1 -g $SAN -fno-omit-frame-pointer -I$SS/sqlite -DSQLITE_EXTRA_INIT=mw_extra_init -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_RTREE -DSQLITE_CORE -c $SS/sqlite/sqlite3.c -o $OUT/sqlite3.o
cc -w -O1 -g $SAN -c $SS/src/lz4.c -o $OUT/lz4.o
OCF="-O1 -g $SAN -w -I$SS/src -I$SS/src/sqlite -I$SS/src/network -I$SS/modules/fractional-indexing -I$SS/sqlite -DSQLITE_CORE -DCLOUDSYNC_OMIT_NETWORK -DCLOUDSYNC_OMIT_PRINT_RESULT"
NEED_ORACLE=0
TESTS=${@:-"mw_metastore mw_ddl_meta mw_mpmeta mw_sync mw_capture mw_rowdiff oracle_meta oracle_sync oracle_features"}
for t in $TESTS; do case $t in oracle_*) NEED_ORACLE=1;; esac; done
if [ $NEED_ORACLE = 1 ]; then
  for f in $SS/src/cloudsync.c $SS/src/dbutils.c $SS/src/pk.c $SS/src/utils.c $SS/src/block.c $SS/src/network/network.c \
           $SS/src/sqlite/cloudsync_changes_sqlite.c $SS/src/sqlite/cloudsync_sqlite.c $SS/src/sqlite/database_sqlite.c $SS/src/sqlite/sql_sqlite.c $SS/modules/fractional-indexing/fractional_indexing.c; do
    cc $OCF -c $f -o $OUT/oracle/$(basename $f .c).o
  done
fi
LIBS=$(ls $OUT/*.o | grep -v "/t_")
EXTRA=""; [ "$(uname)" = Darwin ] && EXTRA="-framework Security"
for t in $TESTS; do
  b=$(basename $t)
  cc -w $CF -c test/$t.c -o $OUT/t_$b.o
  case $t in oracle_*) OBJ="$OUT/oracle/*.o";; *) OBJ="";; esac
  cc $OUT/t_$b.o $LIBS $OBJ -o $BIN/$b $EXTRA $SAN -lpthread -lm
  echo "== $KIND: $t"
  ./$BIN/$b 2>&1 | tail -${TAIL:-6}
done
