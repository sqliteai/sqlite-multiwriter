# sqlite-multiwriter: multi-writer SQLite as a wrapper VFS (macOS first; other platforms later).
#   make            build the engine objects and the test programs
#   make test       run the test suite
#   make bench      build the benchmark   (dist/mw_bench)
# SQLite comes from the amalgamation shipped in deps/sqlite-sync (a submodule); the engine registers its VFS from inside sqlite3_initialize()
# through the compile option SQLITE_EXTRA_INIT (sqlite3.c itself is not modified).

CC ?= cc
SQLITE_DIR := deps/sqlite-sync/sqlite
SRC_DIR := src
BUILD := build
DIST := dist

ifdef EXPERIMENTS
CFLAGS += -DMW_EXPERIMENTS      # (make EXPERIMENTS=1: the knobs of measurement MW_EXP_*, MW_META_NOMERGE, MW_META_NOCOMPRESS)
endif
CFLAGS += -O2 -g -Wall -Wno-unused-function -I$(SRC_DIR) -I$(SRC_DIR)/crdt -I$(SQLITE_DIR) -Ideps/sqlite-sync/src -DSQLITE_DISABLE_PAGECACHE_OVERFLOW_STATS
SQLITE_FLAGS := -DSQLITE_EXTRA_INIT=mw_extra_init -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_RTREE -DSQLITE_CORE
LDFLAGS += -lpthread -lm
ifeq ($(shell uname -s),Darwin)
LDFLAGS += -framework Security
endif
ifeq ($(shell uname -s),Linux)
CFLAGS += -D_GNU_SOURCE
LDFLAGS += -ldl
endif

ENGINE_SRC := $(wildcard $(SRC_DIR)/*.c)
CRDT_SRC := $(wildcard $(SRC_DIR)/crdt/*.c)
CRDT_OBJ := $(patsubst $(SRC_DIR)/crdt/%.c,$(BUILD)/crdt_%.o,$(CRDT_SRC))
ENGINE_OBJ := $(patsubst $(SRC_DIR)/%.c,$(BUILD)/%.o,$(ENGINE_SRC))
HEADERS := $(wildcard $(SRC_DIR)/*.h) $(wildcard $(SRC_DIR)/crdt/*.h)
LZ4_OBJ := $(BUILD)/lz4.o                      # (the payload container of sqlite-sync compresses with LZ4: the submodule's own copy)
LIB_OBJ := $(ENGINE_OBJ) $(CRDT_OBJ) $(LZ4_OBJ) $(BUILD)/sqlite3.o

TEST_SRC := $(wildcard test/mw_*.c)
TEST_BIN := $(patsubst test/%.c,$(DIST)/%,$(TEST_SRC))
IO_TESTS := $(DIST)/mw_ioerr $(DIST)/mw_diskfull                       # (minutes: make test-io)
FAST_BIN := $(filter-out $(IO_TESTS),$(TEST_BIN))

.PHONY: all test bench clean
all: $(TEST_BIN)

$(BUILD)/sqlite3.o: $(SQLITE_DIR)/sqlite3.c
	@mkdir -p $(BUILD)
	$(CC) $(CFLAGS) $(SQLITE_FLAGS) -w -c $< -o $@
$(BUILD)/lz4.o: deps/sqlite-sync/src/lz4.c
	@mkdir -p $(BUILD)
	$(CC) -O2 -w -c $< -o $@
$(BUILD)/crdt_%.o: $(SRC_DIR)/crdt/%.c $(wildcard $(SRC_DIR)/crdt/*.h)
	@mkdir -p $(BUILD)
	$(CC) $(CFLAGS) -c $< -o $@
$(BUILD)/%.o: $(SRC_DIR)/%.c $(HEADERS)
	@mkdir -p $(BUILD)
	$(CC) $(CFLAGS) -c $< -o $@
$(BUILD)/t_%.o: test/%.c $(HEADERS) test/mw_test.h
	@mkdir -p $(BUILD)
	$(CC) $(CFLAGS) -c $< -o $@
$(DIST)/mw_%: $(BUILD)/t_mw_%.o $(LIB_OBJ)
	@mkdir -p $(DIST)
	$(CC) $^ -o $@ $(LDFLAGS)
.PRECIOUS: $(BUILD)/t_%.o

test: $(FAST_BIN)
	@set -e; for t in $(FAST_BIN); do echo "== $$t"; ./$$t; done

# errors of the file system: a fault at every n-th file call of a workload, and a real full disk (a small disk image)
.PHONY: test-io
test-io: $(IO_TESTS)
	@set -e; for t in $(IO_TESTS); do echo "== $$t"; ./$$t; done

bench: $(DIST)/mw_bench
$(BUILD)/b_mw_bench.o: bench/mw_bench.c $(HEADERS)
	@mkdir -p $(BUILD)
	$(CC) $(CFLAGS) -c $< -o $@
$(DIST)/mw_bench: $(BUILD)/b_mw_bench.o $(LIB_OBJ)
	@mkdir -p $(DIST)
	$(CC) $^ -o $@ $(LDFLAGS)

clean:
	rm -rf $(BUILD) $(DIST)

# ---- the oracle: sqlite-sync itself (the submodule), linked into the differential tests (test/oracle_*.c) -----------------------------------------------------
SS := deps/sqlite-sync
ORACLE_SRC := $(SS)/src/cloudsync.c $(SS)/src/dbutils.c $(SS)/src/pk.c $(SS)/src/utils.c $(SS)/src/block.c $(wildcard $(SS)/src/network/*.c) \
              $(SS)/src/sqlite/cloudsync_changes_sqlite.c $(SS)/src/sqlite/cloudsync_sqlite.c $(SS)/src/sqlite/database_sqlite.c $(SS)/src/sqlite/sql_sqlite.c \
              $(SS)/modules/fractional-indexing/fractional_indexing.c
ORACLE_OBJ := $(patsubst %.c,$(BUILD)/oracle/%.o,$(notdir $(ORACLE_SRC)))
ORACLE_CFLAGS := -O1 -w -I$(SS)/src -I$(SS)/src/sqlite -I$(SS)/src/network -I$(SS)/modules/fractional-indexing -I$(SQLITE_DIR) -DSQLITE_CORE -DCLOUDSYNC_OMIT_NETWORK -DCLOUDSYNC_OMIT_PRINT_RESULT
vpath %.c $(SS)/src $(SS)/src/sqlite $(SS)/src/network $(SS)/modules/fractional-indexing
$(BUILD)/oracle/%.o: %.c
	@mkdir -p $(BUILD)/oracle
	$(CC) $(ORACLE_CFLAGS) -c $< -o $@
ORACLE_TEST_SRC := $(wildcard test/oracle_*.c)
ORACLE_TEST_BIN := $(patsubst test/%.c,$(DIST)/%,$(ORACLE_TEST_SRC))
$(BUILD)/t_oracle_%.o: test/oracle_%.c $(HEADERS) test/mw_test.h
	@mkdir -p $(BUILD)
	$(CC) $(CFLAGS) -I$(SS)/src -I$(SS)/src/sqlite -c $< -o $@
$(DIST)/oracle_%: $(BUILD)/t_oracle_%.o $(LIB_OBJ) $(ORACLE_OBJ)
	@mkdir -p $(DIST)
	$(CC) $^ -o $@ $(LDFLAGS)
.PHONY: oracle-test
oracle-test: $(ORACLE_TEST_BIN)
	@set -e; for t in $(ORACLE_TEST_BIN); do echo "== $$t"; ./$$t; done

# the metadata tests again with the shared multi-process mode (mw_mp=1) in place of the thread mode
.PHONY: test-mp
# the metadata tests with the store under pressure: a cache of one megabyte (rows are read from the runs), a flush after every row, merges of two runs in parts of 40 rows
STRESS_TESTS := mw_runstore mw_metastore mw_ddl_meta mw_sync mw_mpmeta oracle_meta oracle_sync
test-stress: $(addprefix $(DIST)/,$(STRESS_TESTS))
	@set -e; for t in $(STRESS_TESTS); do echo "== $$t (under pressure)"; MW_META_CACHE_MB=1 MW_META_FANOUT=2 MW_META_PART_ROWS=40 MW_META_FLUSH_ROWS=1 MW_META_FLUSH_MS=1 ./$(DIST)/$$t; done

test-mp: $(DIST)/mw_metastore $(DIST)/mw_ddl_meta $(DIST)/mw_sync $(DIST)/oracle_meta $(DIST)/oracle_sync $(DIST)/mw_mpmeta $(DIST)/mw_runstore
	@set -e; for t in mw_metastore mw_ddl_meta mw_sync oracle_meta oracle_sync mw_mpmeta mw_runstore; do echo "== $$t (mw_mp=1)"; MW_TEST_MP=1 ./$(DIST)/$$t; done
