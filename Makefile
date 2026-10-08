# sqlite-multiwriter: multi-writer SQLite as a wrapper VFS (macOS and Linux).
#   make            build the engine objects and the test programs
#   make test       run the test suite
#   make bench      build the benchmark   (dist/mw_bench)
# SQLite (the amalgamation) is vendored in third_party/sqlite; nothing else is needed to build and test. The engine registers its VFS from inside sqlite3_initialize()
# through the compile option SQLITE_EXTRA_INIT (sqlite3.c itself is not modified).

CC ?= cc
SQLITE_DIR := third_party/sqlite
SRC_DIR := src
BUILD := build
DIST := dist

ifdef EXPERIMENTS
CFLAGS += -DMW_EXPERIMENTS      # (make EXPERIMENTS=1: the knobs of measurement)
endif
CFLAGS += -O2 -g -Wall -Wno-unused-function -I$(SRC_DIR) -I$(SQLITE_DIR) -DSQLITE_DISABLE_PAGECACHE_OVERFLOW_STATS
SQLITE_FLAGS := -DSQLITE_EXTRA_INIT=mw_extra_init -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_FTS4 -DSQLITE_ENABLE_RTREE -DSQLITE_CORE
LDFLAGS += -lpthread -lm
ifeq ($(shell uname -s),Darwin)
LDFLAGS += -framework Security
endif
ifeq ($(shell uname -s),Linux)
CFLAGS += -D_GNU_SOURCE
LDFLAGS += -ldl
endif

ENGINE_SRC := $(wildcard $(SRC_DIR)/*.c)
ENGINE_OBJ := $(patsubst $(SRC_DIR)/%.c,$(BUILD)/%.o,$(ENGINE_SRC))
HEADERS := $(wildcard $(SRC_DIR)/*.h)
LIB_OBJ := $(ENGINE_OBJ) $(BUILD)/sqlite3.o

TEST_SRC := $(wildcard test/mw_*.c)
TEST_BIN := $(patsubst test/%.c,$(DIST)/%,$(TEST_SRC))
IO_TESTS := $(DIST)/mw_ioerr $(DIST)/mw_diskfull                       # (minutes: make test-io)
FAST_BIN := $(filter-out $(IO_TESTS),$(TEST_BIN))

.PHONY: all test bench clean test-mp test-io
all: $(TEST_BIN)

$(BUILD)/sqlite3.o: $(SQLITE_DIR)/sqlite3.c
	@mkdir -p $(BUILD)
	$(CC) $(CFLAGS) $(SQLITE_FLAGS) -w -c $< -o $@
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
	@set -e; for t in $(FAST_BIN); do echo "== $$t"; ./$$t; done; echo "== mw_rebase (a WITHOUT ROWID table)"; MW_TEST_WR=1 ./$(DIST)/mw_rebase

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

# ---- the loadable extension (dist/multiwriter.so|dylib): the engine without SQLite, every sqlite3_* call through the host's table of routines (sqlite3ext.h) ----
HOSTOS := $(shell uname -s)
ifeq ($(HOSTOS),Darwin)
EXT_SUFFIX := dylib
EXT_LDFLAGS := -dynamiclib -framework Security
else
EXT_SUFFIX := so
EXT_LDFLAGS := -shared -lpthread -lm -ldl
endif
EXT := $(DIST)/multiwriter.$(EXT_SUFFIX)
EXT_OBJ := $(patsubst $(SRC_DIR)/%.c,$(BUILD)/ext/%.o,$(ENGINE_SRC))
$(BUILD)/ext/%.o: $(SRC_DIR)/%.c $(HEADERS)
	@mkdir -p $(BUILD)/ext
	$(CC) $(CFLAGS) -DMW_LOADABLE -fPIC -c $< -o $@
$(EXT): $(EXT_OBJ)
	@mkdir -p $(DIST)
	$(CC) $^ -o $@ $(EXT_LDFLAGS)
.PHONY: extension
extension: $(EXT)
# SQLite as it is in a host (no engine) for the test of the loaded extension
$(BUILD)/sqlite3_plain.o: $(SQLITE_DIR)/sqlite3.c
	@mkdir -p $(BUILD)
	$(CC) $(CFLAGS) -DSQLITE_THREADSAFE=1 -w -c $< -o $@
$(BUILD)/t_loadable.o: test/loadable.c test/mw_test.h $(HEADERS)
	@mkdir -p $(BUILD)
	$(CC) $(CFLAGS) -c $< -o $@
$(DIST)/loadable: $(BUILD)/t_loadable.o $(BUILD)/sqlite3_plain.o
	@mkdir -p $(DIST)
	$(CC) $^ -o $@ -lpthread -lm -ldl
.PHONY: test-loadable
test-loadable: $(EXT) $(DIST)/loadable
	./$(DIST)/loadable ./$(EXT)

clean:
	rm -rf $(BUILD) $(DIST)

# the transaction tests with the shared multi-process mode (mw_mp=1) in place of the thread mode
MP_TESTS := mw_rebase mw_rebasefk mw_rebasefkstress mw_rebasenoop mw_rebaseskew mw_rebasewr mw_serial mw_relocprep mw_shared
.PHONY: test-mp
test-mp: $(addprefix $(DIST)/,$(MP_TESTS))
	@set -e; for t in $(MP_TESTS); do echo "== $$t (mw_mp=1)"; MW_TEST_MP=1 ./$(DIST)/$$t; done
