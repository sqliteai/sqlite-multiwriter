# sqlite-multiwriter: multi-writer SQLite as a wrapper VFS.
#   make            build the engine objects and the test programs
#   make test       run the test suite
#   make bench      build the benchmark   (dist/mw_bench)
#   make extension [PLATFORM=..] [ARCH=..]   the loadable extension (dist/multiwriter.so|dylib|dll): see mk/extension.mk, `make help`
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
include mk/platform.mk
LDFLAGS += $(if $(filter android,$(PLATFORM)),,-lpthread) -lm
ifeq ($(PLATFORM),linux)
CFLAGS += -D_GNU_SOURCE
LDFLAGS += -ldl
endif
ifeq ($(PLATFORM),android)
CFLAGS += -D_GNU_SOURCE      # (tests for a device or an emulator: make all PLATFORM=android CC=<ndk>/aarch64-linux-android26-clang)
LDFLAGS += -ldl
endif
ifeq ($(PLATFORM),linux-musl)
CFLAGS += -D_GNU_SOURCE
endif
ifeq ($(PLATFORM),windows)
LDFLAGS += -static          # (no DLL of MinGW next to the programs)
endif

ENGINE_SRC := $(wildcard $(SRC_DIR)/*.c)
ENGINE_OBJ := $(patsubst $(SRC_DIR)/%.c,$(BUILD)/%.o,$(ENGINE_SRC))
HEADERS := $(wildcard $(SRC_DIR)/*.h)
LIB_OBJ := $(ENGINE_OBJ) $(BUILD)/sqlite3.o

TEST_SRC := $(wildcard test/mw_*.c)
ifeq ($(PLATFORM),windows)
# the tests that fork, wait for processes, map files or set the environment of a child are not for Windows (docs/windows.md)
TEST_SRC := $(filter-out $(filter-out $(shell grep -l MW_PORTABLE_TEST $(TEST_SRC)),$(shell grep -lE 'sys/wait.h|sys/mman.h|fork|setenv' $(TEST_SRC))) test/mw_relocprep.c,$(TEST_SRC))
endif
TEST_BIN := $(patsubst test/%.c,$(DIST)/%$(EXE),$(TEST_SRC))
IO_TESTS := $(DIST)/mw_ioerr$(EXE) $(DIST)/mw_diskfull$(EXE)                       # (minutes: make test-io)
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
$(DIST)/mw_%$(EXE): $(BUILD)/t_mw_%.o $(LIB_OBJ)
	@mkdir -p $(DIST)
	$(CC) $^ -o $@ $(LDFLAGS)
.PRECIOUS: $(BUILD)/t_%.o

test: $(FAST_BIN)
	@set -e; for t in $(FAST_BIN); do echo "== $$t"; ./$$t; done; echo "== mw_rebase (a WITHOUT ROWID table)"; MW_TEST_WR=1 ./$(DIST)/mw_rebase$(EXE)

# the suite of the CI legs that run it on shared machines: without mw_serial (about a minute and a half of stress, which has a threshold of events that a loaded machine can miss); `make test` runs everything
CI_BIN := $(filter-out $(DIST)/mw_serial$(EXE),$(FAST_BIN))
.PHONY: test-ci
test-ci: $(CI_BIN)
	@set -e; for t in $(CI_BIN); do echo "== $$t"; ./$$t; done; echo "== mw_rebase (a WITHOUT ROWID table)"; MW_TEST_WR=1 ./$(DIST)/mw_rebase$(EXE)

# errors of the file system: a fault at every n-th file call of a workload, and a real full disk (a small disk image)
.PHONY: test-io
test-io: $(IO_TESTS)
	@set -e; for t in $(IO_TESTS); do echo "== $$t"; ./$$t; done

bench: $(DIST)/mw_bench$(EXE)
$(BUILD)/b_mw_bench.o: bench/mw_bench.c $(HEADERS)
	@mkdir -p $(BUILD)
	$(CC) $(CFLAGS) -c $< -o $@
$(DIST)/mw_bench$(EXE): $(BUILD)/b_mw_bench.o $(LIB_OBJ)
	@mkdir -p $(DIST)
	$(CC) $^ -o $@ $(LDFLAGS)

include mk/extension.mk
include mk/package.mk

clean:
	rm -rf $(BUILD) $(DIST)

# the transaction tests with the shared multi-process mode (mw_mp=1) in place of the thread mode
MP_TESTS := mw_rebase mw_rebasefk mw_rebasefkstress mw_rebasenoop mw_rebaseskew mw_rebasewr mw_serial mw_relocprep mw_shared
ifeq ($(PLATFORM),windows)
MP_TESTS := mw_rebase mw_rebasefk mw_rebasefkstress mw_rebasenoop mw_rebaseskew mw_rebasewr mw_serial     # (the others fork)
endif
.PHONY: test-mp
test-mp: $(addsuffix $(EXE),$(addprefix $(DIST)/,$(MP_TESTS)))
	@set -e; for t in $(MP_TESTS); do echo "== $$t (mw_mp=1)"; MW_TEST_MP=1 ./$(DIST)/$$t; done
