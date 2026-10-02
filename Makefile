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

CFLAGS += -O2 -g -Wall -Wno-unused-function -I$(SRC_DIR) -I$(SQLITE_DIR) -DSQLITE_DISABLE_PAGECACHE_OVERFLOW_STATS
SQLITE_FLAGS := -DSQLITE_EXTRA_INIT=mw_extra_init -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_RTREE -DSQLITE_CORE
LDFLAGS += -lpthread -lm
ifeq ($(shell uname -s),Darwin)
LDFLAGS += -framework Security
endif

ENGINE_SRC := $(wildcard $(SRC_DIR)/*.c)
ENGINE_OBJ := $(patsubst $(SRC_DIR)/%.c,$(BUILD)/%.o,$(ENGINE_SRC))
HEADERS := $(wildcard $(SRC_DIR)/*.h)
LIB_OBJ := $(ENGINE_OBJ) $(BUILD)/sqlite3.o

TEST_SRC := $(wildcard test/mw_*.c)
TEST_BIN := $(patsubst test/%.c,$(DIST)/%,$(TEST_SRC))

.PHONY: all test bench clean
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

test: $(TEST_BIN)
	@set -e; for t in $(TEST_BIN); do echo "== $$t"; ./$$t; done

bench: $(DIST)/mw_bench
$(BUILD)/b_mw_bench.o: bench/mw_bench.c $(HEADERS)
	@mkdir -p $(BUILD)
	$(CC) $(CFLAGS) -c $< -o $@
$(DIST)/mw_bench: $(BUILD)/b_mw_bench.o $(LIB_OBJ)
	@mkdir -p $(DIST)
	$(CC) $^ -o $@ $(LDFLAGS)

clean:
	rm -rf $(BUILD) $(DIST)
