// mw_ensure_wal_db() looks for a real -wal next to the database before and after it checkpoints it. A stat() of the -wal that fails (not "no such file") says nothing about whether it
// holds committed frames: it was read as "none", so the first look could take the clean-header success path, and the look after the checkpoint could accept a WAL that a reader had kept
// from being checkpointed. Now the error is the result (SQLITE_IOERR_FSTAT), and the database is not opened by the engine on it.
// The test includes multiwriter_lane.c with stat() and sqlite3_busy_timeout() replaced (after the system headers: a -D would rename the declaration of stat too).
#include "mw_test.h"
#ifndef _WIN32
#include <errno.h>
#include <sys/stat.h>

static int fault_at, wal_stats, failures;
static int inject_stat (const char *path, struct stat *out) {
    size_t n = strlen(path);
    if (n >= 4 && !strcmp(path + n - 4, "-wal") && ++wal_stats == fault_at) { failures++; errno = EIO; return -1; }
    return stat(path, out);
}
static int inject_busy_timeout (sqlite3 *db, int ms) { (void)ms; return sqlite3_busy_timeout(db, 0); }     // (the pinned reader below is on purpose: do not wait 5 s for it)
#define stat(...) inject_stat(__VA_ARGS__)
#define sqlite3_busy_timeout(db, ms) inject_busy_timeout(db, ms)
#include "multiwriter_lane.c"
#undef sqlite3_busy_timeout
#undef stat

static int value (sqlite3 *db) {
    sqlite3_stmt *s = NULL; int v = -1;
    if (sqlite3_prepare_v2(db, "SELECT v FROM t", -1, &s, NULL) == SQLITE_OK && sqlite3_step(s) == SQLITE_ROW) v = sqlite3_column_int(s, 0);
    sqlite3_finalize(s);
    return v;
}

// A database in WAL mode with a -wal that a reader (at 1) keeps from being checkpointed after a commit of 2; the answer of mw_ensure_wal_db() with the n-th stat of the -wal failing
// (0: none fails). The files and both snapshots must be as they were.
static int ensure (int n, long long *wal_bytes) {
    char path[256]; mw_tmpdb(path, sizeof path, "walstatfail");
    sqlite3 *w = NULL, *r = NULL;
    CHECK_RC(sqlite3_open(path, &w), SQLITE_OK);
    CHECK_RC(sqlite3_exec(w, "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE t(v); INSERT INTO t VALUES(1)", NULL, NULL, NULL), SQLITE_OK);
    CHECK_RC(sqlite3_wal_checkpoint_v2(w, NULL, SQLITE_CHECKPOINT_TRUNCATE, NULL, NULL), SQLITE_OK);
    CHECK_RC(sqlite3_open(path, &r), SQLITE_OK);
    CHECK_RC(sqlite3_exec(r, "BEGIN; SELECT v FROM t", NULL, NULL, NULL), SQLITE_OK);
    CHECK_RC(sqlite3_exec(w, "UPDATE t SET v=2", NULL, NULL, NULL), SQLITE_OK);
    char wal[300]; snprintf(wal, sizeof wal, "%s-wal", path);
    struct stat before, after;
    CHECK(stat(wal, &before) == 0 && before.st_size > 32);
    fault_at = n; wal_stats = 0; failures = 0;
    int rc = mw_ensure_wal_db(path);
    fault_at = 0;
    int clean = mw_path_is_clean_wal_db(path);
    CHECK(stat(wal, &after) == 0 && after.st_size == before.st_size);
    CHECK(value(r) == 1 && value(w) == 2);
    CHECK(!clean);                                  // (a -wal with frames is never "clean")
    if (n) CHECK(failures >= 1);
    *wal_bytes = after.st_size;
    sqlite3_exec(r, "ROLLBACK", NULL, NULL, NULL);
    sqlite3_close(r); sqlite3_close(w);
    return rc;
}
#endif

int main (void) {
#ifndef _WIN32
    long long bytes;
    CHECK_RC(ensure(0, &bytes), SQLITE_BUSY);                       // control: the reader keeps the checkpoint from finishing, and it is said
    CHECK_RC(ensure(1, &bytes), SQLITE_IOERR_FSTAT);                // the first look fails
    CHECK_RC(ensure(2, &bytes), SQLITE_IOERR_FSTAT);                // the look after the checkpoint fails

    // no -wal at all is not an error: a new database is converted
    char path[256]; mw_tmpdb(path, sizeof path, "walstatfail_new");
    fault_at = 0;
    CHECK_RC(mw_ensure_wal_db(path), SQLITE_OK);
    CHECK(mw_path_is_clean_wal_db(path));

    // a stat that fails with EIO on a clean database: the engine does not guess
    fault_at = 1; wal_stats = 0;
    CHECK_RC(mw_ensure_wal_db(path), SQLITE_IOERR_FSTAT);
    wal_stats = 0;
    CHECK(!mw_path_is_clean_wal_db(path));
    fault_at = 0;
#endif
    printf("test/mw_walstatfail.c: %d failure(s)\n", mw_failures);
    return mw_failures ? 1 : 0;
}
