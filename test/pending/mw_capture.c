// Phase 9: sqlite-sync on private lanes. Unique db_versions per concurrent transaction and the
// export frontier: a change becomes syncable only once no unresolved transaction can still
// publish a lower db_version, and speculative (uncommitted/aborted) changes never appear.
#include "mw_test.h"
#include "multiwriter.h"

static int close_cs (sqlite3 *db) { return sqlite3_close(db); }
static uint64_t stats_rebases (sqlite3 *db) { mw_db_stats s; memset(&s, 0, sizeof s); sqlite3_file_control(db, "main", MW_FCNTL_DBSTATS, &s); return s.rebases; }
static int open_lane (const char *path, sqlite3 **db) {
    char uri[300];
    snprintf(uri, sizeof uri, "file:%s?mw=2&mw_gc=0", path);
    return sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
}
static int64_t chunks_rows (sqlite3 *db) { return mw_scalar(db, "SELECT COALESCE(sum(payload_size),0) FROM cloudsync_payload_chunks WHERE since_db_version=0"); }

int main (void) {
    char path[256];
    mw_tmpdb(path, sizeof path, "capture");

    sqlite3 *s;
    CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id TEXT PRIMARY KEY NOT NULL, a TEXT, pad BLOB);"), SQLITE_OK);
    CHECK_RC(close_cs(s), SQLITE_OK);

    sqlite3 *a, *b;
    CHECK_RC(open_lane(path, &a), SQLITE_OK);
    CHECK_RC(open_lane(path, &b), SQLITE_OK);
    sqlite3_extended_result_codes(a, 1); sqlite3_extended_result_codes(b, 1);

    // ---- outside any transaction nothing is reserved: the ceiling is simply the newest reserved version
    int64_t c0 = mw_db_send_ceiling(a);
    CHECK(c0 >= 0);

    // ---- two overlapping transactions reserve different db_versions
    CHECK_RC(mw_exec(a, "BEGIN; INSERT INTO t VALUES('a1','x',zeroblob(10))"), SQLITE_OK);

    CHECK_RC(mw_exec(b, "BEGIN; INSERT INTO t VALUES('b1','y',zeroblob(10))"), SQLITE_OK);
    int64_t ceiling_open = mw_db_send_ceiling(b);
    CHECK(ceiling_open < c0 + 1 + 0 || ceiling_open == c0);     // frontier sits below A's (lowest) reservation
    CHECK(mw_scalar(b, "SELECT cloudsync_send_ceiling()") == ceiling_open);

    // A commits first; B touches the same pages (table + meta + index): it is rebased, not refused.
    // Its logical changes keep the db_version it reserved.
    CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "COMMIT"), SQLITE_OK);
    CHECK(stats_rebases(a) == 1);
    CHECK(mw_scalar(a, "SELECT count(*) FROM t WHERE id IN ('a1','b1')") == 2);
    CHECK(distinct_versions(a) == 2);                          // one db_version per committed transaction

    // ---- frontier: an unresolved low reservation hides later commits from export
    int64_t before = chunks_rows(a);
    CHECK(before > 0);                                         // everything resolved: exportable
    CHECK_RC(mw_exec(a, "BEGIN; INSERT INTO t VALUES('a2','x',zeroblob(10))"), SQLITE_OK);   // reserves v1 and holds it
    int64_t hold = mw_db_send_ceiling(b);
    CHECK_RC(mw_exec(b, "INSERT INTO t VALUES('b2','y',zeroblob(10))"), SQLITE_OK);           // commits a higher version
    CHECK(mw_db_send_ceiling(b) == hold);                      // frontier did not move past the open transaction
    int64_t during = chunks_rows(b);
    CHECK(during == before);                                   // ... but is not exported while A is unresolved
    CHECK(mw_scalar(b, "SELECT count(*) FROM t WHERE id='a2'") == 0);   // A's speculative row is invisible to B

    // A aborts: its version is resolved, the frontier advances, b2 becomes exportable, a2 never existed
    CHECK_RC(mw_exec(a, "ROLLBACK"), SQLITE_OK);
    CHECK(mw_db_send_ceiling(b) > hold);
    CHECK(chunks_rows(b) > before);
    CHECK(mw_scalar(a, "SELECT count(*) FROM t") == 3);

    // ---- versions stay monotonic per reservation and the local db_version reflects committed state
    CHECK(mw_scalar(a, "SELECT cloudsync_db_version()") <= mw_db_send_ceiling(a));
    CHECK_RC(close_cs(a), SQLITE_OK); CHECK_RC(close_cs(b), SQLITE_OK);
    mw_rmdb(path);
    MW_DONE();
}
