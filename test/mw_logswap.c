// The log is rewritten (after a compaction) while a committing thread reserves room in it: the descriptor was closed under the committer ("disk I/O error", EBADF, found by
// SQLite's fts4merge4 test, one run in twenty). Many commits of several MB (each of them asks for room in the log) on one connection with the compactor running; none may fail.
// (a race: the old code failed in about half of the runs of this test, with the two delays below that widen the window.)
#include "mw_test.h"
#include "multiwriter.h"

int main (void) {
    setenv("MW_TEST_ROOM_DELAY_US", "1500", 1);                  // (the descriptor is read, and used 1.5 ms later)
    setenv("MW_TEST_SWAP_DELAY_US", "1500", 1);                  // (the gap, between the quiesce and the swap of the descriptor, in which a commit can ask for room in the old file: made wide)
    char path[256]; mw_tmpdb(path, sizeof path, "logswap");
    int fails = 0, rounds = getenv("MW_LOGSWAP_ROUNDS") ? atoi(getenv("MW_LOGSWAP_ROUNDS")) : 3;
    // the pattern that found it: SQLite's fts4merge4 test: 5 inserts of a document of 10000 words per transaction, the automatic merge writes segments of megabytes
    char *doc = malloc(10000 * 5 + 1); size_t n = 0;
    for (int i = 0; i < 10000; i++) n += (size_t)sprintf(doc + n, "%c%c%c ", 'a' + i % 10, 'a' + i / 10 % 10, 'a' + i / 100 % 10);
    for (int r = 0; r < rounds; r++) {
        char uri[300]; snprintf(uri, sizeof uri, "file:%s?mw=2&mw_compact_ms=1", path);
        sqlite3 *db; CHECK_RC(sqlite3_open_v2(uri, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
        sqlite3_extended_result_codes(db, 1);
        CHECK_RC(mw_exec(db, "DROP TABLE IF EXISTS t2; CREATE VIRTUAL TABLE t2 USING fts4; INSERT INTO t2(t2) VALUES('automerge=1')"), SQLITE_OK);
        sqlite3_stmt *st; sqlite3_prepare_v2(db, "INSERT INTO t2 VALUES(?1)", -1, &st, NULL);
        for (int i = 0; i < 100; i++) {
            int rc = mw_exec(db, "BEGIN");
            for (int k = 0; k < 5 && rc == SQLITE_OK; k++) { sqlite3_bind_text(st, 1, doc, (int)n, SQLITE_STATIC); rc = sqlite3_step(st); rc = rc == SQLITE_DONE ? SQLITE_OK : rc; sqlite3_reset(st); }
            if (rc == SQLITE_OK) rc = mw_exec(db, "COMMIT");
            if (rc != SQLITE_OK) { fails++; if (getenv("MW_VERBOSE")) printf("round %d i %d rc %d\n", r, i, rc); mw_exec(db, "ROLLBACK"); break; }
        }
        { mw_db_stats ds; memset(&ds, 0, sizeof ds); sqlite3_file_control(db, "main", MW_FCNTL_DBSTATS, &ds); if (getenv("MW_VERBOSE")) printf("round %d: %llu compactions, %llu commits\n", r, (unsigned long long)ds.compactions, (unsigned long long)ds.commits); }
        sqlite3_finalize(st); sqlite3_close(db);
    }
    free(doc);
    // and commits of several MB, each asks for room in the log
    {
        char uri[300]; snprintf(uri, sizeof uri, "file:%s?mw=2&mw_compact_ms=1", path);
        sqlite3 *db; CHECK_RC(sqlite3_open_v2(uri, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
        CHECK_RC(mw_exec(db, "CREATE TABLE IF NOT EXISTS big(id INTEGER PRIMARY KEY, b BLOB)"), SQLITE_OK);
        for (int i = 0; i < 300; i++) {
            int rc = mw_exec(db, "INSERT INTO big(b) VALUES(randomblob(5000000)); DELETE FROM big WHERE id % 2 = 0");
            if (rc != SQLITE_OK) { fails++; if (getenv("MW_VERBOSE")) printf("big %d rc %d\n", i, rc); break; }
        }
        sqlite3_close(db);
    }
    CHECK(fails == 0);
    mw_rmfiles(path);
    printf("test/mw_logswap.c: %d failure(s)\n", mw_failures);
    return mw_failures ? 1 : 0;
}
