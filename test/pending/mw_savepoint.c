// Phase 15: savepoints. Savepoints live entirely inside the pager/VDBE: the physical write set is what
// SQLite commits and the logical changes (sqlite-sync metadata rows) roll back with the same savepoints,
// because they are ordinary rows of the same transaction. The test proves both representations describe
// exactly the same effective transaction: the outcome after a fast commit equals the outcome after a
// rebase (which replays only the logical representation).
#include "mw_test.h"
#include "multiwriter.h"

static int close_cs (sqlite3 *db) { return sqlite3_close(db); }
static int open_lane (const char *path, sqlite3 **db) {
    char uri[300];
    snprintf(uri, sizeof uri, "file:%s?mw=2&mw_gc=0&mw_readcheck=0", path);
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) sqlite3_extended_result_codes(*db, 1);
    return rc;
}
static uint64_t rebases (sqlite3 *db) { mw_db_stats s; memset(&s, 0, sizeof s); sqlite3_file_control(db, "main", MW_FCNTL_DBSTATS, &s); return s.rebases; }

static char *dump (sqlite3 *db, const char *sql, char *out, size_t n) {
    out[0] = 0;
    sqlite3_stmt *st;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return out;
    int cols = sqlite3_column_count(st);
    while (sqlite3_step(st) == SQLITE_ROW) {
        for (int i = 0; i < cols; i++) { const char *t = (const char *)sqlite3_column_text(st, i); strncat(out, t ? t : "NULL", n - strlen(out) - 2); strncat(out, i + 1 < cols ? "," : ";", n - strlen(out) - 1); }
    }
    sqlite3_finalize(st);
    return out;
}

static const char *SCRIPT =
    "UPDATE t SET a='A1' WHERE id='r1';"
    "SAVEPOINT s1;"
    "UPDATE t SET b='B1' WHERE id='r1';"
    "INSERT INTO t VALUES('n1','x','x','x','x','x');"
    "SAVEPOINT s2;"
    "UPDATE t SET c='C1' WHERE id='r1';"
    "DELETE FROM t WHERE id='r2';"
    "ROLLBACK TO s2;"
    "UPDATE t SET d='D1' WHERE id='r1';"
    "ROLLBACK TO s1;"
    "UPDATE t SET e='E1' WHERE id='r1';"
    "INSERT INTO t VALUES('n2','y','y','y','y','y');"
    "RELEASE s1;";

static void scenario (const char *tag, int with_conflict, char *state, char *meta) {
    char path[256]; mw_tmpdb(path, sizeof path, tag);
    sqlite3 *s, *a, *b;
    CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id TEXT PRIMARY KEY NOT NULL, a TEXT, b TEXT, c TEXT, d TEXT, e TEXT);"
                        "INSERT INTO t VALUES('r1','a0','b0','c0','d0','e0'),('r2','a0','b0','c0','d0','e0');"), SQLITE_OK);
    CHECK_RC(close_cs(s), SQLITE_OK);
    CHECK_RC(open_lane(path, &a), SQLITE_OK);
    CHECK_RC(open_lane(path, &b), SQLITE_OK);

    char sql[1200];
    snprintf(sql, sizeof sql, "BEGIN; %s", SCRIPT);
    CHECK_RC(mw_exec(a, sql), SQLITE_OK);
    if (with_conflict) CHECK_RC(mw_exec(b, "INSERT INTO t VALUES('z','z','z','z','z','z')"), SQLITE_OK);   // same pages, commits first
    CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_OK);
    CHECK(rebases(a) == (uint64_t)(with_conflict ? 1 : 0));

    dump(a, "SELECT id,a,b,c,d,e FROM t WHERE id<>'z' ORDER BY id", state, 1024);
    // logical representation, without the values that legitimately differ (db_version / seq / site)
    close_cs(a); close_cs(b); mw_rmdb(path);
}

int main (void) {
    char st0[1024], m0[4096], st1[1024], m1[4096];
    scenario("sp0", 0, st0, m0);
    scenario("sp1", 1, st1, m1);
    printf("fast:    %s\nrebased: %s\n", st0, st1);
    // effective transaction: r1.a and r1.e changed, s1's work (b, n1, c, d, delete r2) undone, n2 inserted after
    CHECK(strcmp(st0, "n2,y,y,y,y,y;r1,A1,b0,c0,d0,E1;r2,a0,b0,c0,d0,e0;") == 0);
    CHECK(strcmp(st0, st1) == 0);                          // physical fast commit == logical replay
    CHECK(strcmp(m0, m1) == 0);                            // and so are the sqlite-sync change sets
    CHECK(strstr(m0, "n1") == NULL);                        // (rolled-back insert left no logical trace)
    MW_DONE();
}
