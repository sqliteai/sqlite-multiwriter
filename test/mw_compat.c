// Phase 19: backup, journal modes, mmap, virtual tables and unsupported configurations.
#include "mw_test.h"
#include "multiwriter.h"

static int open_lane (const char *path, sqlite3 **db, const char *extra) {
    char uri[400];
    snprintf(uri, sizeof uri, "file:%s?mw=2%s", path, extra);
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) sqlite3_extended_result_codes(*db, 1);
    return rc;
}
static int integrity_ok (sqlite3 *db) {
    sqlite3_stmt *st; int ok = 0;
    if (sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) ok = strcmp((const char *)sqlite3_column_text(st, 0), "ok") == 0;
    sqlite3_finalize(st);
    return ok;
}
static int64_t stock_scalar (const char *path, const char *sql) {
    char uri[400]; snprintf(uri, sizeof uri, "file:%s?immutable=1", path);
    sqlite3 *c; int64_t v = -1;
    if (sqlite3_open_v2(uri, &c, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, "unix") == SQLITE_OK) v = mw_scalar(c, sql);
    sqlite3_close(c);
    return v;
}
static char *text (sqlite3 *db, const char *sql) {
    static char buf[256]; buf[0] = 0;
    sqlite3_stmt *st;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0)) snprintf(buf, sizeof buf, "%s", (const char *)sqlite3_column_text(st, 0));
    sqlite3_finalize(st);
    return buf;
}

int main (void) {
    char path[256], out[256], lp[300];
    mw_tmpdb(path, sizeof path, "compat"); snprintf(lp, sizeof lp, "%s-mw", path); unlink(lp);
    snprintf(out, sizeof out, "/tmp/mw_compat_%d_backup.db", (int)getpid()); unlink(out);
    sqlite3 *s, *a;
    CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base');"), SQLITE_OK);
    sqlite3_close(s);
    CHECK_RC(open_lane(path, &a, ""), SQLITE_OK);
    CHECK_RC(mw_exec(a, "INSERT INTO t VALUES(2,'committed-not-compacted'),(3,'x')"), SQLITE_OK);

    // ---- a raw copy of the main file is NOT the committed database (documented): it lacks the store's state
    CHECK(stock_scalar(path, "SELECT count(*) FROM t") == 1);

    // ---- sqlite3_backup from a lane: the pager reads through the VFS, so the backup is the logical committed database
    sqlite3 *dst;
    CHECK_RC(sqlite3_open_v2(out, &dst, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    sqlite3_backup *bk = sqlite3_backup_init(dst, "main", a, "main");
    CHECK(bk != NULL);
    if (bk) { CHECK_RC(sqlite3_backup_step(bk, -1), SQLITE_DONE); CHECK_RC(sqlite3_backup_finish(bk), SQLITE_OK); }
    CHECK(mw_scalar(dst, "SELECT count(*) FROM t") == 3);
    CHECK(mw_scalar(dst, "SELECT count(*) FROM t WHERE v='committed-not-compacted'") == 1);
    CHECK(integrity_ok(dst));
    sqlite3_close(dst);
    unlink(out);

    // ---- VACUUM INTO from a lane
    char vsql[300]; snprintf(vsql, sizeof vsql, "VACUUM INTO '%s'", out);
    CHECK_RC(mw_exec(a, vsql), SQLITE_OK);
    CHECK(stock_scalar(out, "SELECT count(*) FROM t") == 3);
    unlink(out);

    // ---- the documented safe copy procedure: compact, then copy the main file
    mw_compact_result cr;
    CHECK_RC(sqlite3_file_control(a, "main", MW_FCNTL_COMPACT, &cr), SQLITE_OK);
    CHECK(stock_scalar(path, "SELECT count(*) FROM t") == 3);

    // ---- journal modes: only WAL is supported; everything else is rejected, never silently run
    const char *bad[] = { "DELETE", "TRUNCATE", "PERSIST", "MEMORY", "OFF" };
    for (unsigned i = 0; i < 5; i++) {
        char sql[80]; snprintf(sql, sizeof sql, "PRAGMA journal_mode=%s", bad[i]);
        CHECK(mw_exec(a, sql) != SQLITE_OK);
    }
    CHECK_RC(mw_exec(a, "PRAGMA journal_mode=WAL"), SQLITE_OK);
    CHECK(strcmp(text(a, "PRAGMA journal_mode"), "wal") == 0);
    CHECK(mw_exec(a, "PRAGMA locking_mode=EXCLUSIVE") != SQLITE_OK);
    CHECK(mw_exec(a, "PRAGMA auto_vacuum=FULL") != SQLITE_OK);

    // ---- mmap is disabled: reads must go through the versioned store
    CHECK_RC(mw_exec(a, "PRAGMA mmap_size=268435456"), SQLITE_OK);
    CHECK(strcmp(text(a, "PRAGMA mmap_size"), "0") == 0);
    mw_vfs_events_reset();
    mw_scalar(a, "SELECT count(*) FROM t");
    CHECK(mw_vfs_event_count(MW_EV_FETCH) == 0);

    // ---- FTS5 and RTree: ordinary tables underneath (shadow tables), so they ride on page versioning
    CHECK_RC(mw_exec(a, "CREATE VIRTUAL TABLE docs USING fts5(title, body)"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "INSERT INTO docs VALUES('first','the quick brown fox'),('second','jumps over the lazy dog')"), SQLITE_OK);
    CHECK(mw_scalar(a, "SELECT count(*) FROM docs WHERE docs MATCH 'fox'") == 1);
    CHECK_RC(mw_exec(a, "CREATE VIRTUAL TABLE geo USING rtree(id, minx, maxx, miny, maxy)"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "INSERT INTO geo VALUES(1, 0,10, 0,10),(2, 5,15, 5,15),(3, 20,30, 20,30)"), SQLITE_OK);
    CHECK(mw_scalar(a, "SELECT count(*) FROM geo WHERE minx<=7 AND maxx>=7 AND miny<=7 AND maxy>=7") == 2);
    // concurrent writers to the same FTS5 index: conflicts are refused (retryable), never merged; retry succeeds
    sqlite3 *b; CHECK_RC(open_lane(path, &b, ""), SQLITE_OK);
    CHECK_RC(mw_exec(a, "BEGIN; INSERT INTO docs VALUES('a','alpha alpha')"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "BEGIN; INSERT INTO docs VALUES('b','beta beta')"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "COMMIT"), SQLITE_BUSY_SNAPSHOT);
    CHECK_RC(mw_exec(b, "INSERT INTO docs VALUES('b','beta beta')"), SQLITE_OK);
    CHECK(mw_scalar(a, "SELECT count(*) FROM docs WHERE docs MATCH 'alpha OR beta'") == 2);
    CHECK_RC(mw_exec(a, "INSERT INTO docs(docs) VALUES('integrity-check')"), SQLITE_OK);
    CHECK(integrity_ok(a));
    sqlite3_close(b);

    // ---- unsupported: shared cache, auto_vacuum databases, second Multi-Writer process (see multi-process phase)
    sqlite3 *x;
    char uri[400]; snprintf(uri, sizeof uri, "file:%s?mw=2&cache=shared", path);
    CHECK(sqlite3_open_v2(uri, &x, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL) != SQLITE_OK);
    sqlite3_close(x);
    sqlite3_close(a);
    {
        char av[256]; mw_tmpdb(av, sizeof av, "autovac");
        sqlite3 *y;
        sqlite3_open_v2(av, &y, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix");
        mw_exec(y, "PRAGMA auto_vacuum=FULL; CREATE TABLE t(x); PRAGMA journal_mode=WAL; INSERT INTO t VALUES(1)");
        sqlite3_close(y);
        CHECK(open_lane(av, &x, "") != SQLITE_OK);
        sqlite3_close(x);
        mw_rmdb(av);
    }
    // in-memory databases are simply not lane databases (untouched)
    CHECK_RC(sqlite3_open(":memory:", &x), SQLITE_OK);
    CHECK_RC(mw_exec(x, "CREATE TABLE m(a); INSERT INTO m VALUES(1)"), SQLITE_OK);
    sqlite3_close(x);

    mw_rmdb(path); unlink(lp);
    MW_DONE();
}
