// mw_profile=small: the caches are small, the database is still right (tracked, with a flush and merges forced by tiny limits).
#include "mw_test.h"
#include "multiwriter.h"

static int open_lane (const char *path, sqlite3 **db, const char *extra) {
    char uri[500]; snprintf(uri, sizeof uri, "file:%s?mw=1%s", path, extra);
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) { sqlite3_extended_result_codes(*db, 1); sqlite3_busy_timeout(*db, 5000); }
    return rc;
}
int main (void) {
    char path[300]; mw_tmpdb(path, sizeof path, "profile");
    { sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
      CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)"), SQLITE_OK); sqlite3_close(s); }
    sqlite3 *db; CHECK_RC(open_lane(path, &db, "&mw_profile=small&mw_log_max_mb=1"), SQLITE_OK);
    for (int t = 0; t < 300; t++) {
        CHECK_RC(mw_exec(db, "BEGIN"), SQLITE_OK);
        for (int i = 0; i < 100; i++) { char q[160]; snprintf(q, sizeof q, "INSERT INTO t VALUES(%d, %d, 'payload %d')", t * 100 + i, i, t); CHECK_RC(mw_exec(db, q), SQLITE_OK); }
        CHECK_RC(mw_exec(db, "COMMIT"), SQLITE_OK);
        if (t % 5 == 0) CHECK_RC(mw_exec(db, "UPDATE t SET a = a + 1 WHERE id % 7 = 0"), SQLITE_OK);
    }
    CHECK(mw_scalar(db, "SELECT count(*) FROM t") == 30000);
    sqlite3_close(db);
    { sqlite3 *v; CHECK_RC(open_lane(path, &v, "&mw_profile=small"), SQLITE_OK);        // reopened: the state is the same
      CHECK(mw_scalar(v, "SELECT count(*) FROM t") == 30000);
      CHECK(mw_scalar(v, "SELECT count(*) FROM t WHERE b IS NULL") == 0);
      sqlite3_close(v); }
    mw_rmdb(path);
    MW_DONE();
}
