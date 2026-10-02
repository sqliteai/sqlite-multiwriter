// A real -wal left by stock use must be checkpointed before the lane store is built; and a large transaction must build its write set in linear time.
#include <time.h>
#include "mw_test.h"
#include "multiwriter.h"

static int open_lane (const char *path, sqlite3 **db) {
    char uri[300];
    snprintf(uri, sizeof uri, "file:%s?mw=2", path);
    return sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
}
static void copy_file (const char *a, const char *b) {
    FILE *i = fopen(a, "rb"), *o = fopen(b, "wb");
    if (!i || !o) return;
    char buf[65536]; size_t n;
    while ((n = fread(buf, 1, sizeof buf, i)) > 0) fwrite(buf, 1, n, o);
    fclose(i); fclose(o);
}

int main (void) {
    char src[256], dst[256], p1[300], p2[300];
    mw_tmpdb(src, sizeof src, "swsrc"); mw_tmpdb(dst, sizeof dst, "swdst");
    sqlite3 *s;
    CHECK_RC(sqlite3_open_v2(src, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER); INSERT INTO t VALUES(1,10),(2,20),(3,30)"), SQLITE_OK);
    // "crash": copy the files while the stock connection is open, so the copy has a -wal with committed frames the main file lacks
    snprintf(p1, sizeof p1, "%s-wal", src); snprintf(p2, sizeof p2, "%s-wal", dst);
    copy_file(src, dst); copy_file(p1, p2);
    sqlite3_close(s);

    sqlite3 *a;
    CHECK_RC(open_lane(dst, &a), SQLITE_OK);
    CHECK(mw_scalar(a, "SELECT sum(v) FROM t") == 60);                  // the committed rows in the stale WAL are visible
    CHECK_RC(mw_exec(a, "UPDATE t SET v=v+1 WHERE id=1"), SQLITE_OK);
    sqlite3_close(a);
    CHECK_RC(sqlite3_open_v2(dst, &s, SQLITE_OPEN_READWRITE, "unix"), SQLITE_OK);   // and a stock reader afterwards sees a consistent file
    CHECK(mw_scalar(s, "SELECT sum(v) FROM t") == 61);
    CHECK_RC(mw_exec(s, "PRAGMA integrity_check"), SQLITE_OK);
    sqlite3_close(s);
    mw_rmdb(src); mw_rmdb(dst);

    // one big transaction: 50k rows of 4000-byte blobs (~50k dirty pages)
    char big[256]; mw_tmpdb(big, sizeof big, "swbig");
    CHECK_RC(sqlite3_open_v2(big, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE b(id INTEGER PRIMARY KEY, d BLOB)"), SQLITE_OK);
    sqlite3_close(s);
    CHECK_RC(open_lane(big, &a), SQLITE_OK);
    struct timespec t0, t1; clock_gettime(CLOCK_MONOTONIC, &t0);
    CHECK_RC(mw_exec(a, "BEGIN; WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<50000) INSERT INTO b SELECT i, zeroblob(4000) FROM n; COMMIT"), SQLITE_OK);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double ms = (double)(t1.tv_sec - t0.tv_sec) * 1e3 + (double)(t1.tv_nsec - t0.tv_nsec) / 1e6;
    printf("50000-row transaction (200 MB): %.0f ms\n", ms);
    CHECK(mw_scalar(a, "SELECT count(*) FROM b") == 50000);
    sqlite3_close(a);
    mw_rmdb(big);
    MW_DONE();
}
