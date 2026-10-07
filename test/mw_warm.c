// A snapshot that only read keeps the connection's page cache for the next snapshot if no commit happened between the two (the same epoch). Checks:
//  1. nothing stale: a connection that reads, while another commits, always sees the newest state;
//  2. the read set is inherited: a transaction that reads a page from the warm cache (no xRead reaches the engine) is still refused if that page changes before its commit;
//  3. the incremental backup (sqlite3_backup_step with a small step) terminates: the pager resets its cache, and the backup starts again, when the snapshot is not the same.
#include "mw_test.h"
#include "multiwriter.h"

static int open_lane (const char *path, sqlite3 **db) {
    char uri[300]; snprintf(uri, sizeof uri, "file:%s?mw=2&mw_gc=0", path);
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) sqlite3_extended_result_codes(*db, 1);
    return rc;
}
static void setup (char *path, const char *tag) {
    mw_tmpdb(path, 256, tag);
    sqlite3 *s;
    CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL;"
        "CREATE TABLE cells(id INTEGER PRIMARY KEY, v INTEGER, pad BLOB);"
        "INSERT INTO cells VALUES(1,1,zeroblob(3000)),(2,1,zeroblob(3000)),(3,100,zeroblob(3000)),(4,0,zeroblob(3000));"
        "CREATE TABLE pricelist(sku INTEGER PRIMARY KEY, price INTEGER); INSERT INTO pricelist VALUES(10,5);"), SQLITE_OK);
    CHECK_RC(sqlite3_close(s), SQLITE_OK);
}

int main (void) {
    char path[256]; sqlite3 *a, *b;

    // 1. never stale
    setup(path, "warm1");
    open_lane(path, &a); open_lane(path, &b);
    int64_t last = mw_scalar(a, "SELECT price FROM pricelist");
    for (int i = 0; i < 300; i++) {
        for (int k = 0; k < 3; k++) mw_scalar(a, "SELECT price FROM pricelist");       // (warm, once it has read for a while)
        char sql[100]; snprintf(sql, sizeof sql, "UPDATE pricelist SET price = %d", 1000 + i);
        CHECK_RC(mw_exec(b, sql), SQLITE_OK);
        int64_t p = mw_scalar(a, "SELECT price FROM pricelist"); if (p != 1000 + i) { CHECK(p == 1000 + i); break; }
        (void)last;
    }
    sqlite3_close(a); sqlite3_close(b); mw_rmfiles(path);

    // 2. the read set is inherited
    setup(path, "warm2");
    open_lane(path, &a); open_lane(path, &b);
    for (int i = 0; i < 70; i++) CHECK(mw_scalar(a, "SELECT price FROM pricelist WHERE sku=10") == 5);     // (a reader: after 64 read-only snapshots the cache stays)
    CHECK_RC(mw_exec(a, "BEGIN"), SQLITE_OK);
    CHECK(mw_scalar(a, "SELECT price FROM pricelist WHERE sku=10") == 5);                // served by the cache of the connection
    CHECK_RC(mw_exec(a, "UPDATE cells SET v=500 WHERE id=4"), SQLITE_OK);                // another page
    CHECK_RC(mw_exec(b, "UPDATE pricelist SET price=9 WHERE sku=10"), SQLITE_OK);        // a commit changes what a read
    CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_BUSY_SNAPSHOT);
    CHECK(mw_scalar(a, "SELECT v FROM cells WHERE id=4") == 0);
    sqlite3_close(a); sqlite3_close(b); mw_rmfiles(path);

    // 3. the incremental backup ends
    setup(path, "warm3");
    open_lane(path, &a);
    char dst[300]; mw_tmpdb(dst, sizeof dst, "warm3dst"); sqlite3 *d;
    CHECK_RC(sqlite3_open_v2(dst, &d, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    sqlite3_backup *bk = sqlite3_backup_init(d, "main", a, "main");
    int steps = 0, rc = SQLITE_OK;
    while (bk && steps < 400 && (rc = sqlite3_backup_step(bk, 1)) == SQLITE_OK) steps++;
    CHECK(rc == SQLITE_DONE);
    CHECK(steps < 200);                                  // (a connection that has written keeps its cache after 64 snapshots of reads: until then every step starts again)
    sqlite3_backup_finish(bk);
    CHECK(mw_scalar(d, "SELECT sum(v) FROM cells") == 102);
    CHECK(mw_scalar(d, "SELECT price FROM pricelist") == 5);
    sqlite3_close(d); sqlite3_close(a); mw_rmfiles(path); mw_rmfiles(dst);

    printf("test/mw_warm.c: %d failure(s)\n", mw_failures);
    return mw_failures ? 1 : 0;
}
