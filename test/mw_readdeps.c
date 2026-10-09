// Phase 12: read dependencies. Page-level read validation is a conservative first read-set:
// a transaction that only *read* a page which a newer commit changed is refused with a retryable
// SQLITE_BUSY_SNAPSHOT (never rebased: a replay of its writes would not redo its reads).
// The tests show cases that are safe and unsafe, and what happens with mw_readcheck=0 (plain
// snapshot isolation + CRDT merge: write skew and stale reads are then possible).
#include "mw_test.h"
#include "multiwriter.h"

static int close_cs (sqlite3 *db) { return sqlite3_close(db); }
static int open_lane (const char *path, sqlite3 **db, int readcheck) {
    char uri[300];
    snprintf(uri, sizeof uri, "file:%s?mw=1&mw_gc=0&mw_readcheck=%d", path, readcheck);
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) sqlite3_extended_result_codes(*db, 1);
    return rc;
}
static mw_db_stats stats (sqlite3 *db) { mw_db_stats s; memset(&s, 0, sizeof s); sqlite3_file_control(db, "main", MW_FCNTL_DBSTATS, &s); return s; }

static void setup (char *path, const char *tag) {
    mw_tmpdb(path, 256, tag);
    sqlite3 *s;
    CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
    // rows of `cells` are ~3000 bytes: one per leaf page, so "different rows" means "different pages"
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL;"
        "CREATE TABLE cells(id INTEGER PRIMARY KEY, v INTEGER, pad BLOB);"
        "INSERT INTO cells VALUES(1,1,zeroblob(3000)),(2,1,zeroblob(3000)),(3,100,zeroblob(3000)),(4,0,zeroblob(3000));"
        "CREATE TABLE items(id INTEGER PRIMARY KEY, n INTEGER); INSERT INTO items VALUES(1,1),(2,1),(3,1);"
        "CREATE TABLE summary(id INTEGER PRIMARY KEY, total INTEGER); INSERT INTO summary VALUES(1,0);"
        "CREATE TABLE pricelist(sku INTEGER PRIMARY KEY, price INTEGER); INSERT INTO pricelist VALUES(10,5);"
        "CREATE TABLE t(id TEXT PRIMARY KEY NOT NULL, note TEXT);"), SQLITE_OK);
    CHECK_RC(close_cs(s), SQLITE_OK);
}

int main (void) {
    char path[256];
    sqlite3 *a, *b;

    // ---- 1. stale read (price changed under a computation): refused, retry recomputes
    setup(path, "dep1");
    open_lane(path, &a, 1); open_lane(path, &b, 1);
    CHECK_RC(mw_exec(a, "BEGIN"), SQLITE_OK);
    int64_t price = mw_scalar(a, "SELECT price FROM pricelist WHERE sku=10");           // read dependency
    CHECK(price == 5);
    CHECK_RC(mw_exec(a, "UPDATE cells SET v=" "500" " WHERE id=4"), SQLITE_OK);           // written on another page
    CHECK_RC(mw_exec(b, "UPDATE pricelist SET price=9 WHERE sku=10"), SQLITE_OK);        // a newer commit changes what A read
    CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_BUSY_SNAPSHOT);                                // A's basis is stale
    CHECK(stats(a).read_conflicts == 1);
    CHECK(mw_scalar(a, "SELECT v FROM cells WHERE id=4") == 0);                          // nothing of A was published
    // the retry reads the new price and computes from it
    CHECK_RC(mw_exec(a, "BEGIN"), SQLITE_OK);
    price = mw_scalar(a, "SELECT price FROM pricelist WHERE sku=10");
    CHECK(price == 9);
    char sql[100]; snprintf(sql, sizeof sql, "UPDATE cells SET v=%lld WHERE id=4", (long long)(price * 10));
    CHECK_RC(mw_exec(a, sql), SQLITE_OK);
    CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_OK);
    CHECK(mw_scalar(b, "SELECT v FROM cells WHERE id=4") == 90);
    close_cs(a); close_cs(b); mw_rmdb(path);

    // ---- 1b. same interleaving with the read check off: the stale computation commits (documented SI behaviour)
    setup(path, "dep1b");
    open_lane(path, &a, 0); open_lane(path, &b, 0);
    mw_exec(a, "BEGIN"); mw_scalar(a, "SELECT price FROM pricelist WHERE sku=10");
    mw_exec(a, "UPDATE cells SET v=50 WHERE id=4");
    mw_exec(b, "UPDATE pricelist SET price=9 WHERE sku=10");
    CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_OK);
    CHECK(mw_scalar(a, "SELECT v FROM cells WHERE id=4") == 50);                         // based on price 5, though price is now 9
    close_cs(a); close_cs(b); mw_rmdb(path);

    // ---- 2. write skew: invariant v1 + v2 >= 1 ("someone stays on call"); each txn reads both, clears one
    setup(path, "skew");
    open_lane(path, &a, 1); open_lane(path, &b, 1);
    mw_exec(a, "BEGIN"); mw_exec(b, "BEGIN");
    CHECK(mw_scalar(a, "SELECT sum(v) FROM (SELECT v FROM cells WHERE id IN (1,2))") == 2);
    CHECK(mw_scalar(b, "SELECT sum(v) FROM (SELECT v FROM cells WHERE id IN (1,2))") == 2);
    mw_exec(a, "UPDATE cells SET v=0 WHERE id=1");
    mw_exec(b, "UPDATE cells SET v=0 WHERE id=2");
    CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "COMMIT"), SQLITE_BUSY_SNAPSHOT);                                // prevented
    CHECK(mw_scalar(a, "SELECT sum(v) FROM cells WHERE id IN (1,2)") == 1);
    close_cs(a); close_cs(b); mw_rmdb(path);
    setup(path, "skew0");
    open_lane(path, &a, 0); open_lane(path, &b, 0);
    mw_exec(a, "BEGIN"); mw_exec(b, "BEGIN");
    mw_scalar(a, "SELECT sum(v) FROM cells WHERE id IN (1,2)"); mw_scalar(b, "SELECT sum(v) FROM cells WHERE id IN (1,2)");
    mw_exec(a, "UPDATE cells SET v=0 WHERE id=1"); mw_exec(b, "UPDATE cells SET v=0 WHERE id=2");
    CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "COMMIT"), SQLITE_OK);
    CHECK(mw_scalar(a, "SELECT sum(v) FROM cells WHERE id IN (1,2)") == 0);              // write skew: invariant broken (snapshot isolation)
    close_cs(a); close_cs(b); mw_rmdb(path);

    // ---- 3. phantom: a scan is invalidated by a concurrent insert into the scanned range
    setup(path, "phantom");
    open_lane(path, &a, 1); open_lane(path, &b, 1);
    mw_exec(a, "BEGIN");
    int64_t n = mw_scalar(a, "SELECT count(*) FROM items");
    snprintf(sql, sizeof sql, "UPDATE summary SET total=%lld WHERE id=1", (long long)n);
    mw_exec(a, sql);
    mw_exec(b, "INSERT INTO items VALUES(4,1)");
    CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_BUSY_SNAPSHOT);
    mw_exec(a, "BEGIN"); n = mw_scalar(a, "SELECT count(*) FROM items"); CHECK(n == 4);
    snprintf(sql, sizeof sql, "UPDATE summary SET total=%lld WHERE id=1", (long long)n);
    mw_exec(a, sql);
    CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_OK);
    CHECK(mw_scalar(b, "SELECT total FROM summary WHERE id=1") == 4);
    close_cs(a); close_cs(b); mw_rmdb(path);

    // ---- 4. safe: transactions that read and write only their own pages both commit, no retry
    setup(path, "safe");
    open_lane(path, &a, 1); open_lane(path, &b, 1);
    mw_exec(a, "BEGIN"); mw_exec(b, "BEGIN");
    mw_scalar(a, "SELECT v FROM cells WHERE id=1"); mw_scalar(b, "SELECT v FROM cells WHERE id=4");
    mw_exec(a, "UPDATE cells SET v=v+1 WHERE id=1"); mw_exec(b, "UPDATE cells SET v=v+1 WHERE id=4");
    CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "COMMIT"), SQLITE_OK);
    CHECK(stats(a).read_conflicts == 0 && stats(a).page_conflicts == 0);
    close_cs(a); close_cs(b); mw_rmdb(path);

    // ---- 5. unsafe to rebase: same-page conflict on a tracked table (would normally be rebased), but the
    //         transaction also read a table that changed => refused instead of replaying stale logic
    setup(path, "unsafe");
    open_lane(path, &a, 1); open_lane(path, &b, 1);
    mw_exec(a, "BEGIN"); mw_scalar(a, "SELECT price FROM pricelist WHERE sku=10");
    mw_exec(a, "INSERT INTO t VALUES('a','decided on price 5')");
    mw_exec(b, "BEGIN; UPDATE pricelist SET price=9 WHERE sku=10; INSERT INTO t VALUES('b','b')");
    CHECK_RC(mw_exec(b, "COMMIT"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_BUSY_SNAPSHOT);
    CHECK(stats(a).rebases == 0);
    CHECK(mw_scalar(a, "SELECT count(*) FROM t WHERE id='a'") == 0);
    // ... and blind same-page writes (no other reads) are refused too, until the logical replay exists (docs/design.md): the application retries
    mw_exec(a, "BEGIN; INSERT INTO t VALUES('a','blind')");
    mw_exec(b, "BEGIN; INSERT INTO t VALUES('c','c')");
    CHECK_RC(mw_exec(b, "COMMIT"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_BUSY_SNAPSHOT);
    mw_exec(a, "ROLLBACK");
    CHECK_RC(mw_exec(a, "INSERT INTO t VALUES('a','blind')"), SQLITE_OK);
    close_cs(a); close_cs(b); mw_rmdb(path);

    MW_DONE();
}
