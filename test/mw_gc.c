// Phase 7: page-version garbage collection against pinned snapshots.
#include <pthread.h>
#include "mw_test.h"
#include "multiwriter.h"

static int open_lane (const char *path, sqlite3 **db, const char *extra) {
    char uri[300];
    snprintf(uri, sizeof uri, "file:%s?mw=1%s", path, extra);
    return sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
}
static mw_tx_info txinfo (sqlite3 *db) { mw_tx_info i; memset(&i, 0, sizeof i); sqlite3_file_control(db, "main", MW_FCNTL_TXINFO, &i); return i; }
static mw_db_stats stats (sqlite3 *db) { mw_db_stats s; memset(&s, 0, sizeof s); sqlite3_file_control(db, "main", MW_FCNTL_DBSTATS, &s); return s; }
static uint64_t gc (sqlite3 *db) { uint64_t n = 0; sqlite3_file_control(db, "main", MW_FCNTL_GC, &n); return n; }

// ---- part 2: readers must always see a consistent snapshot while a writer commits and GC runs
static volatile int stop_flag;
static const char *g_path;
static int reader_errors;
static void *reader (void *arg) {
    sqlite3 *db;
    if (open_lane(g_path, &db, "&mw_gc=0") != SQLITE_OK) { reader_errors++; return NULL; }
    long n = 0;
    while (!stop_flag) {
        // the writer moves value between two rows inside one transaction: the sum is invariant
        mw_exec(db, "BEGIN");
        int64_t s1 = mw_scalar(db, "SELECT sum(bal) FROM acct");
        for (volatile int i = 0; i < 2000; i++) ;
        int64_t s2 = mw_scalar(db, "SELECT sum(bal) FROM acct");
        mw_exec(db, "COMMIT");
        if (s1 != 1000 || s2 != 1000) reader_errors++;
        n++;
    }
    sqlite3_close(db);
    return (void *)(intptr_t)n;
}

int main (void) {
    char path[256];
    mw_tmpdb(path, sizeof path, "gc");
    g_path = path;
    sqlite3 *s;
    CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);"
                        "INSERT INTO t VALUES(1,0),(2,0);"
                        "CREATE TABLE acct(id INTEGER PRIMARY KEY, bal INTEGER); INSERT INTO acct VALUES(1,500),(2,500)"), SQLITE_OK);
    sqlite3_close(s);

    sqlite3 *w, *r;
    CHECK_RC(open_lane(path, &w, "&mw_gc=0"), SQLITE_OK);      // manual GC only
    CHECK_RC(open_lane(path, &r, "&mw_gc=0"), SQLITE_OK);

    // reader R pins a snapshot at v=0
    CHECK_RC(mw_exec(r, "BEGIN"), SQLITE_OK);
    CHECK(mw_scalar(r, "SELECT v FROM t WHERE id=1") == 0);
    uint64_t pinned = txinfo(r).snapshot_epoch;

    // writers advance the database by 1000 commits touching the same page
    for (int i = 1; i <= 1000; i++) {
        char sql[80];
        snprintf(sql, sizeof sql, "UPDATE t SET v=%d WHERE id=1", i);
        CHECK_RC(mw_exec(w, sql), SQLITE_OK);
    }
    mw_db_stats st = stats(w);
    CHECK(st.epoch == pinned + 1000);
    CHECK(st.oldest_active_snapshot == pinned);
    CHECK(st.page_versions >= 1000);
    printf("before gc: versions=%llu retained=%llu KB\n", (unsigned long long)st.page_versions, (unsigned long long)st.bytes_retained / 1024);

    // GC while R is pinned: nothing newer than R's snapshot may go, R still reads its snapshot
    uint64_t rec = gc(w);
    st = stats(w);
    CHECK(st.page_versions >= 1000);                       // all versions are newer than the pinned snapshot
    CHECK(mw_scalar(r, "SELECT v FROM t WHERE id=1") == 0);
    CHECK(txinfo(r).snapshot_epoch == pinned);
    CHECK(rec == 0);

    // a second reader pins the middle: after R releases, GC may reclaim up to (not including) its horizon
    sqlite3 *m;
    CHECK_RC(open_lane(path, &m, "&mw_gc=0"), SQLITE_OK);
    CHECK_RC(mw_exec(m, "BEGIN"), SQLITE_OK);
    CHECK(mw_scalar(m, "SELECT v FROM t WHERE id=1") == 1000);
    uint64_t mid = txinfo(m).snapshot_epoch;
    for (int i = 1001; i <= 1100; i++) {
        char sql[80];
        snprintf(sql, sizeof sql, "UPDATE t SET v=%d WHERE id=1", i);
        CHECK_RC(mw_exec(w, sql), SQLITE_OK);
    }
    CHECK_RC(mw_exec(r, "COMMIT"), SQLITE_OK);              // R releases its old snapshot
    CHECK(stats(w).oldest_active_snapshot == mid);
    rec = gc(w);
    st = stats(w);
    CHECK(rec >= 990);                                     // ~1000 obsolete versions of the hot page
    CHECK(st.page_versions <= 110);                        // what remains: M's version + the 100 newer ones (+ a few)
    CHECK(mw_scalar(m, "SELECT v FROM t WHERE id=1") == 1000);   // M still correct after GC
    CHECK_RC(mw_exec(m, "COMMIT"), SQLITE_OK);
    rec = gc(w);
    st = stats(w);
    CHECK(st.page_versions <= 3);                          // fully collapsed: one version per touched page
    CHECK(st.versions_reclaimed >= 1090);
    CHECK(mw_scalar(w, "SELECT v FROM t WHERE id=1") == 1100);
    printf("after gc: versions=%llu retained=%llu KB reclaimed=%llu gc_runs=%llu gc_ns=%llu\n", (unsigned long long)st.page_versions,
           (unsigned long long)st.bytes_retained / 1024, (unsigned long long)st.versions_reclaimed, (unsigned long long)st.gc_runs, (unsigned long long)st.gc_ns);
    sqlite3_close(m); sqlite3_close(r);

    // automatic GC: a fresh database handle with mw_gc=8 keeps the store bounded under writes
    for (int i = 0; i < 400; i++) {
        char sql[80];
        snprintf(sql, sizeof sql, "UPDATE t SET v=%d WHERE id=2", i);
        CHECK_RC(mw_exec(w, sql), SQLITE_OK);
    }
    CHECK(stats(w).page_versions > 300);                   // manual mode: it grows
    sqlite3_close(w);

    // ---- concurrent readers + a writer + GC every 4 commits
    CHECK_RC(open_lane(path, &w, "&mw_gc=4"), SQLITE_OK);
    stop_flag = 0;
    pthread_t th[4];
    for (int i = 0; i < 4; i++) pthread_create(&th[i], NULL, reader, NULL);
    for (int i = 0; i < 1500; i++) {
        char sql[200];
        int amt = (i % 7) - 3;
        snprintf(sql, sizeof sql, "BEGIN; UPDATE acct SET bal=bal-(%d) WHERE id=1; UPDATE acct SET bal=bal+(%d) WHERE id=2; COMMIT", amt, amt);
        CHECK_RC(mw_exec(w, sql), SQLITE_OK);
    }
    stop_flag = 1;
    long reads = 0;
    for (int i = 0; i < 4; i++) { void *ret; pthread_join(th[i], &ret); reads += (long)(intptr_t)ret; }
    printf("reader transactions: %ld, reader errors: %d\n", reads, reader_errors);
    CHECK(reader_errors == 0);
    CHECK(reads > 0);
    sqlite3_close(w);
    mw_rmdb(path);
    MW_DONE();
}
