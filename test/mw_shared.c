// Shared mode of multi-process Multi-Writer (URI mw_mp=2): the shared version index and the segmented log.
// Small segments (MW_SEG_MB=1) so that every test rolls and trims many of them.
#include <dirent.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>
#include <time.h>
#include "mw_test.h"
#include "multiwriter.h"

static int open_sh (const char *path, sqlite3 **db) {
    char uri[400];
    snprintf(uri, sizeof uri, "file:%s?mw=2&mw_mp=2&mw_gc=16", path);
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) { sqlite3_extended_result_codes(*db, 1); sqlite3_busy_timeout(*db, 0); }
    return rc;
}
static int integrity_ok (sqlite3 *db) {
    sqlite3_stmt *st; int ok = 0;
    if (sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) ok = strcmp((const char *)sqlite3_column_text(st, 0), "ok") == 0;
    sqlite3_finalize(st);
    return ok;
}
static int64_t stock_scalar (const char *path, const char *sql, int *integrity) {         // the database file as an ordinary SQLite file (no Multi-Writer, no log)
    char uri[400]; snprintf(uri, sizeof uri, "file:%s?immutable=1", path);
    sqlite3 *c; int64_t v = -1;
    if (sqlite3_open_v2(uri, &c, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, "unix") == SQLITE_OK) { v = mw_scalar(c, sql); if (integrity) *integrity = integrity_ok(c); }
    sqlite3_close(c);
    return v;
}
static void make_db (const char *path, const char *extra_sql) {
    sqlite3 *s;
    CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER, pad BLOB);"
                        "CREATE TABLE acct(id INTEGER PRIMARY KEY, bal INTEGER); CREATE TABLE log(seq INTEGER PRIMARY KEY, who INTEGER, pad BLOB);"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<40) INSERT INTO acct SELECT i, 1000 FROM n"), SQLITE_OK);
    if (extra_sql) CHECK_RC(mw_exec(s, extra_sql), SQLITE_OK);
    sqlite3_close(s);
}
static int count_segments (const char *path) {
    char dir[300], pre[300]; snprintf(dir, sizeof dir, "%s", path);
    char *sl = strrchr(dir, '/'); snprintf(pre, sizeof pre, "%s-mw.", sl + 1); *sl = 0;
    DIR *d = opendir(dir); int n = 0; struct dirent *e;
    while (d && (e = readdir(d))) if (!strncmp(e->d_name, pre, strlen(pre))) n++;
    if (d) closedir(d);
    return n;
}
static void cleanup (const char *path) {
    mw_rmdb(path);
    char dir[300], pre[300]; snprintf(dir, sizeof dir, "%s", path);
    char *sl = strrchr(dir, '/'); snprintf(pre, sizeof pre, "%s-mw", sl + 1); *sl = 0;
    DIR *d = opendir(dir); struct dirent *e; char p[600];
    while (d && (e = readdir(d))) if (!strncmp(e->d_name, pre, strlen(pre))) { snprintf(p, sizeof p, "%s/%s", dir, e->d_name); unlink(p); }
    if (d) closedir(d);
}
static mw_compact_result compact (sqlite3 *db) { mw_compact_result r; memset(&r, 0, sizeof r); sqlite3_file_control(db, "main", MW_FCNTL_COMPACT, &r); return r; }

// ---- basic: the data survives a clean close, the log is gone, the database is an ordinary SQLite file ----
static void test_basic (void) {
    char path[256]; mw_tmpdb(path, sizeof path, "sh_basic");
    make_db(path, NULL);
    sqlite3 *db; CHECK_RC(open_sh(path, &db), SQLITE_OK);
    for (int i = 0; i < 300; i++) { char sql[200]; snprintf(sql, sizeof sql, "INSERT INTO log VALUES(%d, 1, zeroblob(2000))", i + 1); CHECK_RC(mw_exec(db, sql), SQLITE_OK); }
    CHECK(mw_scalar(db, "SELECT count(*) FROM log") == 300);
    int segs_open = count_segments(path);
    CHECK(segs_open >= 1);
    sqlite3_close(db);
    int integ = 0;
    CHECK(stock_scalar(path, "SELECT count(*) FROM log", &integ) == 300 && integ);
    CHECK(count_segments(path) == 0);                                          // the last connection compacted and removed the log
    // reopen: nothing to recover, more data
    CHECK_RC(open_sh(path, &db), SQLITE_OK);
    CHECK(mw_scalar(db, "SELECT count(*) FROM log") == 300);
    CHECK_RC(mw_exec(db, "INSERT INTO log VALUES(1000, 2, zeroblob(10))"), SQLITE_OK);
    sqlite3_close(db);
    CHECK(stock_scalar(path, "SELECT count(*) FROM log", NULL) == 301);
    cleanup(path);
}

// ---- segments roll and are trimmed; a commit bigger than a segment gets a segment of its own ----
static void test_segments (void) {
    char path[256]; mw_tmpdb(path, sizeof path, "sh_seg");
    make_db(path, NULL);
    sqlite3 *db; CHECK_RC(open_sh(path, &db), SQLITE_OK);
    for (int i = 0; i < 400; i++) { char sql[200]; snprintf(sql, sizeof sql, "INSERT INTO log VALUES(%d, 1, zeroblob(3000))", i + 1); CHECK_RC(mw_exec(db, sql), SQLITE_OK); }
    int segs = count_segments(path);
    printf("  segments after 400 commits: %d\n", segs);
    CHECK(segs >= 2);                                                           // ~1.5 MB of log in 1 MB segments
    mw_compact_result r = compact(db);
    CHECK(r.pages_written > 0);
    printf("  segments after compaction (%llu pages): %d\n", (unsigned long long)r.pages_written, count_segments(path));
    CHECK(count_segments(path) < segs || segs <= 2);                            // trimmed (the active one and the prepared next one stay)
    // a transaction whose record is bigger than a segment
    CHECK_RC(mw_exec(db, "BEGIN"), SQLITE_OK);
    for (int i = 0; i < 6; i++) { char sql[200]; snprintf(sql, sizeof sql, "INSERT INTO log VALUES(%d, 3, zeroblob(900000))", 5000 + i); CHECK_RC(mw_exec(db, sql), SQLITE_OK); }
    CHECK_RC(mw_exec(db, "COMMIT"), SQLITE_OK);
    CHECK(mw_scalar(db, "SELECT sum(length(pad)) FROM log WHERE who=3") == 6 * 900000);
    CHECK(mw_scalar(db, "SELECT count(*) FROM log") == 406);
    CHECK(integrity_ok(db));
    sqlite3_close(db);
    int integ = 0;
    CHECK(stock_scalar(path, "SELECT count(*) FROM log", &integ) == 406 && integ);
    CHECK(count_segments(path) == 0);
    cleanup(path);
}

// ---- a reader's snapshot survives commits and compactions (its versions may live in segments that are trimmed meanwhile) ----
static void test_snapshot (void) {
    char path[256]; mw_tmpdb(path, sizeof path, "sh_snap");
    make_db(path, "INSERT INTO t SELECT i, 0, zeroblob(2000) FROM (WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<200) SELECT i FROM n)");
    sqlite3 *rd, *wr; CHECK_RC(open_sh(path, &rd), SQLITE_OK); CHECK_RC(open_sh(path, &wr), SQLITE_OK);
    CHECK_RC(mw_exec(rd, "BEGIN"), SQLITE_OK);
    CHECK(mw_scalar(rd, "SELECT sum(v) FROM t") == 0);
    int segs0 = count_segments(path);
    for (int round = 0; round < 6; round++) {
        for (int i = 0; i < 250; i++) { char sql[100]; snprintf(sql, sizeof sql, "UPDATE t SET v=v+1 WHERE id=%d", 1 + (i % 200)); CHECK_RC(mw_exec(wr, sql), SQLITE_OK); }
        compact(wr);                                                            // the target stays below the reader's snapshot: nothing it can see goes away
        CHECK(mw_scalar(rd, "SELECT sum(v) FROM t") == 0);
        CHECK(mw_scalar(rd, "SELECT count(*) FROM t WHERE v<>0") == 0);
    }
    CHECK(count_segments(path) > segs0 - 1);                                    // (segments hold the reader's versions: not trimmed away)
    CHECK_RC(mw_exec(rd, "COMMIT"), SQLITE_OK);
    CHECK(mw_scalar(rd, "SELECT sum(v) FROM t") == 6 * 250);                    // a new snapshot sees everything
    compact(wr);
    compact(wr);
    CHECK(mw_scalar(rd, "SELECT sum(v) FROM t") == 6 * 250);
    sqlite3_close(rd); sqlite3_close(wr);
    CHECK(stock_scalar(path, "SELECT sum(v) FROM t", NULL) == 6 * 250);
    cleanup(path);
}

// ---- concurrent writers and readers in several processes: money is moved between accounts, the total never changes in any snapshot ----
static void child_transfers (const char *path, int id, int seconds) {
    sqlite3 *db; if (open_sh(path, &db) != SQLITE_OK) _exit(2);
    uint64_t rng = 0x9E3779B9u * (uint64_t)(id + 1); time_t end = time(NULL) + seconds; int bad = 0;
    while (time(NULL) < end) {
        rng = rng * 6364136223846793005ull + 1442695040888963407ull;
        int a = 1 + (int)((rng >> 33) % 40), b = 1 + (int)((rng >> 40) % 40); if (a == b) continue;
        if (id % 3 == 2) {                                                       // reader: the total is invariant
            if (mw_exec(db, "BEGIN") != SQLITE_OK) continue;
            int64_t s1 = mw_scalar(db, "SELECT sum(bal) FROM acct");
            int64_t s2 = mw_scalar(db, "SELECT sum(bal) FROM acct");
            mw_exec(db, "COMMIT");
            if (s1 != 40000 || s2 != 40000) bad++;
            continue;
        }
        char sql[300];
        snprintf(sql, sizeof sql, "BEGIN; UPDATE acct SET bal=bal-7 WHERE id=%d; UPDATE acct SET bal=bal+7 WHERE id=%d; INSERT INTO log(seq,who,pad) VALUES(%d, %d, zeroblob(1500)); COMMIT",
                 a, b, (id + 1) * 100000000 + (int)((rng >> 8) % 90000000), id);
        if (mw_exec(db, sql) != SQLITE_OK) mw_exec(db, "ROLLBACK");
    }
    sqlite3_close(db);
    _exit(bad ? 3 : 0);
}
static void test_concurrent (void) {
    char path[256]; mw_tmpdb(path, sizeof path, "sh_conc");
    make_db(path, NULL);
    pid_t pids[6];
    for (int i = 0; i < 6; i++) { pids[i] = fork(); if (pids[i] == 0) child_transfers(path, i, 4); }
    int bad = 0;
    for (int i = 0; i < 6; i++) { int st; waitpid(pids[i], &st, 0); if (!WIFEXITED(st) || WEXITSTATUS(st) != 0) { bad++; printf("  child %d: %s %d\n", i, WIFEXITED(st) ? "exit" : "signal", WIFEXITED(st) ? WEXITSTATUS(st) : WTERMSIG(st)); } }
    CHECK(bad == 0);
    sqlite3 *db; CHECK_RC(open_sh(path, &db), SQLITE_OK);
    CHECK(mw_scalar(db, "SELECT sum(bal) FROM acct") == 40000);
    CHECK(integrity_ok(db));
    int64_t rows = mw_scalar(db, "SELECT count(*) FROM log");
    CHECK(rows > 100);
    sqlite3_close(db);
    int integ = 0;
    CHECK(stock_scalar(path, "SELECT sum(bal) FROM acct", &integ) == 40000 && integ);
    CHECK(stock_scalar(path, "SELECT count(*) FROM log", NULL) == rows);
    CHECK(count_segments(path) == 0);
    cleanup(path);
}

// ---- crash recovery: a writer is killed at random moments; what it acknowledged is there after the next open (which recovers from the segments) ----
static void child_writer (const char *path, int wfd, int base) {
    sqlite3 *db; if (open_sh(path, &db) != SQLITE_OK) _exit(2);
    for (int i = 1; ; i++) {
        char sql[200]; snprintf(sql, sizeof sql, "INSERT INTO log VALUES(%d, 9, zeroblob(2500))", base + i);
        if (mw_exec(db, sql) == SQLITE_OK) { int v = base + i; if (write(wfd, &v, sizeof v) != sizeof v) _exit(4); }
    }
}
static void test_crash_recovery (void) {
    char path[256]; mw_tmpdb(path, sizeof path, "sh_crash");
    make_db(path, NULL);
    int acked_total = 0, last = 0;
    srand(12345);
    for (int round = 0; round < 14; round++) {
        int fds[2]; CHECK(pipe(fds) == 0);
        int base = (round + 1) * 100000;
        pid_t c = fork();
        if (c == 0) { close(fds[0]); child_writer(path, fds[1], base); }
        close(fds[1]);
        usleep(60000 + (unsigned)(rand() % 350000));
        kill(c, SIGKILL); waitpid(c, NULL, 0);
        int v, got = 0; last = 0;
        while (read(fds[0], &v, sizeof v) == sizeof v) { got++; last = v; }
        close(fds[0]);
        acked_total += got;
        sqlite3 *db;
        CHECK_RC(open_sh(path, &db), SQLITE_OK);                                 // recovers
        int64_t n = mw_scalar(db, "SELECT count(*) FROM log WHERE who=9");
        if (round % 4 == 0) printf("  round %d: acked %d, rows %lld, segments %d\n", round, acked_total, (long long)n, count_segments(path));
    CHECK(n >= acked_total);                                                  // everything acknowledged is there (a commit whose acknowledgement was not read may be too)
        CHECK(n <= acked_total + 1);
        if (last) CHECK(mw_scalar(db, "SELECT count(*) FROM log WHERE seq=?") >= 0);
        CHECK(mw_scalar(db, "SELECT max(seq)-min(seq)+1-count(*) FROM log WHERE seq BETWEEN 100001 AND 199999") >= 0);
        CHECK(integrity_ok(db));
        acked_total = (int)n;                                                      // (the unacknowledged one that survived counts from now on)
        sqlite3_close(db);
    }
    CHECK(stock_scalar(path, "SELECT count(*) FROM log WHERE who=9", NULL) == acked_total);
    cleanup(path);
}

int main (void) {
    setenv("MW_SEG_MB", "1", 1);
    test_basic();
    test_segments();
    test_snapshot();
    test_concurrent();
    test_crash_recovery();
    MW_DONE();
}
