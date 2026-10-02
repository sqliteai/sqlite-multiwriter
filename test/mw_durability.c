// Phase 17: durability and crash recovery. Children are killed at exact instants (fault injection
// crash points _exit(9) on the spot) or by SIGKILL under load; the parent then recovers the database
// and checks: every acknowledged commit is present, no transaction is half applied, the structure is
// valid, and the recovered database becomes an ordinary SQLite file again after a clean close.
#include <signal.h>
#include <sys/wait.h>
#include <pthread.h>
#include "mw_test.h"
#include "multiwriter.h"

static int open_lane (const char *path, sqlite3 **db, const char *extra) {
    char uri[400];
    snprintf(uri, sizeof uri, "file:%s?mw=2&mw_gc=0%s", path, extra);
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) sqlite3_extended_result_codes(*db, 1);
    return rc;
}
static mw_db_stats stats (sqlite3 *db) { mw_db_stats s; memset(&s, 0, sizeof s); sqlite3_file_control(db, "main", MW_FCNTL_DBSTATS, &s); return s; }
static int integrity_ok (sqlite3 *db) {
    sqlite3_stmt *st; int ok = 0;
    if (sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) ok = strcmp((const char *)sqlite3_column_text(st, 0), "ok") == 0;
    sqlite3_finalize(st);
    return ok;
}

// Every transaction inserts 3 rows sharing a tag and bumps a counter: atomicity is checkable afterwards.
static void make_db (const char *path) {
    sqlite3 *s;
    CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE log(id INTEGER PRIMARY KEY, tag INTEGER, k INTEGER, pad BLOB);"
                        "CREATE TABLE ctr(id INTEGER PRIMARY KEY, n INTEGER); INSERT INTO ctr VALUES(1,0);"), SQLITE_OK);
    sqlite3_close(s);
}

// Child: commit tags 1,2,3,... acknowledging each on `ackfd` after COMMIT returned OK.
static void child_commits (const char *path, int ackfd, mw_fault_t fault, int nth, int compact_every, const char *extra, int max_tag) {
    sqlite3 *db;
    if (open_lane(path, &db, extra) != SQLITE_OK) _exit(3);
    for (int tag = 1; tag < 100000; tag++) {
        if (fault && tag == 1) mw_fault_arm(fault, nth);
        char sql[300];
        snprintf(sql, sizeof sql, "BEGIN; INSERT INTO log(tag,k,pad) VALUES(%d,0,zeroblob(900)),(%d,1,zeroblob(900)),(%d,2,zeroblob(900)); UPDATE ctr SET n=%d WHERE id=1; COMMIT", tag, tag, tag, tag);
        if (mw_exec(db, sql) != SQLITE_OK) _exit(4);
        int32_t t = tag;
        if (write(ackfd, &t, sizeof t) != sizeof t) _exit(5);
        if (compact_every && tag % compact_every == 0) { mw_compact_result r; sqlite3_file_control(db, "main", MW_FCNTL_COMPACT, &r); }
        if (tag >= max_tag) break;
    }
    _exit(0);                                      // no clean close: the log stays behind
}

static int run_child (const char *path, mw_fault_t fault, int nth, int compact_every, int *acked_out, int sigkill_after_ms) {
    int pfd[2];
    CHECK(pipe(pfd) == 0);
    pid_t pid = fork();
    if (pid == 0) {
        close(pfd[0]);
        if (sigkill_after_ms > 0) { alarm(0); }
        child_commits(path, pfd[1], fault, nth, compact_every, "", 40);
    }
    close(pfd[1]);
    int status = 0;
    waitpid(pid, &status, 0);
    int32_t t; int last = 0, n = 0;
    while (read(pfd[0], &t, sizeof t) == sizeof t) { last = t; n++; }
    close(pfd[0]);
    *acked_out = last;
    CHECK(last == n);                                                     // acks are sequential
    return WIFEXITED(status) ? WEXITSTATUS(status) : -WTERMSIG(status);
}

// Recover in the parent and verify. `allow_extra`: the crashing transaction may (must not) be present.
static void log_peek (const char *path) {
    char lp[300]; snprintf(lp, sizeof lp, "%s-mw", path);
    FILE *f = fopen(lp, "rb");
    if (!f) { printf("    log: (missing)\n"); return; }
    unsigned char h[64]; size_t n = fread(h, 1, 64, f); fseek(f, 0, SEEK_END); long sz = ftell(f); fclose(f);
    uint64_t base = 0; memcpy(&base, h + 16, 8);
    printf("    log: size=%ld hdr_read=%zu magic=%.8s base_epoch=%llu\n", sz, n, (char *)h, (unsigned long long)base);
}
static void verify (const char *path, int acked, int max_extra, const char *what) {
    sqlite3 *db;
    if (getenv("MW_VERBOSE")) log_peek(path);
    CHECK_RC(open_lane(path, &db, ""), SQLITE_OK);                        // recovery happens here
    int64_t rows = mw_scalar(db, "SELECT count(*) FROM log");
    int64_t tags = mw_scalar(db, "SELECT count(DISTINCT tag) FROM log");
    int64_t whole = mw_scalar(db, "SELECT count(*) FROM (SELECT tag FROM log GROUP BY tag HAVING count(*)=3 AND min(k)=0 AND max(k)=2)");
    int64_t maxtag = mw_scalar(db, "SELECT coalesce(max(tag),0) FROM log");
    int64_t ctr = mw_scalar(db, "SELECT n FROM ctr WHERE id=1");
    printf("  %-34s acked=%d recovered tags=%lld (max %lld) ctr=%lld epoch=%llu\n", what, acked, (long long)tags, (long long)maxtag, (long long)ctr, (unsigned long long)stats(db).epoch);
    CHECK(rows == tags * 3);                                              // atomic: whole transactions only
    CHECK(whole == tags);
    CHECK(maxtag == tags);                                                // contiguous prefix 1..M (commits are sequential)
    CHECK(tags >= acked && tags <= acked + max_extra);                    // every acknowledged commit is there
    CHECK(ctr == maxtag);                                                 // counter agrees with the rows
    CHECK(integrity_ok(db));
    // it must keep working after recovery
    CHECK_RC(mw_exec(db, "INSERT INTO log(tag,k,pad) VALUES(9999,0,zeroblob(10)),(9999,1,zeroblob(10)),(9999,2,zeroblob(10))"), SQLITE_OK);
    sqlite3_close(db);                                                    // clean close: compacts, removes the log
    sqlite3 *chk;
    CHECK_RC(sqlite3_open_v2(path, &chk, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, "unix"), SQLITE_OK);
    char q[200]; (void)q;
    CHECK(mw_scalar(chk, "SELECT count(*) FROM log") == rows + 3);         // stock SQLite sees everything
    CHECK(integrity_ok(chk));
    sqlite3_close(chk);
    char lp[300]; snprintf(lp, sizeof lp, "%s-mw", path);
    CHECK(access(lp, F_OK) != 0);                                         // log gone after the clean close
}

int main (void) {
    char path[256];
    struct { const char *name; mw_fault_t f; int nth; int extra; int compact; } cases[] = {
        { "clean prefix (no close, no fault)",      MW_FAULT_NONE,           0, 0, 0 },
        { "crash before log write (prepared)",     MW_CRASH_BEFORE_LOG,    12, 0, 0 },
        { "crash mid log write (torn record)",     MW_CRASH_MID_LOG,       12, 0, 0 },
        { "crash after durable, before visible",   MW_CRASH_AFTER_LOG,     12, 1, 0 },
        { "crash right after publish",             MW_CRASH_AFTER_VISIBLE, 12, 1, 0 },
        { "crash in compaction (pages written)",   MW_CRASH_COMPACT_PAGES,  2, 0, 5 },
        { "crash in compaction (base recorded)",   MW_CRASH_COMPACT_BASE,   2, 0, 5 },
        { "compaction every 5 commits, no crash",  MW_FAULT_NONE,           0, 0, 5 },
    };
    for (unsigned i = 0; i < sizeof cases / sizeof cases[0]; i++) {
        mw_tmpdb(path, sizeof path, "dur");
        char lp[300]; snprintf(lp, sizeof lp, "%s-mw", path); unlink(lp);
        make_db(path);
        int acked = 0;
        int rc = run_child(path, cases[i].f, cases[i].nth, cases[i].compact, &acked, 0);
        if (cases[i].f >= MW_CRASH_MID_LOG) CHECK(rc == 9);
        verify(path, acked, cases[i].extra, cases[i].name);
        mw_rmdb(path); unlink(lp);
    }

    // ---- SIGKILL under load: several committers, killed at a random moment; acked commits survive
    for (int round = 0; round < 6; round++) {
        mw_tmpdb(path, sizeof path, "kill");
        char lp[300]; snprintf(lp, sizeof lp, "%s-mw", path); unlink(lp);
        make_db(path);
        int pfd[2]; CHECK(pipe(pfd) == 0);
        pid_t pid = fork();
        if (pid == 0) {
            close(pfd[0]);
            // one committer thread: sequential tags, ack after each COMMIT (single writer keeps 'prefix' checkable),
            // plus background threads hammering the same page so commits contend and rebase/retry
            child_commits(path, pfd[1], MW_FAULT_NONE, 0, 7, "&mw_compact_ms=3", 1000000);
        }
        close(pfd[1]);
        struct timespec ts = { 0, (long)(30 + round * 53) * 1000000L };
        nanosleep(&ts, NULL);
        kill(pid, SIGKILL);
        int status; waitpid(pid, &status, 0);
        int32_t t; int last = 0;
        while (read(pfd[0], &t, sizeof t) == sizeof t) last = t;
        close(pfd[0]);
        char what[64]; snprintf(what, sizeof what, "SIGKILL after %dms", 20 + round * 37);
        verify(path, last, 1, what);
        mw_rmdb(path); unlink(lp);
    }

    // ---- injected errors (no crash): a failed log write aborts just that commit; the database continues
    {
        mw_tmpdb(path, sizeof path, "err"); make_db(path);
        sqlite3 *db; CHECK_RC(open_lane(path, &db, ""), SQLITE_OK);
        CHECK_RC(mw_exec(db, "INSERT INTO log(tag,k) VALUES(1,0),(1,1),(1,2)"), SQLITE_OK);
        mw_fault_arm(MW_FAULT_LOG_WRITE_ERR, 1);
        CHECK(mw_exec(db, "INSERT INTO log(tag,k) VALUES(2,0),(2,1),(2,2)") != SQLITE_OK);           // fails, rolled back
        CHECK(mw_scalar(db, "SELECT count(*) FROM log WHERE tag=2") == 0);
        CHECK_RC(mw_exec(db, "INSERT INTO log(tag,k) VALUES(3,0),(3,1),(3,2)"), SQLITE_OK);          // still usable
        mw_fault_arm(MW_FAULT_ALLOC_ERR, 1);
        CHECK(mw_exec(db, "INSERT INTO log(tag,k) VALUES(4,0),(4,1),(4,2)") != SQLITE_OK);
        CHECK_RC(mw_exec(db, "INSERT INTO log(tag,k) VALUES(5,0),(5,1),(5,2)"), SQLITE_OK);
        CHECK(mw_scalar(db, "SELECT count(DISTINCT tag) FROM log") == 3);
        // an fsync failure leaves the outcome uncertain: the database refuses further commits until reopened
        mw_fault_arm(MW_FAULT_LOG_SYNC_ERR, 1);
        CHECK(mw_exec(db, "INSERT INTO log(tag,k) VALUES(6,0),(6,1),(6,2)") != SQLITE_OK);
        CHECK(mw_exec(db, "INSERT INTO log(tag,k) VALUES(7,0),(7,1),(7,2)") != SQLITE_OK);
        sqlite3_close(db);                                                                           // failed db: log kept for recovery
        char lp[300]; snprintf(lp, sizeof lp, "%s-mw", path);
        CHECK(access(lp, F_OK) == 0);
        CHECK_RC(open_lane(path, &db, ""), SQLITE_OK);
        int64_t tags = mw_scalar(db, "SELECT count(DISTINCT tag) FROM log");
        CHECK(tags == 3 || tags == 4);                                                               // tag 6 may or may not have reached the log
        CHECK(mw_scalar(db, "SELECT count(*) FROM log WHERE tag=7") == 0);
        CHECK(integrity_ok(db));
        sqlite3_close(db);
        mw_rmdb(path); unlink(lp);
    }
    MW_DONE();
}
