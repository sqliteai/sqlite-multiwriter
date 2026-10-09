// Multi-process mode (URI mw_mp=1): several processes, each with several connections, on one database.
// Children are forked BEFORE the database is opened in the parent (a forked lane state is not shareable).
#include <pthread.h>
#include <signal.h>
#include <sys/wait.h>
#include "mw_test.h"
#include "multiwriter.h"

static int close_cs (sqlite3 *db) { return sqlite3_close(db); }
static int open_mp (const char *path, sqlite3 **db, const char *extra) {
    char uri[400];
    snprintf(uri, sizeof uri, "file:%s?mw=1&mw_mp=1&mw_gc=32%s", path, extra);
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) { sqlite3_extended_result_codes(*db, 1); sqlite3_busy_timeout(*db, 0); }
    return rc;
}
static mw_db_stats stats (sqlite3 *db) { mw_db_stats s; memset(&s, 0, sizeof s); sqlite3_file_control(db, "main", MW_FCNTL_DBSTATS, &s); return s; }
static int integrity_ok (sqlite3 *db) {
    sqlite3_stmt *st; int ok = 0;
    if (sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) ok = strcmp((const char *)sqlite3_column_text(st, 0), "ok") == 0;
    sqlite3_finalize(st);
    return ok;
}
static int64_t stock_scalar (const char *path, const char *sql, int *integrity) {
    char uri[400]; snprintf(uri, sizeof uri, "file:%s?immutable=1", path);
    sqlite3 *c; int64_t v = -1;
    if (sqlite3_open_v2(uri, &c, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, MW_PLAIN_VFS) == SQLITE_OK) { v = mw_scalar(c, sql); if (integrity) *integrity = integrity_ok(c); }
    sqlite3_close(c);
    return v;
}

static void make_db (const char *path) {
    sqlite3 *s;
    CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL;"
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER, pad BLOB);"                     // one row per page: disjoint writes
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<64) INSERT INTO t SELECT i, 0, zeroblob(3000) FROM n;"
        "CREATE TABLE ctr(id INTEGER PRIMARY KEY, n INTEGER); INSERT INTO ctr VALUES(1,0);"
        "CREATE TABLE docs(id TEXT PRIMARY KEY NOT NULL, owner INTEGER, body TEXT);"), SQLITE_OK);
    CHECK_RC(close_cs(s), SQLITE_OK);
}

typedef struct { const char *path; int proc, thread, row, iters, kind; int64_t ok, retries; const char *extra; } job_t;
static volatile int g_stop;

// kind 0: own row, disjoint pages; 1: shared counter (retry until committed); 2: unique inserts into a tracked table
static void *worker (void *arg) {
    job_t *j = arg;
    sqlite3 *db; int orc = open_mp(j->path, &db, j->extra);
    if (orc != SQLITE_OK) { fprintf(stderr, "proc %d thread %d: open failed rc=%d %s\n", j->proc, j->thread, orc, db ? sqlite3_errmsg(db) : ""); j->ok = -1; return NULL; }
    for (int i = 0; (j->iters ? i < j->iters : !g_stop); i++) {
        char sql[200];
        if (j->kind == 0) snprintf(sql, sizeof sql, "UPDATE t SET v=v+1 WHERE id=%d", j->row);
        else if (j->kind == 1) snprintf(sql, sizeof sql, "UPDATE ctr SET n=n+1 WHERE id=1");
        else snprintf(sql, sizeof sql, "INSERT INTO docs VALUES('p%d-t%d-%d',%d,'body %d')", j->proc, j->thread, i, j->proc, i);
        for (int a = 0; a < 5000; a++) {
            int rc = mw_exec(db, sql);
            if (rc == SQLITE_OK) { j->ok++; break; }
            if ((rc & 0xff) != SQLITE_BUSY) { fprintf(stderr, "proc %d thread %d iter %d: rc=%d ext=%d %s\n", j->proc, j->thread, i, rc, sqlite3_extended_errcode(db), sqlite3_errmsg(db)); break; }
            j->retries++;
            if (a == 4999) fprintf(stderr, "proc %d thread %d iter %d: gave up after 5000 retries, last rc=%d\n", j->proc, j->thread, i, sqlite3_extended_errcode(db));
        }
    }
    close_cs(db);
    return NULL;
}

// Child process: `nthreads` workers; writes "acked" per worker into the pipe (proc, thread, ok, retries).
static void child_main (int wfd, const char *path, int proc, int nthreads, int kind, int iters, int row_base, const char *extra) {
    pthread_t th[8]; job_t jobs[8];
    for (int t = 0; t < nthreads; t++) {
        jobs[t] = (job_t){ .path = path, .proc = proc, .thread = t, .row = row_base + t, .iters = iters, .kind = kind, .extra = extra };
        pthread_create(&th[t], NULL, worker, &jobs[t]);
    }
    for (int t = 0; t < nthreads; t++) {
        pthread_join(th[t], NULL);
        int64_t rec[4] = { proc, t, jobs[t].ok, jobs[t].retries };
        if (write(wfd, rec, sizeof rec) != sizeof rec) _exit(5);
    }
    _exit(0);
}

static int spawn (int *rfd, const char *path, int proc, int nthreads, int kind, int iters, int row_base, const char *extra) {
    int pfd[2]; CHECK(pipe(pfd) == 0);
    pid_t pid = fork();
    if (pid == 0) { close(pfd[0]); child_main(pfd[1], path, proc, nthreads, kind, iters, row_base, extra); }
    close(pfd[1]);
    *rfd = pfd[0];
    return (int)pid;
}
// reads (proc, thread, ok, retries) records; sums ok per row group
static int64_t collect (int rfd, int64_t *retries) {
    int64_t rec[4], total = 0;
    while (read(rfd, rec, sizeof rec) == sizeof rec) { total += rec[2]; if (retries) *retries += rec[3]; }
    close(rfd);
    return total;
}

int main (void) {
    char path[256], lp[300], mp[300];
    const int NP = getenv("MP_NP") ? atoi(getenv("MP_NP")) : 4, NT = getenv("MP_NT") ? atoi(getenv("MP_NT")) : 4;

    // ---- 1. disjoint writes from 4 processes x 4 threads
    mw_tmpdb(path, sizeof path, "mp1"); snprintf(lp, sizeof lp, "%s-mw", path); snprintf(mp, sizeof mp, "%s-mwlock", path); unlink(lp); unlink(mp);
    make_db(path);
    int rfd[16], pid[16];
    for (int p = 0; p < NP; p++) pid[p] = spawn(&rfd[p], path, p, NT, 0, 2000, 1 + p * NT, "");
    int64_t acked = 0;
    for (int p = 0; p < NP; p++) { acked += collect(rfd[p], NULL); int st; waitpid(pid[p], &st, 0); CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 0); }
    printf("disjoint: %lld acked commits from %d processes\n", (long long)acked, NP);
    CHECK(acked == (int64_t)NP * NT * 2000);
    {   // the parent opens after everybody left: recovers from the log, everything is there
        sqlite3 *a; CHECK_RC(open_mp(path, &a, ""), SQLITE_OK);
        CHECK(mw_scalar(a, "SELECT sum(v) FROM t") == acked);
        CHECK(mw_scalar(a, "SELECT count(*) FROM t WHERE v=2000") == NP * NT);
        CHECK(integrity_ok(a));
        CHECK_RC(close_cs(a), SQLITE_OK);
    }
    { int ig = 0; CHECK(stock_scalar(path, "SELECT sum(v) FROM t", &ig) == acked && ig); }   // ordinary SQLite file again
    CHECK(access(lp, F_OK) != 0 && access(mp, F_OK) != 0);                                     // log and lock file gone
    mw_rmdb(path);

    // ---- 2. one shared counter hammered from every process: no lost update (conflicts are retried)
    mw_tmpdb(path, sizeof path, "mp2"); snprintf(lp, sizeof lp, "%s-mw", path); snprintf(mp, sizeof mp, "%s-mwlock", path); unlink(lp); unlink(mp);
    make_db(path);
    int64_t retries = 0; acked = 0;
    for (int p = 0; p < NP; p++) pid[p] = spawn(&rfd[p], path, p, 2, 1, 400, 0, "");
    for (int p = 0; p < NP; p++) { acked += collect(rfd[p], &retries); int st; waitpid(pid[p], &st, 0); CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 0); }
    printf("shared counter: %lld commits, %lld retries\n", (long long)acked, (long long)retries);
    { sqlite3 *a; CHECK_RC(open_mp(path, &a, ""), SQLITE_OK); CHECK(mw_scalar(a, "SELECT n FROM ctr WHERE id=1") == acked); CHECK(acked == NP * 2 * 400); close_cs(a); }
    mw_rmdb(path);

    // ---- 3. tracked table, unique inserts from every process (rebase across processes), unique db_versions
    mw_tmpdb(path, sizeof path, "mp3"); snprintf(lp, sizeof lp, "%s-mw", path); snprintf(mp, sizeof mp, "%s-mwlock", path); unlink(lp); unlink(mp);
    make_db(path);
    acked = 0;
    for (int p = 0; p < 3; p++) pid[p] = spawn(&rfd[p], path, p, 2, 2, 150, 0, "");
    for (int p = 0; p < 3; p++) { acked += collect(rfd[p], NULL); int st; waitpid(pid[p], &st, 0); CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 0); }
    printf("tracked inserts: %lld commits\n", (long long)acked);
    CHECK(acked == 3 * 2 * 150);
    {
        sqlite3 *a; CHECK_RC(open_mp(path, &a, ""), SQLITE_OK);
        CHECK(mw_scalar(a, "SELECT count(*) FROM docs") == acked);
        CHECK(integrity_ok(a));
        close_cs(a);
    }
    mw_rmdb(path);

    // ---- 4. SIGKILL one process while the others keep writing
    mw_tmpdb(path, sizeof path, "mp4"); snprintf(lp, sizeof lp, "%s-mw", path); snprintf(mp, sizeof mp, "%s-mwlock", path); unlink(lp); unlink(mp);
    make_db(path);
    for (int p = 0; p < 3; p++) pid[p] = spawn(&rfd[p], path, p, 4, 0, p == 0 ? 0 : 3000, 1 + p * 4, "&mw_compact_ms=5");   // proc 0 runs until killed
    struct timespec ts = { 0, 150 * 1000000L }; nanosleep(&ts, NULL);
    kill(pid[0], SIGKILL);
    int st; waitpid(pid[0], &st, 0); CHECK(WIFSIGNALED(st));
    int64_t survivors = 0;
    for (int p = 1; p < 3; p++) { survivors += collect(rfd[p], NULL); waitpid(pid[p], &st, 0); CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 0); }
    close(rfd[0]);
    CHECK(survivors == 2 * 4 * 3000);                                    // the others were not disturbed by the death
    {
        sqlite3 *a; CHECK_RC(open_mp(path, &a, ""), SQLITE_OK);
        CHECK(mw_scalar(a, "SELECT sum(v) FROM t WHERE id > 4") == survivors);       // rows of the survivors: exact
        int64_t killed = mw_scalar(a, "SELECT sum(v) FROM t WHERE id <= 4");          // the killed process: whatever it had committed, atomically
        printf("SIGKILL: killed process had committed %lld updates; survivors %lld\n", (long long)killed, (long long)survivors);
        CHECK(killed > 0);
        CHECK(integrity_ok(a));
        CHECK_RC(mw_exec(a, "UPDATE t SET v=v+1 WHERE id=1"), SQLITE_OK);              // and the database keeps working
        close_cs(a);
    }
    { int ig = 0; CHECK(stock_scalar(path, "SELECT count(*) FROM t", &ig) == 64 && ig); }
    mw_rmdb(path);

    // ---- 5. snapshot isolation across processes: a reader in this process is not disturbed by another process's commits
    mw_tmpdb(path, sizeof path, "mp5"); snprintf(lp, sizeof lp, "%s-mw", path); snprintf(mp, sizeof mp, "%s-mwlock", path); unlink(lp); unlink(mp);
    make_db(path);
    {
        int go[2], done[2]; CHECK(pipe(go) == 0 && pipe(done) == 0);
        pid_t w = fork();
        if (w == 0) {                                   // writer process: waits for the go signal, commits 500 updates, exits
            char c; if (read(go[0], &c, 1) != 1) _exit(2);
            sqlite3 *b; if (open_mp(path, &b, "") != SQLITE_OK) _exit(3);
            for (int i = 0; i < 500; i++) { int rc; do rc = mw_exec(b, "UPDATE t SET v=v+1 WHERE id=1"); while ((rc & 0xff) == SQLITE_BUSY); }
            close_cs(b);
            if (write(done[1], "x", 1) != 1) _exit(4);
            _exit(0);
        }
        sqlite3 *a; CHECK_RC(open_mp(path, &a, ""), SQLITE_OK);
        CHECK_RC(mw_exec(a, "BEGIN"), SQLITE_OK);
        CHECK(mw_scalar(a, "SELECT v FROM t WHERE id=1") == 0);
        CHECK(write(go[1], "g", 1) == 1);
        char c; CHECK(read(done[0], &c, 1) == 1);         // the other process committed 500 times meanwhile
        CHECK(mw_scalar(a, "SELECT v FROM t WHERE id=1") == 0);                                 // same snapshot
        CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_OK);
        CHECK(mw_scalar(a, "SELECT v FROM t WHERE id=1") == 500);                               // a new snapshot sees them
        int wst; waitpid(w, &wst, 0); CHECK(WIFEXITED(wst) && WEXITSTATUS(wst) == 0);
        close_cs(a);
    }
    mw_rmdb(path);

    MW_DONE();
}
