// MW_PORTABLE_TEST: this test has a Windows branch for what it does with processes (the Makefile leaves out of the Windows build the tests that fork and do not say so)
// Several processes write one database (mw_mp=1), and one of them is killed: the others finish, the database is whole, and a new process can write. The processes are this program started again with
// arguments (fork does not exist on Windows), so the test is the same on every platform.
#include <fcntl.h>
#include <sys/stat.h>
#include "mw_test.h"
#include "multiwriter.h"
#ifndef O_BINARY
#define O_BINARY 0
#endif
#ifdef _WIN32
#include <windows.h>
typedef HANDLE proc_t;
#else
#include <signal.h>
#include <sys/wait.h>
typedef pid_t proc_t;
#endif

enum { KIDS = 4, ROWS = 400 };

static int child (const char *path, int id, int rows, int rebase) {
    char uri[600]; sqlite3 *db;
    snprintf(uri, sizeof uri, "file:%s?mw=1&mw_mp=1%s", path, rebase ? "&mw_rebase=1" : "");
    int orc = sqlite3_open_v2(uri, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL);
    if (orc != SQLITE_OK) { printf("child %d: open failed: %d %s\n", id, db ? sqlite3_extended_errcode(db) : -1, db ? sqlite3_errmsg(db) : "(no handle)"); return 2; }
    sqlite3_busy_timeout(db, 0);
    char ackp[700]; snprintf(ackp, sizeof ackp, "%s.ack%d", path, id); int ack = open(ackp, O_WRONLY | O_APPEND | O_CREAT | O_BINARY, 0644);      // (a byte for each insert that was acknowledged: a kill cannot take back what the file has)
    for (int i = 0; i < rows; i++) {
        char sql[200]; snprintf(sql, sizeof sql, "INSERT INTO t(id, w, v) VALUES(%d, %d, %d)", id * 1000000 + i, id, i);
        int rc, tries = 0;
        do { rc = mw_exec(db, sql); } while ((rc & 0xff) == SQLITE_BUSY && ++tries < 1000000);
        if (rc == SQLITE_OK && ack >= 0) (void)!write(ack, "a", 1);
        if (rc != SQLITE_OK) { printf("child %d: insert %d failed: %d %s\n", id, i, rc, sqlite3_errmsg(db)); return 3; }
    }
    sqlite3_close(db);
    return 0;
}

static proc_t spawn (const char *self, const char *path, int id, int rows, int rebase) {
    char a1[16], a2[16], a3[16];
    snprintf(a1, sizeof a1, "%d", id); snprintf(a2, sizeof a2, "%d", rows); snprintf(a3, sizeof a3, "%d", rebase);
#ifdef _WIN32
    SetEnvironmentVariableA("MW_PROCS_CHILD", "1");
    char cmd[1200]; snprintf(cmd, sizeof cmd, "\"%s\" child \"%s\" %s %s %s", self, path, a1, a2, a3);
    STARTUPINFOA si; PROCESS_INFORMATION pi; memset(&si, 0, sizeof si); si.cb = sizeof si; memset(&pi, 0, sizeof pi);
    if (!CreateProcessA(NULL, cmd, NULL, NULL, TRUE, 0, NULL, NULL, &si, &pi)) return NULL;
    CloseHandle(pi.hThread);
    return pi.hProcess;
#else
    pid_t p = fork();
    if (p == 0) { setenv("MW_PROCS_CHILD", "1", 1); execl(self, self, "child", path, a1, a2, a3, (char *)NULL); _exit(127); }
    return p;
#endif
}
static int wait_for (proc_t p) {
#ifdef _WIN32
    WaitForSingleObject(p, INFINITE); DWORD code = 99; GetExitCodeProcess(p, &code); CloseHandle(p); return (int)code;
#else
    int st = 0; waitpid(p, &st, 0); return WIFEXITED(st) ? WEXITSTATUS(st) : 128 + (WIFSIGNALED(st) ? WTERMSIG(st) : 0);
#endif
}
static void kill_it (proc_t p) {
#ifdef _WIN32
    TerminateProcess(p, 137);
#else
    kill(p, SIGKILL);
#endif
}
static void pause_ms (int ms) {
#ifdef _WIN32
    Sleep((DWORD)ms);
#else
    usleep((useconds_t)ms * 1000);
#endif
}

static int integrity_ok (sqlite3 *db) {
    sqlite3_stmt *st; int ok = 0;
    if (sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) ok = strcmp((const char *)sqlite3_column_text(st, 0), "ok") == 0;
    sqlite3_finalize(st); return ok;
}

int main (int argc, char **argv) {
    const int is_child = argc >= 6 && strcmp(argv[1], "child") == 0;
    // a safeguard: a process that the test started and that does not understand its arguments must not start the test again (a program that starts copies of itself can fill the process table of the machine)
    if (getenv("MW_PROCS_CHILD") && !is_child) { fprintf(stderr, "mw_procs: started as a child without its arguments\n"); return 4; }
    if (is_child) return child(argv[2], atoi(argv[3]), atoi(argv[4]), atoi(argv[5]));
    char self[600];
#ifdef _WIN32
    GetModuleFileNameA(NULL, self, sizeof self);
#else
    snprintf(self, sizeof self, "%s", argv[0]);
#endif
    char path[300]; mw_tmpdb(path, sizeof path, "procs");
    for (int rebase = 0; rebase <= 1; rebase++) {
        mw_rmdb(path);
        for (int k = 0; k < 40; k++) { char ap[700]; snprintf(ap, sizeof ap, "%s.ack%d", path, k); unlink(ap); }
        sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
        CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, w INTEGER, v INTEGER)"), SQLITE_OK);
        sqlite3_close(s);

        proc_t p[KIDS]; sqlite3 *c; char uri[600]; snprintf(uri, sizeof uri, "file:%s?mw=1&mw_mp=1", path);
        if (!getenv("MW_PROCS_PHASE3")) {
        // 1. KIDS processes write at once
        for (int i = 0; i < KIDS; i++) { p[i] = spawn(self, path, i, ROWS, rebase); CHECK(p[i] != 0 && p[i] != (proc_t)-1); }
        int bad = 0; for (int i = 0; i < KIDS; i++) bad += wait_for(p[i]) != 0;
        CHECK(bad == 0);
        CHECK_RC(sqlite3_open_v2(uri, &c, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
        int64_t n1 = mw_scalar(c, "SELECT count(*) FROM t");
        printf("rebase %d: %d processes inserted %d rows each: %lld rows, integrity %s\n", rebase, KIDS, ROWS, (long long)n1, integrity_ok(c) ? "ok" : "BAD");
        CHECK(n1 == KIDS * ROWS); CHECK(integrity_ok(c));
        sqlite3_close(c);

        // 2. one is killed in the middle of its inserts: the others finish, nothing is torn, and a new process can write
        for (int i = 0; i < KIDS; i++) { p[i] = spawn(self, path, 10 + i, i == 1 ? 1000000 : ROWS * 4, rebase); CHECK(p[i] != 0 && p[i] != (proc_t)-1); }
        pause_ms(400); kill_it(p[1]); wait_for(p[1]);
        bad = 0; for (int i = 0; i < KIDS; i++) if (i != 1) bad += wait_for(p[i]) != 0;
        CHECK(bad == 0);
        proc_t again = spawn(self, path, 20, 50, rebase); CHECK(wait_for(again) == 0);
        CHECK_RC(sqlite3_open_v2(uri, &c, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
        int64_t killed = mw_scalar(c, "SELECT count(*) FROM t WHERE w = 11"), survivors = mw_scalar(c, "SELECT count(*) FROM t WHERE w IN (10, 12, 13)"), late = mw_scalar(c, "SELECT count(*) FROM t WHERE w = 20");
        printf("rebase %d: one process killed after %lld of %d inserts; the others %lld of %d; a new one %lld of 50; integrity %s\n", rebase, (long long)killed, 1000000, (long long)survivors, ROWS * 12, (long long)late, integrity_ok(c) ? "ok" : "BAD");
        { char ackp[700]; snprintf(ackp, sizeof ackp, "%s.ack11", path); struct stat sb; long long acked = stat(ackp, &sb) == 0 ? (long long)sb.st_size : -1;
          printf("rebase %d: the killed process had %lld inserts acknowledged and %lld rows are there\n", rebase, acked, (long long)killed); CHECK(killed >= acked); CHECK(killed <= acked + 1); }
        CHECK(survivors == ROWS * 12); CHECK(late == 50); CHECK(killed > 0 && killed < 1000000); CHECK(integrity_ok(c));
        CHECK(mw_scalar(c, "SELECT count(*) FROM t") == n1 + killed + survivors + late);
        sqlite3_close(c);
        }

        // 3. all of them are killed in the middle of their inserts, and the database is opened at once: what each had acknowledged is there
        for (int k = 0; k < 40; k++) { char ap[700]; snprintf(ap, sizeof ap, "%s.ack%d", path, k); unlink(ap); }
        for (int i = 0; i < KIDS; i++) { p[i] = spawn(self, path, 30 + i, 1000000, rebase); CHECK(p[i] != 0 && p[i] != (proc_t)-1); }
        pause_ms(600);
        for (int i = 0; i < KIDS; i++) { kill_it(p[i]); wait_for(p[i]); }
        { const char *tf = getenv("MW_OS_TRACE"); FILE *f = tf && tf[0] != '1' ? fopen(tf, "a") : NULL; if (f) { fprintf(f, "MARK all killed, the database is opened now\n"); fclose(f); } }
        CHECK_RC(sqlite3_open_v2(uri, &c, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
        for (int i = 0; i < KIDS; i++) {
            char ackp[700]; snprintf(ackp, sizeof ackp, "%s.ack%d", path, 30 + i); struct stat sb; long long acked = stat(ackp, &sb) == 0 ? (long long)sb.st_size : -1;
            char q[100]; snprintf(q, sizeof q, "SELECT count(*) FROM t WHERE w = %d", 30 + i); long long rows = mw_scalar(c, q);
            printf("rebase %d: all killed: process %d had %lld inserts acknowledged and %lld rows are there\n", rebase, 30 + i, acked, rows);
            CHECK(rows >= acked); CHECK(rows <= acked + 1);
        }
        CHECK(integrity_ok(c));
        sqlite3_close(c);
    }
    mw_rmdb(path);
    MW_DONE();
}
