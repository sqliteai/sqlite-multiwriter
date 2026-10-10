// A publisher is killed in the middle of installing the versions of its commit in the index, and the index is nearly full. The record is complete in the log, so the process that takes the
// lock finishes it. The repair installed the whole commit again if there was room for all of it, and otherwise nothing: with the entries that the dead one had taken the room was short, and
// the commit was published with only some of its pages in the index (a table whose pages did not belong together: integrity_check failed, and the compaction made it permanent). Now the
// pages that are in the index at that epoch are left, and the rest is installed.
#include <sys/wait.h>
#include <signal.h>
#include "mw_test.h"
#include "multiwriter.h"
#include "multiwriter_internal.h"
#include "multiwriter_shidx.h"

static int g_die_at;
static void die_hook (int done) { if (done >= g_die_at) _exit(9); }

static int scenario (int die_at, int slack) {
    setenv("MW_IDX_ENTRIES", "1100", 1);
    char path[256]; mw_tmpdb(path, sizeof path, "repairinst");
    char uri[400]; snprintf(uri, sizeof uri, "file:%s?vfs=multiwriter&mw_mp=1", path);
    sqlite3 *a = NULL, *rd = NULL;
    CHECK_RC(sqlite3_open_v2(uri, &a, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    CHECK_RC(sqlite3_open_v2(uri, &rd, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    CHECK_RC(mw_exec(a, "CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB); CREATE TABLE u(id INTEGER PRIMARY KEY, v)"), SQLITE_OK);
    CHECK_RC(mw_exec(rd, "BEGIN"), SQLITE_OK); mw_scalar(rd, "SELECT count(*) FROM t");        // a reader pins the snapshot: nothing newer can be collected
    char *ip = mw_sidecar_path(path, "mwidx");
    shidx *ix = shidx_open(ip, NULL);                                                          // the index of the database (it adopts what is in the file): to see the room
    CHECK(ix != NULL);
    if (!ix) return 1;
    uint32_t r0 = shidx_room(ix);
    CHECK_RC(mw_exec(a, "INSERT INTO t(v) VALUES (replace(hex(zeroblob(10000)), '00', 'ab'))"), SQLITE_OK);
    int cons = (int)(r0 - shidx_room(ix));                                                     // entries that a commit of this kind takes: the size record and its pages
    int n = cons - 1;
    while ((int)shidx_room(ix) > 2 * cons + 4 + slack + n) CHECK_RC(mw_exec(a, "INSERT INTO t(v) VALUES (replace(hex(zeroblob(10000)), '00', 'ab'))"), SQLITE_OK);
    for (int guard = 0; (int)shidx_room(ix) > n + 2 + slack && guard < 3000; guard++) CHECK_RC(mw_exec(a, "INSERT INTO u(v) VALUES ('x')"), SQLITE_OK);
    printf("a commit takes %d entries; room before the victim %u (n + 2 = %d); killed after %d entries\n", cons, shidx_room(ix), n + 2, die_at);
    int64_t rows0 = mw_scalar(a, "SELECT count(*) FROM t");
    pid_t c = fork();
    if (c == 0) {
        g_die_at = die_at; shidx_install_hook = die_hook;
        sqlite3 *d = NULL; if (sqlite3_open_v2(uri, &d, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL) != SQLITE_OK) _exit(2);
        int crc = mw_exec(d, "INSERT INTO t(v) VALUES (replace(hex(zeroblob(10000)), '00', 'ab'))");
        _exit(crc ? 20 + (crc & 0x1f) : 3);
    }
    int st = 0; waitpid(c, &st, 0);
    printf("child: exit code %d\n", WIFEXITED(st) ? WEXITSTATUS(st) : -1);
    CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 9);
    mw_exec(rd, "COMMIT");
    CHECK_RC(mw_exec(a, "INSERT INTO u(v) VALUES ('after')"), SQLITE_OK);                      // another live process: takes the lock, repairs
    int bad = 0;
    sqlite3_stmt *s = NULL; char msg[200] = "";
    if (sqlite3_prepare_v2(a, "PRAGMA integrity_check", -1, &s, NULL) == SQLITE_OK && sqlite3_step(s) == SQLITE_ROW) snprintf(msg, sizeof msg, "%s", (const char *)sqlite3_column_text(s, 0));
    sqlite3_finalize(s);
    if (strcmp(msg, "ok") != 0) { printf("integrity_check: %.150s\n", msg); bad = 1; }
    // the commit of the dead one was complete in the log: it is there, the whole of it, and every row reads back
    int64_t rows = mw_scalar(a, "SELECT count(*) FROM t"), whole = mw_scalar(a, "SELECT count(*) FROM t WHERE v = replace(hex(zeroblob(10000)), '00', 'ab')");
    if (rows != rows0 + 1 || whole != rows) { printf("rows %lld (expected %lld), rows that read back whole %lld\n", (long long)rows, (long long)rows0 + 1, (long long)whole); bad = 1; }
    shidx_close(ix); sqlite3_free(ip);
    sqlite3_close(rd); sqlite3_close(a); mw_rmfiles(path);
    return bad;
}

int main (void) {
    int bad = 0;
    bad += scenario(4, 2);       // killed after the size record and three pages, with room for the commit and a little more: not for all of it again
    bad += scenario(3, 2);
    bad += scenario(5, 2);
    CHECK(bad == 0);
    MW_DONE();
}
