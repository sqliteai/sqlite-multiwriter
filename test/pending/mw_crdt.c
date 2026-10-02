// Phase 11: fine-grained CRDT conflicts resolved through the rebase. Every outcome is decided by
// merge_insert(); the assertions check the CRDT semantics (column merge, deterministic winners,
// delete/resurrect, block-level merge), including that the result is independent of commit order.
#include "mw_test.h"
#include "multiwriter.h"

static int close_cs (sqlite3 *db) { return sqlite3_close(db); }
static int open_lane (const char *path, sqlite3 **db) {
    char uri[300];
    snprintf(uri, sizeof uri, "file:%s?mw=2&mw_gc=0&mw_readcheck=0", path);
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) sqlite3_extended_result_codes(*db, 1);
    return rc;
}
static uint64_t rebases (sqlite3 *db) { mw_db_stats s; memset(&s, 0, sizeof s); sqlite3_file_control(db, "main", MW_FCNTL_DBSTATS, &s); return s.rebases; }
static char *text (sqlite3 *db, const char *sql) {
    static char buf[4][512]; static int k;
    char *b = buf[k++ & 3]; b[0] = 0;
    sqlite3_stmt *st;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0)) snprintf(b, 512, "%s", (const char *)sqlite3_column_text(st, 0));
    sqlite3_finalize(st);
    return b;
}

static void setup (char *path, const char *tag, sqlite3 **a, sqlite3 **b) {
    mw_tmpdb(path, 256, tag);
    sqlite3 *s;
    CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id TEXT PRIMARY KEY NOT NULL, a TEXT, b TEXT, c INTEGER);"
                        "CREATE TABLE notes(id TEXT PRIMARY KEY NOT NULL, body TEXT NOT NULL DEFAULT '');"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "SELECT cloudsync_set_column('notes','body','algo','block')"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "INSERT INTO t VALUES('r1','a0','b0',0),('r2','a0','b0',0),('r3','a0','b0',0)"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "INSERT INTO notes VALUES('n1','Line 1: Welcome\nLine 2: Agenda\nLine 3: Action items')"), SQLITE_OK);
    CHECK_RC(close_cs(s), SQLITE_OK);
    CHECK_RC(open_lane(path, a), SQLITE_OK);
    CHECK_RC(open_lane(path, b), SQLITE_OK);
}
static void teardown (const char *path, sqlite3 *a, sqlite3 *b) { close_cs(a); close_cs(b); mw_rmdb(path); }

// A and B both start from the same snapshot; `first` commits first, then the other is rebased.
static void run_pair (sqlite3 *a, const char *sa, sqlite3 *b, const char *sb, int a_first) {
    char sql[600];
    snprintf(sql, sizeof sql, "BEGIN; %s", sa); CHECK_RC(mw_exec(a, sql), SQLITE_OK);
    snprintf(sql, sizeof sql, "BEGIN; %s", sb); CHECK_RC(mw_exec(b, sql), SQLITE_OK);
    CHECK_RC(mw_exec(a_first ? a : b, "COMMIT"), SQLITE_OK);
    CHECK_RC(mw_exec(a_first ? b : a, "COMMIT"), SQLITE_OK);
}

int main (void) {
    char path[256];
    sqlite3 *a, *b;

    // ---- same row, different columns: both survive, in either commit order
    for (int order = 0; order < 2; order++) {
        setup(path, "col", &a, &b);
        run_pair(a, "UPDATE t SET a='A1' WHERE id='r1'", b, "UPDATE t SET b='B1' WHERE id='r1'", order);
        CHECK(strcmp(text(a, "SELECT a||'/'||b FROM t WHERE id='r1'"), "A1/B1") == 0);
        CHECK(rebases(a) == 1);
        teardown(path, a, b);
    }

    char w0[64], w1[64];
    // ---- same cell written by two concurrent local transactions: first committer wins, the second is refused (retryable).
    // (A blind write and UPDATE t SET a=a+1 cannot be told apart: replaying the second would silently lose an update. Merging
    // the same cell by last-writer-wins still applies to changes that arrive from *other replicas*.)
    for (int order = 0; order < 2; order++) {
        setup(path, "same", &a, &b);
        CHECK_RC(mw_exec(a, "BEGIN; UPDATE t SET a='from-A' WHERE id='r1'"), SQLITE_OK);
        CHECK_RC(mw_exec(b, "BEGIN; UPDATE t SET a='from-B' WHERE id='r1'"), SQLITE_OK);
        sqlite3 *w = order ? b : a, *l = order ? a : b;
        CHECK_RC(mw_exec(w, "COMMIT"), SQLITE_OK);
        CHECK_RC(mw_exec(l, "COMMIT"), SQLITE_BUSY_SNAPSHOT);
        mw_exec(l, "ROLLBACK");
        CHECK(strcmp(text(a, "SELECT a FROM t WHERE id='r1'"), order ? "from-B" : "from-A") == 0);
        CHECK(strcmp(text(a, "SELECT a FROM t WHERE id='r1'"), text(b, "SELECT a FROM t WHERE id='r1'")) == 0);
        teardown(path, a, b);
    }
    // a lost counter increment is impossible: refused transactions are retried and every increment lands
    setup(path, "counter", &a, &b);
    for (int i = 0; i < 50; i++) {
        sqlite3 *x = (i & 1) ? a : b, *y = (i & 1) ? b : a;
        CHECK_RC(mw_exec(x, "BEGIN; UPDATE t SET c=c+1 WHERE id='r1'"), SQLITE_OK);
        CHECK_RC(mw_exec(y, "BEGIN; UPDATE t SET c=c+1 WHERE id='r1'"), SQLITE_OK);
        CHECK_RC(mw_exec(x, "COMMIT"), SQLITE_OK);
        if (mw_exec(y, "COMMIT") != SQLITE_OK) { mw_exec(y, "ROLLBACK"); CHECK_RC(mw_exec(y, "UPDATE t SET c=c+1 WHERE id='r1'"), SQLITE_OK); }
    }
    CHECK(mw_scalar(a, "SELECT c FROM t WHERE id='r1'") == 100);
    teardown(path, a, b);

    // a transaction that updates the same cell twice (col_version +4) against one concurrent increment (+2) must still be refused
    setup(path, "twice", &a, &b);
    CHECK_RC(mw_exec(a, "BEGIN; UPDATE t SET c=c+1 WHERE id='r1'; UPDATE t SET c=c+1 WHERE id='r1'"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "BEGIN; UPDATE t SET c=c+1 WHERE id='r1'"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "COMMIT"), SQLITE_OK);
    int rca = mw_exec(a, "COMMIT");
    printf("double update vs concurrent increment: commit rc=%d, c=%lld (2 + 1 = 3 required if committed)\n", rca, (long long)mw_scalar(b, "SELECT c FROM t WHERE id='r1'"));
    if (rca != SQLITE_OK) mw_exec(a, "ROLLBACK");
    CHECK(rca != SQLITE_OK || mw_scalar(b, "SELECT c FROM t WHERE id='r1'") == 3);
    teardown(path, a, b);

    // ---- delete vs update: the delete wins (causal length), in either order
    for (int order = 0; order < 2; order++) {
        setup(path, "delupd", &a, &b);
        run_pair(a, "DELETE FROM t WHERE id='r2'", b, "UPDATE t SET a='late' WHERE id='r2'", order);
        CHECK(mw_scalar(a, "SELECT count(*) FROM t WHERE id='r2'") == 0);
        CHECK(mw_scalar(b, "SELECT count(*) FROM t WHERE id='r2'") == 0);
        CHECK(mw_scalar(a, "SELECT count(*) FROM t") == 2);
        teardown(path, a, b);
    }

    // ---- delete then resurrect while a third transaction, based on the old snapshot, updates the row
    setup(path, "resurrect", &a, &b);
    sqlite3 *c; CHECK_RC(open_lane(path, &c), SQLITE_OK);
    CHECK_RC(mw_exec(c, "BEGIN; UPDATE t SET b='stale-update' WHERE id='r3'"), SQLITE_OK);      // snapshot before the delete
    CHECK_RC(mw_exec(a, "DELETE FROM t WHERE id='r3'"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "INSERT INTO t VALUES('r3','reborn','reborn',7)"), SQLITE_OK);           // resurrection (causal length 3)
    CHECK(mw_scalar(a, "SELECT count(*) FROM t WHERE id='r3' AND a='reborn'") == 1);
    CHECK_RC(mw_exec(c, "COMMIT"), SQLITE_OK);                                                    // rebased against cl=3: stale update loses
    CHECK(strcmp(text(a, "SELECT a||'/'||b||'/'||c FROM t WHERE id='r3'"), "reborn/reborn/7") == 0);
    CHECK(rebases(a) >= 1);
    close_cs(c);
    teardown(path, a, b);

    // ---- Block-level LWW: edits to different lines both survive; the same line resolves per block
    for (int order = 0; order < 2; order++) {
        setup(path, "block", &a, &b);
        run_pair(a, "UPDATE notes SET body='Line 1: Welcome everyone\nLine 2: Agenda\nLine 3: Action items' WHERE id='n1'",
                 b, "UPDATE notes SET body='Line 1: Welcome\nLine 2: Agenda\nLine 3: Action items - DONE' WHERE id='n1'", order);
        CHECK(strcmp(text(a, "SELECT replace(body, char(10), '|') FROM notes WHERE id='n1'"),
                     "Line 1: Welcome everyone|Line 2: Agenda|Line 3: Action items - DONE") == 0);
        CHECK(strcmp(text(a, "SELECT body FROM notes WHERE id='n1'"), text(b, "SELECT body FROM notes WHERE id='n1'")) == 0);
        teardown(path, a, b);
    }
    for (int order = 0; order < 2; order++) {
        setup(path, "block2", &a, &b);
        // same line edited by both + an insertion by one of them
        run_pair(a, "UPDATE notes SET body='Line 1: Welcome\nLine 2: Agenda (A)\nLine 3: Action items' WHERE id='n1'",
                 b, "UPDATE notes SET body='Line 1: Welcome\nLine 2: Agenda (B)\nLine 3: Action items\nLine 4: Extra' WHERE id='n1'", order);
        char one[512]; snprintf(one, sizeof one, "%s", text(a, "SELECT body FROM notes WHERE id='n1'"));
        CHECK(strstr(one, "Line 4: Extra") != NULL);
        CHECK(strstr(one, "Agenda (A)") != NULL || strstr(one, "Agenda (B)") != NULL);
        CHECK(strcmp(one, text(b, "SELECT body FROM notes WHERE id='n1'")) == 0);
        if (order == 0) snprintf(w0, 64, "%s", strstr(one, "(A)") ? "A" : "B"); else snprintf(w1, 64, "%s", strstr(one, "(A)") ? "A" : "B");
        teardown(path, a, b);
    }
    CHECK(strcmp(w0, w1) == 0);                                  // same-line winner is order independent as well

    MW_DONE();
}
