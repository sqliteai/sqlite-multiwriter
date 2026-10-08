// The rebase on a WITHOUT ROWID table (its pages are index b-trees and its interior cells hold rows), under a lot of splitting: threads insert, update and delete rows of their own keys (interleaved
// on the same pages) in transactions of a few blind statements; at the end every thread's rows are what it left, and the file is intact.
#include <pthread.h>
#include <stdatomic.h>
#include "mw_test.h"
#include "multiwriter.h"

static _Atomic unsigned long long g_rebases, g_unreb;
static const char *g_path; enum { NT = 4, KEYS = 1200, ROUNDS = 2500 };
typedef struct { int id; unsigned rng; int exists[KEYS], val[KEYS]; long committed, refused; } worker_t;
static unsigned rnd (unsigned *s) { *s = *s * 1103515245u + 12345u; return *s >> 8; }

static void *work (void *arg) {
    worker_t *w = arg; sqlite3 *db; char uri[300];
    snprintf(uri, sizeof uri, "file:%s?mw=2&mw_rebase=1&mw_rebase_backoff=0%s", g_path, getenv("MW_TEST_MP") ? "&mw_mp=1" : "");
    if (sqlite3_open_v2(uri, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL) != SQLITE_OK) { mw_failures++; return NULL; }
    sqlite3_busy_timeout(db, 0);
    for (int r = 0; r < ROUNDS; r++) {
        int n = 1 + (int)(rnd(&w->rng) % 4), keys[4], kind[4], val[4], used = 0; char sql[1200]; int off = 0;
        off += snprintf(sql + off, sizeof sql - (size_t)off, "BEGIN;");
        for (int i = 0; i < n; i++) {
            int k, dup; do { k = (int)(rnd(&w->rng) % (KEYS / NT)) * NT + w->id; dup = 0; for (int j = 0; j < used; j++) if (keys[j] == k) dup = 1; } while (dup);
            keys[used] = k; val[used] = (int)(rnd(&w->rng) % 100000); kind[used] = w->exists[k] ? (int)(rnd(&w->rng) % 3) : 3; // 0 update, 1 delete, 2 update of the payload, 3 insert
            if (kind[used] == 3) off += snprintf(sql + off, sizeof sql - (size_t)off, "INSERT INTO t(k1, k2, v, pad) VALUES(%d, 'k%d', %d, zeroblob(%d));", k / 100, k, val[used], (int)(rnd(&w->rng) % 150));
            else if (kind[used] == 1) off += snprintf(sql + off, sizeof sql - (size_t)off, "DELETE FROM t WHERE k1 = %d AND k2 = 'k%d';", k / 100, k);
            else if (kind[used] == 0) off += snprintf(sql + off, sizeof sql - (size_t)off, "UPDATE t SET v = %d WHERE k1 = %d AND k2 = 'k%d';", val[used], k / 100, k);
            else off += snprintf(sql + off, sizeof sql - (size_t)off, "UPDATE t SET pad = zeroblob(%d) WHERE k1 = %d AND k2 = 'k%d';", (int)(rnd(&w->rng) % 700), k / 100, k);
            used++;
        }
        snprintf(sql + off, sizeof sql - (size_t)off, "COMMIT");
        int rc = mw_exec(db, sql);
        if (rc == SQLITE_OK) {
            w->committed++;
            for (int i = 0; i < used; i++) { if (kind[i] == 3) { w->exists[keys[i]] = 1; w->val[keys[i]] = val[i]; } else if (kind[i] == 1) w->exists[keys[i]] = 0; else if (kind[i] == 0) w->val[keys[i]] = val[i]; }
        } else { w->refused++; if ((rc & 0xff) != SQLITE_BUSY) { printf("unexpected rc=%d: %s\n", rc, sqlite3_errmsg(db)); mw_failures++; } mw_exec(db, "ROLLBACK"); }
    }
    { mw_db_stats ds; memset(&ds, 0, sizeof ds); sqlite3_file_control(db, "main", MW_FCNTL_DBSTATS, &ds); g_rebases += ds.rebases; g_unreb += ds.unrebasable; }
    sqlite3_close(db); return NULL;
}

int main (void) {
    char path[256]; mw_tmpdb(path, sizeof path, "rebasewr"); g_path = path;
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(k1 INTEGER, k2 TEXT, v INTEGER, pad BLOB, PRIMARY KEY(k1, k2)) WITHOUT ROWID; CREATE INDEX tv ON t(v)"), SQLITE_OK);
    sqlite3_close(s);
    static worker_t w[NT]; pthread_t th[NT];
    for (int i = 0; i < NT; i++) { w[i].id = i; w[i].rng = 77u + 1013u * (unsigned)i; pthread_create(&th[i], NULL, work, &w[i]); }
    for (int i = 0; i < NT; i++) pthread_join(th[i], NULL);
    sqlite3 *c; CHECK_RC(sqlite3_open_v2(path, &c, SQLITE_OPEN_READWRITE, "unix"), SQLITE_OK);
    long rows = 0, wrong = 0, expected = 0, committed = 0, refused = 0;
    for (int i = 0; i < NT; i++) { committed += w[i].committed; refused += w[i].refused; for (int k = 0; k < KEYS; k++) if (w[i].exists[k]) expected++; }
    sqlite3_stmt *st; sqlite3_prepare_v2(c, "SELECT k1, k2, v FROM t", -1, &st, NULL);
    while (sqlite3_step(st) == SQLITE_ROW) {
        rows++; int k = atoi((const char *)sqlite3_column_text(st, 1) + 1), id = k % NT;
        if (k / 100 != sqlite3_column_int(st, 0) || !w[id].exists[k] || w[id].val[k] != sqlite3_column_int(st, 2)) { if (wrong++ < 5) printf("row %d differs: file (%d) model exists %d value %d\n", k, sqlite3_column_int(st, 2), w[id].exists[k], w[id].val[k]); }
    }
    sqlite3_finalize(st);
    int ok = 0; sqlite3_prepare_v2(c, "PRAGMA integrity_check", -1, &st, NULL); if (sqlite3_step(st) == SQLITE_ROW) ok = strcmp((const char *)sqlite3_column_text(st, 0), "ok") == 0; sqlite3_finalize(st);
    printf("rebase on a WITHOUT ROWID table: rebases %llu, refused to replay %llu; %ld transactions committed, %ld refused; rows %ld (model %ld), wrong %ld; integrity %s\n", (unsigned long long)g_rebases, (unsigned long long)g_unreb, committed, refused, rows, expected, wrong, ok ? "ok" : "BAD");
    CHECK(rows == expected); CHECK(wrong == 0); CHECK(ok); CHECK(g_rebases > 0);
    sqlite3_close(c); mw_rmdb(path);
    MW_DONE();
}
