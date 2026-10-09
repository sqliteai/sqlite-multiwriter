// The rebase with foreign keys under load: parents and children (ON DELETE CASCADE) in the same pages, threads that each own the parents with their number and the children of those. They insert and delete parents
// (a delete takes the children with it), insert, update and delete children, in transactions of a few blind statements, with the foreign keys on. At the end the rows are what each thread left, no foreign key is
// broken and the file is intact. (The rebase takes a good part of the commits: the parents' rows share pages.)
#include <pthread.h>
#include <stdatomic.h>
#include "mw_test.h"
#include "multiwriter.h"

enum { NT = 4, PARENTS = 240, KIDS = 6, ROUNDS = 2500 };
static const char *g_path; static _Atomic unsigned long long g_rebases;
typedef struct { int id; unsigned rng; unsigned char par[PARENTS]; unsigned char kid[PARENTS][KIDS]; int val[PARENTS][KIDS]; long committed, refused; } worker_t;
static unsigned rnd (unsigned *s) { *s = *s * 1103515245u + 12345u; return *s >> 8; }

static void *work (void *arg) {
    worker_t *w = arg; sqlite3 *db; char uri[300];
    snprintf(uri, sizeof uri, "file:%s?mw=1&mw_rebase=1&mw_rebase_backoff=0%s", g_path, getenv("MW_TEST_MP") ? "&mw_mp=1" : "");
    if (sqlite3_open_v2(uri, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL) != SQLITE_OK) { mw_failures++; return NULL; }
    sqlite3_busy_timeout(db, 0); mw_exec(db, "PRAGMA foreign_keys=ON");
    for (int r = 0; r < ROUNDS; r++) {
        enum { MAXOPS = 3 };
        int n = 1 + (int)(rnd(&w->rng) % MAXOPS), ps[MAXOPS], kind[MAXOPS], kj[MAXOPS], val[MAXOPS], used = 0; char sql[1500]; int off = 0;
        off += snprintf(sql + off, sizeof sql - (size_t)off, "BEGIN;");
        for (int i = 0; i < n; i++) {
            int p, dup; do { p = (int)(rnd(&w->rng) % (PARENTS / NT)) * NT + w->id; dup = 0; for (int j = 0; j < used; j++) if (ps[j] == p) dup = 1; } while (dup);
            ps[used] = p; kj[used] = (int)(rnd(&w->rng) % KIDS); val[used] = (int)(rnd(&w->rng) % 100000);
            int pick = (int)(rnd(&w->rng) % 10);
            if (!w->par[p]) kind[used] = 0;                                              // insert the parent
            else if (pick == 0) kind[used] = 1;                                          // delete it (and its children)
            else if (!w->kid[p][kj[used]]) kind[used] = 2;                               // insert a child
            else kind[used] = pick < 6 ? 3 : 4;                                          // update or delete a child
            int cid = p * KIDS + kj[used];
            switch (kind[used]) {
                case 0: off += snprintf(sql + off, sizeof sql - (size_t)off, "INSERT INTO p VALUES(%d, %d);", p, val[used]); break;
                case 1: off += snprintf(sql + off, sizeof sql - (size_t)off, "DELETE FROM p WHERE id = %d;", p); break;
                case 2: off += snprintf(sql + off, sizeof sql - (size_t)off, "INSERT INTO c VALUES(%d, %d, %d);", cid, p, val[used]); break;
                case 3: off += snprintf(sql + off, sizeof sql - (size_t)off, "UPDATE c SET v = %d WHERE id = %d;", val[used], cid); break;
                default: off += snprintf(sql + off, sizeof sql - (size_t)off, "DELETE FROM c WHERE id = %d;", cid); break;
            }
            used++;
        }
        snprintf(sql + off, sizeof sql - (size_t)off, "COMMIT");
        int rc = mw_exec(db, sql);
        if (rc == SQLITE_OK) {
            w->committed++;
            for (int i = 0; i < used; i++) {
                int p = ps[i], j = kj[i];
                if (kind[i] == 0) w->par[p] = 1;
                else if (kind[i] == 1) { w->par[p] = 0; memset(w->kid[p], 0, KIDS); }
                else if (kind[i] == 2) { w->kid[p][j] = 1; w->val[p][j] = val[i]; }
                else if (kind[i] == 3) w->val[p][j] = val[i];
                else w->kid[p][j] = 0;
            }
        } else { w->refused++; if ((rc & 0xff) != SQLITE_BUSY) { printf("unexpected rc=%d: %s\n", rc, sqlite3_errmsg(db)); mw_failures++; } mw_exec(db, "ROLLBACK"); }
    }
    { mw_db_stats ds; memset(&ds, 0, sizeof ds); sqlite3_file_control(db, "main", MW_FCNTL_DBSTATS, &ds); if (w->id == 0) g_rebases = ds.rebases; }
    sqlite3_close(db); return NULL;
}

int main (void) {
    char path[256]; mw_tmpdb(path, sizeof path, "rebasefkstress"); g_path = path;
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE p(id INTEGER PRIMARY KEY, x INTEGER); CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER NOT NULL REFERENCES p(id) ON DELETE CASCADE, v INTEGER)"), SQLITE_OK);
    sqlite3_close(s);
    static worker_t w[NT]; pthread_t th[NT];
    for (int i = 0; i < NT; i++) { w[i].id = i; w[i].rng = 99u + 1013u * (unsigned)i; pthread_create(&th[i], NULL, work, &w[i]); }
    for (int i = 0; i < NT; i++) pthread_join(th[i], NULL);
    sqlite3 *c; CHECK_RC(sqlite3_open_v2(path, &c, SQLITE_OPEN_READWRITE, MW_PLAIN_VFS), SQLITE_OK);
    long prows = 0, crows = 0, wrong = 0, ep = 0, ec = 0, committed = 0, refused = 0;
    for (int i = 0; i < NT; i++) { committed += w[i].committed; refused += w[i].refused; for (int p = 0; p < PARENTS; p++) { if (p % NT != i) continue; if (w[i].par[p]) ep++; for (int j = 0; j < KIDS; j++) if (w[i].kid[p][j]) ec++; } }
    sqlite3_stmt *st; sqlite3_prepare_v2(c, "SELECT id, x FROM p", -1, &st, NULL);
    while (sqlite3_step(st) == SQLITE_ROW) { prows++; int p = sqlite3_column_int(st, 0); if (!w[p % NT].par[p]) wrong++; }
    sqlite3_finalize(st);
    sqlite3_prepare_v2(c, "SELECT id, pid, v FROM c", -1, &st, NULL);
    while (sqlite3_step(st) == SQLITE_ROW) { crows++; int id = sqlite3_column_int(st, 0), p = sqlite3_column_int(st, 1), j = id % KIDS; if (id / KIDS != p || !w[p % NT].kid[p][j] || w[p % NT].val[p][j] != sqlite3_column_int(st, 2)) { if (wrong++ < 5) printf("child %d differs\n", id); } }
    sqlite3_finalize(st);
    int ok = 0; sqlite3_prepare_v2(c, "PRAGMA integrity_check", -1, &st, NULL); if (sqlite3_step(st) == SQLITE_ROW) ok = strcmp((const char *)sqlite3_column_text(st, 0), "ok") == 0; sqlite3_finalize(st);
    long broken = mw_scalar(c, "SELECT count(*) FROM pragma_foreign_key_check");
    printf("rebase with foreign keys: rebases %llu; %ld transactions committed, %ld refused; parents %ld (model %ld), children %ld (model %ld), wrong %ld, foreign keys broken %ld; integrity %s\n",
           (unsigned long long)g_rebases, committed, refused, prows, ep, crows, ec, wrong, broken, ok ? "ok" : "BAD");
    CHECK(prows == ep); CHECK(crows == ec); CHECK(wrong == 0); CHECK(broken == 0); CHECK(ok); CHECK(g_rebases > 0);
    sqlite3_close(c); mw_rmdb(path);
    MW_DONE();
}
