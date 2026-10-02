// Interoperability with sqlite-sync at the wire: a real sqlite-sync peer (O) and a multi-writer peer (P) make random concurrent edits to the same rows, then exchange payloads through
// the real payload encoder / decoder of sqlite-sync on one side and ours on the other. After every exchange both must hold the same rows and the same cell versions.
#include <stdint.h>
#include <stdbool.h>
#include "mw_test.h"
#include "multiwriter.h"
#include "multiwriter_meta.h"
#include "multiwriter_catalog.h"
#include "multiwriter_sync.h"
#include "crdt.h"

extern int sqlite3_cloudsync_init (sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi);
static uint64_t rng = 0x9E3779B97F4A7C15ull;
static uint64_t rnd (void) { rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17; return rng; }
#define NK 10

static int cmp_s (const void *a, const void *b) { return strcmp(*(char *const *)a, *(char *const *)b); }
static int rows_of (sqlite3 *db, char **out) {
    sqlite3_stmt *st; int n = 0;
    sqlite3_prepare_v2(db, "SELECT id, quote(a), quote(b), quote(c) FROM t ORDER BY id", -1, &st, NULL);
    while (sqlite3_step(st) == SQLITE_ROW && n < 64) { char b[300]; snprintf(b, sizeof b, "%s|%s|%s|%s", sqlite3_column_text(st, 0), sqlite3_column_text(st, 1), sqlite3_column_text(st, 2), sqlite3_column_text(st, 3)); out[n++] = strdup(b); }
    sqlite3_finalize(st); return n;
}
static int cells_oracle (sqlite3 *o, char **out) {
    sqlite3_stmt *st; int n = 0;
    sqlite3_prepare_v2(o, "SELECT hex(pk), col_name, col_version FROM t_cloudsync", -1, &st, NULL);
    while (sqlite3_step(st) == SQLITE_ROW && n < 400) { char b[200]; snprintf(b, sizeof b, "%s|%s|%lld", sqlite3_column_text(st, 0), sqlite3_column_text(st, 1), (long long)sqlite3_column_int64(st, 2)); out[n++] = strdup(b); }
    sqlite3_finalize(st); qsort(out, (size_t)n, sizeof *out, cmp_s); return n;
}
static int cells_ours (sqlite3 *p, mw_meta *m, char **out) {
    int n = 0; const char *cn[] = { "a", "b", "c" };
    for (int k = 0; k < NK; k++) {
        char key[16]; snprintf(key, sizeof key, "k%d", k); crdt_value v = { CRDT_TEXT, 0, 0, key, strlen(key) }; uint8_t pk[40]; size_t pl = crdt_pk_encode(&v, 1, pk, sizeof pk);
        mw_mcell *c; int nc; mw_meta_row(m, mw_name_id("t"), pk, pl, &c, &nc);
        char hx[100]; for (size_t i = 0; i < pl; i++) sprintf(hx + 2 * i, "%02X", pk[i]);
        for (int i = 0; i < nc; i++) {
            const char *name = c[i].col == CRDT_COL_SENTINEL ? CRDT_SENTINEL : NULL; for (int q = 0; q < 3 && !name; q++) if (c[i].col == mw_name_id(cn[q])) name = cn[q];
            char b[200]; snprintf(b, sizeof b, "%s|%s|%lld", hx, name ? name : "?", (long long)c[i].cv); out[n++] = strdup(b);
        }
        free(c);
    }
    (void)p; qsort(out, (size_t)n, sizeof *out, cmp_s); return n;
}
static void freeall (char **a, int n) { for (int i = 0; i < n; i++) free(a[i]); }

static bool alive_o[NK], alive_p[NK];
static void random_edit (sqlite3 *db, bool *alive, const char *who, int step) {
    int k = (int)(rnd() % NK); char sql[300];
    int what = (int)(rnd() % 10);
    if (!alive[k]) { snprintf(sql, sizeof sql, "INSERT OR REPLACE INTO t(id,a,b,c) VALUES('k%d','%s%d',%d,%d.25)", k, who, step, step, step); alive[k] = true; }
    else if (what == 0) { snprintf(sql, sizeof sql, "DELETE FROM t WHERE id='k%d'", k); alive[k] = false; }
    else if (what < 4) snprintf(sql, sizeof sql, "UPDATE t SET a='%s%d' WHERE id='k%d'", who, step, k);
    else if (what < 7) snprintf(sql, sizeof sql, "UPDATE t SET b=%d WHERE id='k%d'", step * 7 + (int)(rnd() % 3), k);
    else snprintf(sql, sizeof sql, "UPDATE t SET a='%s%d', c=%d.5 WHERE id='k%d'", who, step, step, k);
    CHECK_RC(mw_exec(db, sql), SQLITE_OK);
}
static void resync_alive (sqlite3 *db, bool *alive) { for (int k = 0; k < NK; k++) { char q[100]; snprintf(q, sizeof q, "SELECT count(*) FROM t WHERE id='k%d'", k); alive[k] = mw_scalar(db, q) > 0; } }

int main (void) {
    char path[256]; mw_tmpdb(path, sizeof path, "osync");
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id TEXT PRIMARY KEY NOT NULL, a TEXT, b INTEGER, c REAL)"), SQLITE_OK); sqlite3_close(s);
    char uri[300]; snprintf(uri, sizeof uri, "file:%s?mw=2&mw_cdc=1%s", path, getenv("MW_TEST_MP") ? "&mw_mp=1" : "");
    sqlite3 *p; CHECK_RC(sqlite3_open_v2(uri, &p, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    mw_meta *m = NULL; CHECK_RC(sqlite3_file_control(p, "main", MW_FCNTL_META, &m), SQLITE_OK);
    sqlite3 *o; sqlite3_open(":memory:", &o); sqlite3_cloudsync_init(o, NULL, NULL);
    CHECK_RC(mw_exec(o, "CREATE TABLE t(id TEXT PRIMARY KEY NOT NULL, a TEXT, b INTEGER, c REAL)"), SQLITE_OK);
    CHECK_RC(mw_exec(o, "SELECT cloudsync_init('t')"), SQLITE_OK);

    int64_t p_since = 0, o_since = 0; long bad = 0, rounds = 0, o_to_p = 0, p_to_o = 0; mw_sync_stats ps;
    for (int round = 0; round < 300 && bad < 3; round++) {
        int ne = 1 + (int)(rnd() % 6);
        for (int i = 0; i < ne; i++) random_edit(o, alive_o, "o", round * 10 + i);
        int ne2 = 1 + (int)(rnd() % 6);
        for (int i = 0; i < ne2; i++) random_edit(p, alive_p, "p", round * 10 + i);
        // O -> P: what O changed since last time, through sqlite-sync's encoder, into ours
        { sqlite3_stmt *st; char q[400];
          snprintf(q, sizeof q, "SELECT cloudsync_payload_encode(tbl, pk, col_name, col_value, col_version, db_version, site_id, cl, seq), max(db_version) FROM cloudsync_changes WHERE db_version > %lld AND site_id = cloudsync_siteid()", (long long)o_since);
          CHECK_RC(sqlite3_prepare_v2(o, q, -1, &st, NULL), SQLITE_OK);
          if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_type(st, 0) == SQLITE_BLOB) {
              CHECK_RC(mw_sync_apply(p, sqlite3_column_blob(st, 0), (size_t)sqlite3_column_bytes(st, 0), &ps), SQLITE_OK); o_since = sqlite3_column_int64(st, 1); o_to_p += ps.rows;
          }
          sqlite3_finalize(st); }
        // P -> O: our export, through sqlite-sync's decoder
        { uint8_t *pl; size_t n; int64_t upto; CHECK_RC(mw_sync_export(p, p_since, &pl, &n, &upto), SQLITE_OK);
          if (pl) {
              sqlite3_stmt *st; sqlite3_prepare_v2(o, "SELECT cloudsync_payload_apply(?)", -1, &st, NULL); sqlite3_bind_blob(st, 1, pl, (int)n, SQLITE_STATIC);
              int rc = sqlite3_step(st); if (rc != SQLITE_ROW) { printf("  oracle refused our payload: %s\n", sqlite3_errmsg(o)); bad++; }
              sqlite3_finalize(st); mw_sync_free(pl);
          }
          p_since = upto; }
        // and once more O -> P: the changes the previous step made at O (merges bump db_version there) are not O's own and are not re-sent: both sides now have both sets
        resync_alive(o, alive_o); resync_alive(p, alive_p);
        char *ro[64], *rp[64]; int no = rows_of(o, ro), np = rows_of(p, rp); bool same = no == np; for (int i = 0; same && i < no; i++) if (strcmp(ro[i], rp[i])) same = false;
        char *co[400], *cp[400]; int nco = cells_oracle(o, co), ncp = cells_ours(p, m, cp); bool csame = nco == ncp; for (int i = 0; csame && i < nco; i++) if (strcmp(co[i], cp[i])) csame = false;
        rounds++;
        if (!same || !csame) {
            bad++; printf("  round %d: %s differ\n", round, !same ? "rows" : "cells");
            if (!same) { printf("    sqlite-sync:"); for (int i = 0; i < no; i++) printf(" [%s]", ro[i]); printf("\n    ours:       "); for (int i = 0; i < np; i++) printf(" [%s]", rp[i]); printf("\n"); }
            else { for (int i = 0, sh = 0; i < nco && sh < 8; i++) { bool f = false; for (int j = 0; j < ncp; j++) if (!strcmp(co[i], cp[j])) f = true; if (!f) { printf("    only sqlite-sync: %s\n", co[i]); sh++; } }
                   for (int j = 0, sh = 0; j < ncp && sh < 8; j++) { bool f = false; for (int i = 0; i < nco; i++) if (!strcmp(co[i], cp[j])) f = true; if (!f) { printf("    only ours:        %s\n", cp[j]); sh++; } } }
        }
        freeall(ro, no); freeall(rp, np); freeall(co, nco); freeall(cp, ncp);
    }
    printf("sqlite-sync <-> multi-writer over the wire: %ld rounds (%ld rows from sqlite-sync, applied by us), %ld divergences\n", rounds, o_to_p, bad);
    CHECK(bad == 0);
    sqlite3_close(p); sqlite3_close(o); mw_rmdb(path);
    MW_DONE();
}
