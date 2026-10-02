// Differential test of the CRDT core against sqlite-sync. Three peers run random local operations (insert, update of one to three columns, delete, re-insert) on a real
// sqlite-sync database, one statement per transaction: after every operation our local generation, fed with the same operation, must produce exactly the metadata
// sqlite-sync's triggers wrote (column versions, db_versions). Then the changes of the peers are merged, in random order, with duplicates and partial deliveries, into a
// real sqlite-sync replica and into our merge on a state of our own: the base table (values) and the metadata (column versions, sites) must be identical afterwards.
#include <stdint.h>
#include "mw_test.h"
#include "crdt.h"

extern int sqlite3_cloudsync_init (sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi);
static uint64_t rng = 0x2545F4914F6CDD1Dull;
static uint64_t rnd (void) { rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17; return rng; }

// ---- our state: a flat table of cells, a base table of rows, a site table ----
typedef struct { char pk[48]; size_t pklen; uint32_t col; crdt_cell c; bool live; } cellrec;
typedef struct { char pk[48]; size_t pklen; bool exists; bool has[3]; char a[24]; int64_t b; double cc; int types[3]; } rowrec;     // a TEXT, b INTEGER, c REAL (NULL when unset)
typedef struct { cellrec *cells; int ncells, capc; rowrec *rows; int nrows, caprows; uint8_t sites[8][16]; int nsites; uint8_t self[16]; } sim;

static cellrec *cell_find (sim *s, const void *pk, size_t pklen, uint32_t col) { for (int i = 0; i < s->ncells; i++) if (s->cells[i].live && s->cells[i].col == col && s->cells[i].pklen == pklen && !memcmp(s->cells[i].pk, pk, pklen)) return &s->cells[i]; return NULL; }
static rowrec *row_find (sim *s, const void *pk, size_t pklen, bool create) {
    for (int i = 0; i < s->nrows; i++) if (s->rows[i].pklen == pklen && !memcmp(s->rows[i].pk, pk, pklen)) return &s->rows[i];
    if (!create) return NULL;
    if (s->nrows == s->caprows) { s->caprows = s->caprows ? s->caprows * 2 : 64; s->rows = realloc(s->rows, (size_t)s->caprows * sizeof *s->rows); }
    rowrec *r = &s->rows[s->nrows++]; memset(r, 0, sizeof *r); memcpy(r->pk, pk, pklen); r->pklen = pklen; return r;
}
static bool o_get (void *st, uint32_t t, const void *pk, size_t pl, uint32_t col, crdt_cell *out) { cellrec *c = cell_find(st, pk, pl, col); if (!c) return false; *out = c->c; return true; }
static void o_put (void *st, uint32_t t, const void *pk, size_t pl, uint32_t col, const crdt_cell *c) {
    sim *s = st; cellrec *x = cell_find(s, pk, pl, col);
    if (!x) { if (s->ncells == s->capc) { s->capc = s->capc ? s->capc * 2 : 256; s->cells = realloc(s->cells, (size_t)s->capc * sizeof *s->cells); } x = &s->cells[s->ncells++]; memset(x, 0, sizeof *x); memcpy(x->pk, pk, pl); x->pklen = pl; x->col = col; x->live = true; }
    x->c = *c;
}
static void o_drop (void *st, uint32_t t, const void *pk, size_t pl) { sim *s = st; for (int i = 0; i < s->ncells; i++) if (s->cells[i].live && s->cells[i].col != CRDT_COL_SENTINEL && s->cells[i].pklen == pl && !memcmp(s->cells[i].pk, pk, pl)) s->cells[i].live = false; }
static void o_zero (void *st, uint32_t t, const void *pk, size_t pl, int64_t dv) { sim *s = st; for (int i = 0; i < s->ncells; i++) if (s->cells[i].live && s->cells[i].col != CRDT_COL_SENTINEL && s->cells[i].pklen == pl && !memcmp(s->cells[i].pk, pk, pl)) { s->cells[i].c.cv = 0; s->cells[i].c.dv = dv; } }
static bool o_known (void *st, uint32_t t, const void *pk, size_t pl) { sim *s = st; for (int i = 0; i < s->ncells; i++) if (s->cells[i].live && s->cells[i].pklen == pl && !memcmp(s->cells[i].pk, pk, pl)) return true; return false; }
static bool o_value (void *st, uint32_t t, const void *pk, size_t pl, uint32_t col, crdt_value *out) {
    sim *s = st; rowrec *r = row_find(s, pk, pl, false); if (!r || !r->exists || !r->has[col]) return false;
    if (col == 0) *out = (crdt_value){ CRDT_TEXT, 0, 0, r->a, strlen(r->a) }; else if (col == 1) *out = (crdt_value){ CRDT_INTEGER, r->b, 0, NULL, 0 }; else *out = (crdt_value){ CRDT_FLOAT, 0, r->cc, NULL, 0 };
    if (r->types[col] == CRDT_NULL) *out = (crdt_value){ CRDT_NULL, 0, 0, NULL, 0 };
    return true;
}
static uint32_t o_site (void *st, const uint8_t site[16]) { sim *s = st; if (!memcmp(site, s->self, 16)) return 0; for (int i = 0; i < s->nsites; i++) if (!memcmp(s->sites[i], site, 16)) return (uint32_t)i + 1; memcpy(s->sites[s->nsites], site, 16); return (uint32_t)(++s->nsites); }
static bool o_sbytes (void *st, uint32_t ord, uint8_t out[16]) { sim *s = st; if (ord == 0) { memcpy(out, s->self, 16); return true; } if (ord > (uint32_t)s->nsites) return false; memcpy(out, s->sites[ord - 1], 16); return true; }
static const crdt_ops OPS = { o_get, o_put, o_drop, o_zero, o_known, o_value, o_site, o_sbytes };

// ---- helpers on the oracle ----
static sqlite3 *peer (const char *schema_extra) {
    sqlite3 *db; sqlite3_open(":memory:", &db); sqlite3_cloudsync_init(db, NULL, NULL);
    CHECK_RC(mw_exec(db, "CREATE TABLE t(id TEXT PRIMARY KEY NOT NULL, a TEXT, b INTEGER, c REAL)"), SQLITE_OK);
    CHECK_RC(mw_exec(db, "SELECT cloudsync_init('t')"), SQLITE_OK);
    (void)schema_extra; return db;
}
static int cmp_s (const void *a, const void *b) { return strcmp(*(char *const *)a, *(char *const *)b); }
static char *enc (const char *key, size_t *n) { crdt_value v = { CRDT_TEXT, 0, 0, key, strlen(key) }; *n = crdt_pk_encode(&v, 1, NULL, 0); uint8_t *b = malloc(*n); crdt_pk_encode(&v, 1, b, *n); return (char *)b; }
static void hex (const uint8_t *b, size_t n, char *o) { for (size_t i = 0; i < n; i++) sprintf(o + 2 * i, "%02x", b[i]); }

// the oracle's metadata as sorted strings "pk|col|cv|dv|site"; with_dv = false leaves the db_version out
static int dump_oracle (sqlite3 *db, char **out, int max, bool with_dv) {
    sqlite3_stmt *st; int n = 0;
    sqlite3_prepare_v2(db, "SELECT hex(m.pk), m.col_name, m.col_version, m.db_version, CASE WHEN m.site_id = 0 THEN 'SELF' ELSE hex((SELECT site_id FROM cloudsync_site_id WHERE rowid = m.site_id)) END FROM t_cloudsync m", -1, &st, NULL);
    while (sqlite3_step(st) == SQLITE_ROW && n < max) {
        const char *col = (const char *)sqlite3_column_text(st, 1); int cn = !strcmp(col, "a") ? 0 : !strcmp(col, "b") ? 1 : !strcmp(col, "c") ? 2 : 9;
        char buf[200]; snprintf(buf, sizeof buf, "%s|%d|%lld|%lld|%s", sqlite3_column_text(st, 0), cn, (long long)sqlite3_column_int64(st, 2), with_dv ? (long long)sqlite3_column_int64(st, 3) : 0LL, sqlite3_column_text(st, 4));
        out[n++] = strdup(buf);
    }
    sqlite3_finalize(st); qsort(out, (size_t)n, sizeof *out, cmp_s); return n;
}
static int dump_ours (sim *s, char **out, int max, bool with_dv) {
    int n = 0;
    for (int i = 0; i < s->ncells && n < max; i++) {
        cellrec *c = &s->cells[i]; if (!c->live) continue;
        char h[100], sb[40] = "", buf[220]; hex((const uint8_t *)c->pk, c->pklen, h); uint8_t b16[16];
        if (c->c.site) { o_sbytes(s, c->c.site, b16); hex(b16, 16, sb); } else strcpy(sb, "SELF");
        for (char *p = h; *p; p++) if (*p >= 'a' && *p <= 'f') *p -= 32; for (char *p = sb; *p; p++) if (*p >= 'a' && *p <= 'f') *p -= 32;
        int cn = c->col == CRDT_COL_SENTINEL ? 9 : (int)c->col;
        snprintf(buf, sizeof buf, "%s|%d|%lld|%lld|%s", h, cn, (long long)c->c.cv, with_dv ? (long long)c->c.dv : 0LL, sb);
        out[n++] = strdup(buf);
    }
    qsort(out, (size_t)n, sizeof *out, cmp_s); return n;
}
static bool same (char **a, int na, char **b, int nb, const char *what) {
    bool ok = na == nb;
    for (int i = 0; ok && i < na; i++) if (strcmp(a[i], b[i])) ok = false;
    if (!ok) { printf("  %s differ: oracle %d entries, ours %d\n", what, na, nb); for (int i = 0, shown = 0; i < na && shown < 6; i++) { bool f = false; for (int j = 0; j < nb; j++) if (!strcmp(a[i], b[j])) f = true; if (!f) { printf("    only in sqlite-sync: %s\n", a[i]); shown++; } }
        for (int j = 0, shown = 0; j < nb && shown < 6; j++) { bool f = false; for (int i = 0; i < na; i++) if (!strcmp(a[i], b[j])) f = true; if (!f) { printf("    only in ours:        %s\n", b[j]); shown++; } } }
    return ok;
}
static void freeall (char **a, int n) { for (int i = 0; i < n; i++) free(a[i]); }

static const char *KEYS[] = { "k1", "k2", "k3", "k4", "k5", "k6" };
static const char *SITE_HEX = "00112233445566778899aabbccddeeff";

int main (void) {
    // ---------------- 1. local generation ----------------
    long ops_done = 0, bad = 0;
    for (int round = 0; round < 100; round++) {
        sqlite3 *p = peer(NULL); sim S; memset(&S, 0, sizeof S);
        sqlite3_stmt *sid; sqlite3_prepare_v2(p, "SELECT cloudsync_siteid()", -1, &sid, NULL); sqlite3_step(sid); memcpy(S.self, sqlite3_column_blob(sid, 0), 16); sqlite3_finalize(sid);
        int64_t seq = 0; bool alive[6] = {0}, ever[6] = {0};
        for (int op = 0; op < 60; op++) {
            int k = (int)(rnd() % 6); char sql[256]; size_t pl; char *pk = enc(KEYS[k], &pl); crdt_wcell out[8];
            int what = (int)(rnd() % 10); int64_t dv0 = mw_scalar(p, "SELECT cloudsync_db_version()");
            if (!alive[k]) {                                                                         // insert (a first insert, or a re-insert after a delete)
                snprintf(sql, sizeof sql, "INSERT INTO t(id,a,b,c) VALUES('%s','v%d',%d,%d.5)", KEYS[k], op, op, op);
                CHECK_RC(mw_exec(p, sql), SQLITE_OK); int64_t dv = mw_scalar(p, "SELECT cloudsync_db_version()"); CHECK(dv > dv0);
                crdt_local_insert(&OPS, &S, 0, pk, pl, NULL, 3, dv, &seq, out, 8); alive[k] = ever[k] = true;
            } else if (what < 7) {                                                                    // update 1-3 columns
                uint32_t cols[3]; int nc = 0; char set[120] = "";
                for (uint32_t c = 0; c < 3; c++) if (rnd() % 2 || (c == 2 && nc == 0)) { cols[nc++] = c; char one[40]; snprintf(one, sizeof one, "%s%c=%s%d%s", nc > 1 ? "," : "", "abc"[c], c == 0 ? "'u" : "", op + 1000, c == 0 ? "'" : c == 2 ? ".25" : ""); strcat(set, one); }
                snprintf(sql, sizeof sql, "UPDATE t SET %s WHERE id='%s'", set, KEYS[k]);
                CHECK_RC(mw_exec(p, sql), SQLITE_OK); int64_t dv = mw_scalar(p, "SELECT cloudsync_db_version()");
                crdt_local_update(&OPS, &S, 0, pk, pl, cols, nc, dv, &seq, out, 8);
            } else {                                                                                  // delete
                snprintf(sql, sizeof sql, "DELETE FROM t WHERE id='%s'", KEYS[k]);
                CHECK_RC(mw_exec(p, sql), SQLITE_OK); int64_t dv = mw_scalar(p, "SELECT cloudsync_db_version()");
                crdt_local_delete(&OPS, &S, 0, pk, pl, dv, &seq, out, 8); alive[k] = false;
            }
            free(pk); ops_done++;
            char *a[200], *b[200]; int na = dump_oracle(p, a, 200, true), nb = dump_ours(&S, b, 200, true);
            if (!same(a, na, b, nb, "local metadata")) { bad++; printf("  (round %d, operation %d)\n", round, op); freeall(a, na); freeall(b, nb); goto local_done; }
            freeall(a, na); freeall(b, nb);
        }
        sqlite3_close(p); free(S.cells); free(S.rows);
    }
local_done:
    printf("local generation: %ld operations compared with sqlite-sync's triggers, %ld divergences\n", ops_done, bad);
    CHECK(bad == 0);

    // ---------------- 2. merge ----------------
    long delivered = 0, mbad = 0;
    for (int round = 0; round < 300 && !mbad; round++) {
        sqlite3 *P[3]; sim PS[3]; int64_t pseq[3] = {0, 0, 0}; bool alive[3][6]; memset(alive, 0, sizeof alive);
        for (int i = 0; i < 3; i++) { P[i] = peer(NULL); memset(&PS[i], 0, sizeof PS[i]); sqlite3_stmt *q; sqlite3_prepare_v2(P[i], "SELECT cloudsync_siteid()", -1, &q, NULL); sqlite3_step(q); memcpy(PS[i].self, sqlite3_column_blob(q, 0), 16); sqlite3_finalize(q); }
        sqlite3 *R = peer(NULL); sim RS; memset(&RS, 0, sizeof RS); bool ralive[6] = {0}; int64_t rseq = 0;
        { sqlite3_stmt *q; sqlite3_prepare_v2(R, "SELECT cloudsync_siteid()", -1, &q, NULL); sqlite3_step(q); memcpy(RS.self, sqlite3_column_blob(q, 0), 16); sqlite3_finalize(q); }
        for (int step = 0; step < 50 && !mbad; step++) {
            int act = (int)(rnd() % 10);
            if (act < 5) {                                                                            // a local operation on one of the peers (tracked by our own state too: the changes must agree)
                int w = (int)(rnd() % 3); int k = (int)(rnd() % 6); char sql[256]; size_t pl; char *pk = enc(KEYS[k], &pl); crdt_wcell out[8]; int64_t dv0 = mw_scalar(P[w], "SELECT cloudsync_db_version()");
                if (!alive[w][k]) { snprintf(sql, sizeof sql, "INSERT INTO t(id,a,b,c) VALUES('%s','p%d_%d',%d,%d.5)", KEYS[k], w, step, step + w, step); mw_exec(P[w], sql); crdt_local_insert(&OPS, &PS[w], 0, pk, pl, NULL, 3, mw_scalar(P[w], "SELECT cloudsync_db_version()"), &pseq[w], out, 8); alive[w][k] = true; }
                else if (rnd() % 4) { uint32_t cols[3]; int nc = 0; char set[120] = ""; for (uint32_t c = 0; c < 3; c++) if (rnd() % 2 || (c == 2 && nc == 0)) { cols[nc++] = c; char one[48]; snprintf(one, sizeof one, "%s%c=%s%d%s", nc > 1 ? "," : "", "abc"[c], c == 0 ? "'q" : "", step * 10 + w, c == 0 ? "'" : c == 2 ? ".75" : ""); strcat(set, one); }
                    snprintf(sql, sizeof sql, "UPDATE t SET %s WHERE id='%s'", set, KEYS[k]); mw_exec(P[w], sql); crdt_local_update(&OPS, &PS[w], 0, pk, pl, cols, nc, mw_scalar(P[w], "SELECT cloudsync_db_version()"), &pseq[w], out, 8); }
                else { snprintf(sql, sizeof sql, "DELETE FROM t WHERE id='%s'", KEYS[k]); mw_exec(P[w], sql); crdt_local_delete(&OPS, &PS[w], 0, pk, pl, mw_scalar(P[w], "SELECT cloudsync_db_version()"), &pseq[w], out, 8); alive[w][k] = false; }
                (void)dv0; free(pk);
            } else if (act < 6) {                                                                     // a local operation on the replica itself
                int k = (int)(rnd() % 6); char sql[256]; size_t pl; char *pk = enc(KEYS[k], &pl); crdt_wcell out[8];
                rowrec *rr = row_find(&RS, pk, pl, false); bool exists = rr && rr->exists;
                if (!exists) { snprintf(sql, sizeof sql, "INSERT INTO t(id,a,b,c) VALUES('%s','r%d',%d,%d.5)", KEYS[k], step, step, step); mw_exec(R, sql);
                    crdt_local_insert(&OPS, &RS, 0, pk, pl, NULL, 3, mw_scalar(R, "SELECT cloudsync_db_version()"), &rseq, out, 8);
                    rr = row_find(&RS, pk, pl, true); rr->exists = true; for (int c = 0; c < 3; c++) { rr->has[c] = true; } snprintf(rr->a, sizeof rr->a, "r%d", step); rr->b = step; rr->cc = step + 0.5; rr->types[0] = CRDT_TEXT; rr->types[1] = CRDT_INTEGER; rr->types[2] = CRDT_FLOAT; }
                else { snprintf(sql, sizeof sql, "UPDATE t SET b=%d WHERE id='%s'", step + 500, KEYS[k]); mw_exec(R, sql); uint32_t cols[1] = { 1 };
                    crdt_local_update(&OPS, &RS, 0, pk, pl, cols, 1, mw_scalar(R, "SELECT cloudsync_db_version()"), &rseq, out, 8); rr->b = step + 500; rr->types[1] = CRDT_INTEGER; rr->has[1] = true; }
                free(pk); ralive[k] = true;
            } else {                                                                                  // a delivery: some changes of a peer, in random order, some of them twice, into the replica and into ours
                int w = (int)(rnd() % 3); sqlite3_stmt *q, *ins;
                sqlite3_prepare_v2(P[w], "SELECT tbl, pk, col_name, col_value, col_version, db_version, site_id, cl, seq FROM cloudsync_changes ORDER BY db_version, seq", -1, &q, NULL);
                sqlite3_prepare_v2(R, "INSERT INTO cloudsync_changes(tbl, pk, col_name, col_value, col_version, db_version, site_id, cl, seq) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)", -1, &ins, NULL);
                sqlite3_value *rows[400][9]; int nr = 0;
                while (sqlite3_step(q) == SQLITE_ROW && nr < 400) { if (rnd() % 3 == 0) continue; for (int c = 0; c < 9; c++) rows[nr][c] = sqlite3_value_dup(sqlite3_column_value(q, c)); nr++; if (rnd() % 5 == 0 && nr < 399) { for (int c = 0; c < 9; c++) rows[nr][c] = sqlite3_value_dup(rows[nr - 1][c]); nr++; } }
                for (int i = nr - 1; i > 0; i--) { int j = (int)(rnd() % (uint64_t)(i + 1)); for (int c = 0; c < 9; c++) { sqlite3_value *t = rows[i][c]; rows[i][c] = rows[j][c]; rows[j][c] = t; } }
                for (int i = 0; i < nr; i++) {
                    for (int c = 0; c < 9; c++) sqlite3_bind_value(ins, c + 1, rows[i][c]);
                    int rc = sqlite3_step(ins); if (rc != SQLITE_DONE) { printf("  oracle merge failed rc=%d %s\n", rc, sqlite3_errmsg(R)); mbad++; } sqlite3_reset(ins);
                    // ours
                    const char *cn = (const char *)sqlite3_value_text(rows[i][2]); uint32_t col = !strcmp(cn, CRDT_SENTINEL) ? CRDT_COL_SENTINEL : !strcmp(cn, "a") ? 0 : !strcmp(cn, "b") ? 1 : 2;
                    crdt_value val; int vt = sqlite3_value_type(rows[i][3]);
                    if (vt == SQLITE_INTEGER) val = (crdt_value){ CRDT_INTEGER, sqlite3_value_int64(rows[i][3]), 0, NULL, 0 }; else if (vt == SQLITE_FLOAT) val = (crdt_value){ CRDT_FLOAT, 0, sqlite3_value_double(rows[i][3]), NULL, 0 };
                    else if (vt == SQLITE_TEXT) val = (crdt_value){ CRDT_TEXT, 0, 0, sqlite3_value_text(rows[i][3]), (size_t)sqlite3_value_bytes(rows[i][3]) }; else val = (crdt_value){ CRDT_NULL, 0, 0, NULL, 0 };
                    crdt_change ch = { 0, sqlite3_value_blob(rows[i][1]), (size_t)sqlite3_value_bytes(rows[i][1]), col, &val, sqlite3_value_int64(rows[i][4]), sqlite3_value_int64(rows[i][7]), sqlite3_value_int64(rows[i][5]), sqlite3_value_int64(rows[i][8]), {0} };
                    memcpy(ch.site, sqlite3_value_blob(rows[i][6]), 16);
                    int64_t maxdv = 0; for (int x = 0; x < RS.ncells; x++) if (RS.cells[x].live && RS.cells[x].c.dv > maxdv) maxdv = RS.cells[x].c.dv;
                    int64_t stored = maxdv + 1 > ch.dv ? maxdv + 1 : ch.dv;
                    crdt_actions act; crdt_merge(&OPS, &RS, &ch, stored, false, &act);
                    rowrec *rr = row_find(&RS, ch.pk, ch.pklen, act.n > 0);
                    for (int a = 0; a < act.n; a++) {
                        if (act.kind[a] == CRDT_ACT_INSERT_ROW) { if (!rr->exists) { rr->exists = true; memset(rr->has, 0, sizeof rr->has); } }
                        else if (act.kind[a] == CRDT_ACT_DELETE_ROW) { rr->exists = false; memset(rr->has, 0, sizeof rr->has); }
                        else { if (!rr->exists) { rr->exists = true; memset(rr->has, 0, sizeof rr->has); }
                               rr->has[act.col] = true; rr->types[act.col] = val.type;
                               if (act.col == 0) { snprintf(rr->a, sizeof rr->a, "%.*s", (int)val.n, (const char *)val.p); } else if (act.col == 1) rr->b = val.i; else rr->cc = val.d; }
                    }
                    delivered++;
                }
                for (int i = 0; i < nr; i++) for (int c = 0; c < 9; c++) sqlite3_value_free(rows[i][c]);
                sqlite3_finalize(q); sqlite3_finalize(ins);
            }
            // compare the replica with ours: metadata and base table
            char *a[300], *b[300]; int na = dump_oracle(R, a, 300, true), nb = dump_ours(&RS, b, 300, true);
            bool ok = same(a, na, b, nb, "merge metadata");
            if (ok) {   // base table
                sqlite3_stmt *q; sqlite3_prepare_v2(R, "SELECT id, a, b, c FROM t ORDER BY id", -1, &q, NULL); int nrows = 0;
                while (sqlite3_step(q) == SQLITE_ROW) {
                    size_t pl; char *pk = enc((const char *)sqlite3_column_text(q, 0), &pl); rowrec *rr = row_find(&RS, pk, pl, false); free(pk); nrows++;
                    if (!rr || !rr->exists) { printf("  row %s exists in sqlite-sync only\n", sqlite3_column_text(q, 0)); ok = false; break; }
                    for (int c = 0; c < 3 && ok; c++) {
                        int t = sqlite3_column_type(q, c + 1); int ot = rr->has[c] ? rr->types[c] : CRDT_NULL;
                        if ((t == SQLITE_NULL) != (ot == CRDT_NULL)) ok = false;
                        else if (c == 0 && t == SQLITE_TEXT && strcmp((const char *)sqlite3_column_text(q, 1), rr->a)) ok = false;
                        else if (c == 1 && t == SQLITE_INTEGER && sqlite3_column_int64(q, 2) != rr->b) ok = false;
                        else if (c == 2 && t == SQLITE_FLOAT && sqlite3_column_double(q, 3) != rr->cc) ok = false;
                        else if (t != SQLITE_NULL && ot != t) ok = false;
                        if (!ok) printf("  row %s column %d differs\n", sqlite3_column_text(q, 0), c);
                    }
                    if (!ok) break;
                }
                sqlite3_finalize(q);
                int ours = 0; for (int i = 0; i < RS.nrows; i++) if (RS.rows[i].exists) ours++;
                if (ok && ours != nrows) { printf("  %d rows in sqlite-sync, %d in ours\n", nrows, ours); ok = false; }
            }
            freeall(a, na); freeall(b, nb);
            if (!ok) { mbad++; printf("  (round %d, step %d, action %d)\n", round, step, act); }
        }
        for (int i = 0; i < 3; i++) { sqlite3_close(P[i]); free(PS[i].cells); free(PS[i].rows); }
        sqlite3_close(R); free(RS.cells); free(RS.rows);
    }
    printf("merge: %ld changes delivered (random order, duplicates, partial) to a sqlite-sync replica and to ours, %ld divergences\n", delivered, mbad);
    CHECK(mbad == 0);
    MW_DONE();
}
