//
//  multiwriter_sync.c: see multiwriter_sync.h.
//
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "multiwriter.h"
#include "multiwriter_meta.h"
#include "multiwriter_catalog.h"
#include "multiwriter_sync.h"
#include "crdt.h"
#include "lz4.h"

#define PAYLOAD_SIGNATURE 0x434C5359u      // 'CLSY'
#define PAYLOAD_VERSION 1
#define NCOLS 9

// ---- the schema as the sync layer needs it ----
typedef struct { char *name; uint32_t tid; int npk; char **pk; int nc; char **cell; uint32_t *cid; sqlite3_stmt **val; } syt;
typedef struct { sqlite3 *db; syt *t; int n; mw_meta *meta; } sysch;

static void sch_free (sysch *s) {
    for (int i = 0; i < s->n; i++) {
        syt *t = &s->t[i];
        for (int k = 0; k < t->npk; k++) free(t->pk[k]);
        for (int k = 0; k < t->nc; k++) { free(t->cell[k]); if (t->val) sqlite3_finalize(t->val[k]); }
        free(t->name); free(t->pk); free(t->cell); free(t->cid); free(t->val);
    }
    free(s->t); memset(s, 0, sizeof *s);
}

static int sch_load (sqlite3 *db, sysch *s) {
    memset(s, 0, sizeof *s); s->db = db;
    sqlite3_stmt *st = NULL, *pi = NULL; int cap = 0;
    if (sqlite3_prepare_v2(db, "SELECT name FROM sqlite_schema WHERE type = 'table' AND sql NOT LIKE 'CREATE VIRTUAL%' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'mw_%' ORDER BY name", -1, &st, NULL) != SQLITE_OK) return sqlite3_errcode(db);
    while (sqlite3_step(st) == SQLITE_ROW) {
        char *name = strdup((const char *)sqlite3_column_text(st, 0)); if (!name) continue;
        char *q = sqlite3_mprintf("PRAGMA table_xinfo(\"%w\")", name);
        int rc = sqlite3_prepare_v2(db, q, -1, &pi, NULL); sqlite3_free(q);
        if (rc != SQLITE_OK) { free(name); continue; }
        struct col { char *name; int pk, hidden; } cols[2048]; int nc = 0, npk = 0;
        while (sqlite3_step(pi) == SQLITE_ROW && nc < 2048) { cols[nc].name = strdup((const char *)sqlite3_column_text(pi, 1)); cols[nc].pk = sqlite3_column_int(pi, 5); cols[nc].hidden = sqlite3_column_int(pi, 6); if (cols[nc].pk > 0) npk++; nc++; }
        sqlite3_finalize(pi);
        const char *rowid_name = NULL;                                                          // a table without a primary key: its rowid is its key
        if (npk == 0) { static const char *alias[3] = { "rowid", "_rowid_", "oid" }; for (int a = 0; a < 3 && !rowid_name; a++) { bool used = false; for (int i = 0; i < nc; i++) if (!strcasecmp(cols[i].name, alias[a])) used = true; if (!used) rowid_name = alias[a]; } }
        if ((npk == 0 && !rowid_name) || npk > MW_CAT_MAXPK) { for (int i = 0; i < nc; i++) free(cols[i].name); free(name); continue; }
        if (npk == 0) npk = 1;
        if (s->n == cap) { cap = cap ? cap * 2 : 16; s->t = realloc(s->t, (size_t)cap * sizeof *s->t); }
        syt *t = &s->t[s->n++]; memset(t, 0, sizeof *t);
        t->name = name; t->tid = mw_name_id(name); t->npk = npk; t->pk = calloc((size_t)npk, sizeof(char *));
        if (rowid_name) t->pk[0] = strdup(rowid_name);
        else for (int k = 1; k <= npk; k++) for (int i = 0; i < nc; i++) if (cols[i].pk == k) t->pk[k - 1] = strdup(cols[i].name);
        t->cell = calloc((size_t)nc + 1, sizeof(char *)); t->cid = calloc((size_t)nc + 1, sizeof(uint32_t)); t->val = calloc((size_t)nc + 1, sizeof(sqlite3_stmt *));
        for (int i = 0; i < nc; i++) { if (cols[i].pk == 0 && cols[i].hidden == 0) { t->cell[t->nc] = strdup(cols[i].name); t->cid[t->nc] = mw_name_id(cols[i].name); t->nc++; } free(cols[i].name); }
    }
    sqlite3_finalize(st);
    return SQLITE_OK;
}
static syt *by_tid (sysch *s, uint32_t tid) { for (int i = 0; i < s->n; i++) if (s->t[i].tid == tid) return &s->t[i]; return NULL; }
static syt *by_name (sysch *s, const char *n, size_t len) { for (int i = 0; i < s->n; i++) if (strlen(s->t[i].name) == len && !strncasecmp(s->t[i].name, n, len)) return &s->t[i]; return NULL; }
static int cell_by_id (const syt *t, uint32_t cid) { for (int i = 0; i < t->nc; i++) if (t->cid[i] == cid) return i; return -1; }
static int cell_by_name (const syt *t, const char *n, size_t len) { for (int i = 0; i < t->nc; i++) if (strlen(t->cell[i]) == len && !strncasecmp(t->cell[i], n, len)) return i; return -1; }

// ---- values <-> SQLite ----
static void bind_value (sqlite3_stmt *st, int i, const crdt_value *v) {
    switch (v->type) {
        case CRDT_INTEGER: sqlite3_bind_int64(st, i, v->i); break;
        case CRDT_FLOAT: sqlite3_bind_double(st, i, v->d); break;
        case CRDT_TEXT: sqlite3_bind_text64(st, i, v->p ? v->p : "", v->n, SQLITE_STATIC, SQLITE_UTF8); break;
        case CRDT_BLOB: sqlite3_bind_blob64(st, i, v->p ? v->p : "", v->n, SQLITE_STATIC); break;
        default: sqlite3_bind_null(st, i);
    }
}
static crdt_value column_value (sqlite3_stmt *st, int i) {
    switch (sqlite3_column_type(st, i)) {
        case SQLITE_INTEGER: return (crdt_value){ CRDT_INTEGER, sqlite3_column_int64(st, i), 0, NULL, 0 };
        case SQLITE_FLOAT: return (crdt_value){ CRDT_FLOAT, 0, sqlite3_column_double(st, i), NULL, 0 };
        case SQLITE_TEXT: return (crdt_value){ CRDT_TEXT, 0, 0, sqlite3_column_text(st, i), (size_t)sqlite3_column_bytes(st, i) };
        case SQLITE_BLOB: return (crdt_value){ CRDT_BLOB, 0, 0, sqlite3_column_blob(st, i), (size_t)sqlite3_column_bytes(st, i) };
        default: return (crdt_value){ CRDT_NULL, 0, 0, NULL, 0 };
    }
}
static char *where_clause (const syt *t, int first) {          // "pk1" = ?N AND "pk2" = ?N+1 ...
    size_t cap = 64; for (int k = 0; k < t->npk; k++) cap += strlen(t->pk[k]) + 24;
    char *w = malloc(cap), *p = w; *p = 0;
    for (int k = 0; k < t->npk; k++) p += sprintf(p, "%s\"%s\" = ?%d", k ? " AND " : "", t->pk[k], first + k);
    return w;
}
// the current value of a cell of a row of the base table (the statement is cached per cell)
static bool base_value (sysch *s, syt *t, int cell, const void *pk, size_t pklen, crdt_value *out) {
    if (!t->val[cell]) {
        char *w = where_clause(t, 1); char *q = sqlite3_mprintf("SELECT \"%w\" FROM \"%w\" WHERE %s", t->cell[cell], t->name, w); free(w);
        int rc = sqlite3_prepare_v2(s->db, q, -1, &t->val[cell], NULL); sqlite3_free(q); if (rc != SQLITE_OK) return false;
    }
    sqlite3_stmt *st = t->val[cell]; sqlite3_reset(st);
    crdt_value pv[MW_CAT_MAXPK]; if (crdt_pk_decode(pk, pklen, pv, MW_CAT_MAXPK) != t->npk) return false;
    for (int k = 0; k < t->npk; k++) bind_value(st, k + 1, &pv[k]);
    if (sqlite3_step(st) != SQLITE_ROW) { sqlite3_reset(st); return false; }
    *out = column_value(st, 0);
    return true;
}

// ---- the payload container ----
typedef struct { uint8_t *p; size_t n, cap; } buf;
static int buf_add (buf *b, const void *d, size_t n) {
    if (b->n + n > b->cap) { size_t nc = b->cap ? b->cap * 2 : 4096; while (nc < b->n + n) nc *= 2; uint8_t *np = realloc(b->p, nc); if (!np) return -1; b->p = np; b->cap = nc; }
    memcpy(b->p + b->n, d, n); b->n += n; return 0;
}
static uint64_t checksum (const uint8_t *p, size_t n) { uint64_t h = 14695981039346656037ULL; for (size_t i = 0; i < n; i++) { h ^= p[i]; h *= 1099511628211ULL; } return h; }

typedef struct __attribute__((packed)) {
    uint32_t signature; uint8_t version; uint8_t libversion[3]; uint32_t expanded_size; uint16_t ncols; uint32_t nrows; uint64_t schema_hash; uint8_t checksum[6];
} phdr;

// The payload is compressed with LZ4 by default (4x smaller, no measurable cost); MW_SYNC_COMPRESS=0 writes it uncompressed (expanded_size 0 in the header, as the container allows).
// Both forms are always decoded.
static int container_encode (const buf *tuples, uint32_t nrows, uint8_t **out, size_t *len) {
    static int comp = -1; if (comp < 0) { const char *e = getenv("MW_SYNC_COMPRESS"); comp = !(e && e[0] == '0'); }
    int bound = comp ? LZ4_compressBound((int)tuples->n) : (int)tuples->n;
    uint8_t *z = malloc(sizeof(phdr) + (size_t)bound); if (!z) return SQLITE_NOMEM;
    int zn = comp ? LZ4_compress_default((const char *)tuples->p, (char *)z + sizeof(phdr), (int)tuples->n, bound) : 0;
    bool raw = zn <= 0 || (size_t)zn > tuples->n;
    phdr h; memset(&h, 0, sizeof h);
    h.signature = htonl(PAYLOAD_SIGNATURE); h.version = PAYLOAD_VERSION; h.libversion[0] = 1; h.libversion[1] = 2; h.libversion[2] = 0;
    h.expanded_size = htonl(raw ? 0 : (uint32_t)tuples->n); h.ncols = htons(NCOLS); h.nrows = htonl(nrows);
    if (raw) { zn = (int)tuples->n; memcpy(z + sizeof h, tuples->p, tuples->n); }
    uint64_t ck = checksum(z + sizeof h, (size_t)zn);
    for (int i = 0; i < 6; i++) h.checksum[i] = (uint8_t)(ck >> (40 - 8 * i));
    memcpy(z, &h, sizeof h); *out = z; *len = sizeof h + (size_t)zn;
    return SQLITE_OK;
}
static int container_decode (const uint8_t *p, size_t n, uint8_t **tuples, size_t *tlen, uint32_t *nrows) {
    if (n < sizeof(phdr)) return SQLITE_MISUSE;
    phdr h; memcpy(&h, p, sizeof h);
    if (ntohl(h.signature) != PAYLOAD_SIGNATURE || ntohs(h.ncols) != NCOLS || h.version < 1 || h.version > 2) return SQLITE_MISUSE;
    const uint8_t *body = p + sizeof h; size_t bn = n - sizeof h;
    if (h.version >= 2) { uint64_t ck = checksum(body, bn), have = 0; for (int i = 0; i < 6; i++) have = (have << 8) | h.checksum[i]; if ((ck & 0xFFFFFFFFFFFFULL) != have) return SQLITE_CORRUPT; }
    uint32_t ex = ntohl(h.expanded_size); *nrows = ntohl(h.nrows);
    if (ex) {
        if (ex > INT32_MAX || (uint64_t)ex > (uint64_t)bn * 255 + 64) return SQLITE_CORRUPT;
        uint8_t *o = malloc(ex); if (!o) return SQLITE_NOMEM;
        int r = LZ4_decompress_safe((const char *)body, (char *)o, (int)bn, (int)ex);
        if (r <= 0 || (uint32_t)r != ex) { free(o); return SQLITE_CORRUPT; }
        *tuples = o; *tlen = ex;
    } else { uint8_t *o = malloc(bn ? bn : 1); if (!o) return SQLITE_NOMEM; memcpy(o, body, bn); *tuples = o; *tlen = bn; }
    return SQLITE_OK;
}

// ---- helpers ----
static mw_meta *meta_of (sqlite3 *db) { mw_meta *m = NULL; return sqlite3_file_control(db, "main", MW_FCNTL_META, &m) == SQLITE_OK ? m : NULL; }
void mw_sync_free (void *p) { free(p); }
static bool busyish (int rc) { int p = rc & 0xff; return p == SQLITE_BUSY || p == SQLITE_LOCKED; }

int mw_sync_site_id (sqlite3 *db, uint8_t out[16]) {
    mw_meta *m = meta_of(db); if (!m) return SQLITE_MISUSE;
    mw_meta_ready(m);
    return mw_meta_site_id(m, 0, out) ? SQLITE_OK : SQLITE_ERROR;
}
int64_t mw_sync_db_version (sqlite3 *db) { mw_meta *m = meta_of(db); if (!m) return -1; mw_meta_ready(m); return (int64_t)mw_meta_dv(m, mw_meta_epoch(m)); }

// ---- export ----
typedef mw_xcell xcell;
static int xcell_cmp (const void *a, const void *b) { const xcell *x = a, *y = b; if (x->c.dv != y->c.dv) return x->c.dv < y->c.dv ? -1 : 1; return x->c.seq != y->c.seq ? (x->c.seq < y->c.seq ? -1 : 1) : 0; }
int mw_sync_export (sqlite3 *db, int64_t since, uint8_t **payload, size_t *len, int64_t *upto) {
    *payload = NULL; *len = 0;
    mw_meta *m = meta_of(db); if (!m) return SQLITE_MISUSE;
    mw_meta_ready(m);
    uint64_t V = mw_meta_dv(m, mw_meta_epoch(m));                            // every commit up to V is applied to the store; the flush puts them in the file
    int rc = mw_meta_flush(m); if (rc != SQLITE_OK) return rc;
    if (upto) *upto = (int64_t)V;
    if (since >= (int64_t)V) return SQLITE_OK;
    { int xr = SQLITE_BUSY; for (int i = 0; i < 200 && busyish(xr); i++) { xr = mw_meta_export_index(db); if (busyish(xr)) usleep(1000u * (unsigned)(i + 1)); } if (xr != SQLITE_OK) return xr; }       // (the first export builds the index the later ones use)
    sysch sc; buf tuples = {0}; uint32_t nrows = 0;
    rc = sqlite3_exec(db, "BEGIN", NULL, NULL, NULL); if (rc != SQLITE_OK) return rc;
    rc = sch_load(db, &sc); sc.meta = m;
    // The cells in (since, V] with the causal length of their row, in the order of (db_version, sequence); read in the snapshot of this connection.
    xcell *xs = NULL; size_t nx = 0; uint8_t *pkpool = NULL;
    if (rc == SQLITE_OK && mw_metafile_export(m, db, since, (int64_t)V, &xs, &nx, &pkpool) != 0) rc = SQLITE_ERROR;
    if (rc == SQLITE_OK && nx > 1) qsort(xs, nx, sizeof *xs, xcell_cmp);
    for (size_t xi = 0; rc == SQLITE_OK && xi < nx; xi++) {
        const xcell *x = &xs[xi];
        uint32_t tid = x->tid; syt *t = by_tid(&sc, tid); if (!t) continue;
        const void *pk = pkpool + x->pko; size_t pklen = x->pkl;
        int64_t col = x->c.col == CRDT_COL_SENTINEL ? -1 : (int64_t)x->c.col; crdt_value val = { CRDT_NULL, 0, 0, NULL, 0 }; const char *cname; int cell = -1;
        {                                                                          // the cell of the file may be older than the row we read in this snapshot (a commit since the flush): its value would not be its own; the newer cell goes in the next payload
            mw_mcell *live; int nl; bool ahead = false;
            if (mw_meta_row(m, tid, pk, pklen, &live, &nl) == 0) { for (int i = 0; i < nl; i++) if (live[i].col == x->c.col && live[i].dv > x->c.dv) ahead = true; free(live); }
            if (ahead) continue;
        }
        if (col == -1) cname = CRDT_SENTINEL;
        else { cell = cell_by_id(t, (uint32_t)col); if (cell < 0) continue; cname = t->cell[cell]; if (!base_value(&sc, t, cell, pk, pklen, &val)) val = (crdt_value){ CRDT_NULL, 0, 0, NULL, 0 }; }
        int64_t clv = x->cl;
        uint8_t site[16]; if (!mw_meta_site_id(m, x->c.site, site)) memset(site, 0, 16);
        crdt_value tv[NCOLS] = {
            { CRDT_TEXT, 0, 0, t->name, strlen(t->name) }, { CRDT_BLOB, 0, 0, pk, pklen }, { CRDT_TEXT, 0, 0, cname, strlen(cname) }, val,
            { CRDT_INTEGER, x->c.cv, 0, NULL, 0 }, { CRDT_INTEGER, x->c.dv, 0, NULL, 0 }, { CRDT_BLOB, 0, 0, site, 16 },
            { CRDT_INTEGER, clv, 0, NULL, 0 }, { CRDT_INTEGER, (int64_t)x->c.seq, 0, NULL, 0 } };
        size_t need = crdt_tuple_encode(tv, NCOLS, NULL, 0); uint8_t *tmp = malloc(need);
        if (!tmp) { rc = SQLITE_NOMEM; break; }
        crdt_tuple_encode(tv, NCOLS, tmp, need);
        if (buf_add(&tuples, tmp, need)) rc = SQLITE_NOMEM;
        free(tmp); nrows++;
    }
    free(xs); free(pkpool); sch_free(&sc);
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    if (rc == SQLITE_OK && nrows) rc = container_encode(&tuples, nrows, payload, len);
    free(tuples.p);
    return rc;
}

// ---- apply ----
typedef struct { const char *tbl; size_t tbllen; const uint8_t *pk; size_t pklen; const char *col; size_t collen; crdt_value val; int64_t cv, dv, seq, cl; const uint8_t *site; } chg;

typedef struct { sysch *sc; } vctx;
static bool value_cb (void *arg, uint32_t tbl, const void *pk, size_t pklen, uint32_t col, crdt_value *out) {
    sysch *sc = arg; syt *t = by_tid(sc, tbl); if (!t) return false;
    int cell = cell_by_id(t, col); if (cell < 0) return false;
    return base_value(sc, t, cell, pk, pklen, out);
}

typedef struct { int cell; crdt_value v; } pend;
typedef struct { syt *t; const uint8_t *pk; size_t pklen; bool need_insert; pend *p; int np, cap; } group;

static int exec_stmt (sqlite3 *db, const char *sql, const syt *t, const crdt_value *pkv, const pend *p, int np) {
    sqlite3_stmt *st = NULL; int rc = sqlite3_prepare_v2(db, sql, -1, &st, NULL); if (rc != SQLITE_OK) return rc;
    int n = 1; for (int k = 0; k < t->npk; k++) bind_value(st, n++, &pkv[k]);
    for (int i = 0; i < np; i++) bind_value(st, n++, &p[i].v);
    rc = sqlite3_step(st); sqlite3_finalize(st);
    return rc == SQLITE_DONE ? SQLITE_OK : sqlite3_errcode(db) ? sqlite3_errcode(db) : SQLITE_ERROR;
}

static int group_flush (sqlite3 *db, group *g) {
    if (!g->t) return SQLITE_OK;
    crdt_value pv[MW_CAT_MAXPK]; if (crdt_pk_decode(g->pk, g->pklen, pv, MW_CAT_MAXPK) != g->t->npk) return SQLITE_CORRUPT;
    int rc = SQLITE_OK; const syt *t = g->t;
    if (g->np > 0 || g->need_insert) {
        size_t cap = 256; for (int k = 0; k < t->npk; k++) cap += 2 * strlen(t->pk[k]) + 16; for (int i = 0; i < g->np; i++) cap += 2 * strlen(t->cell[g->p[i].cell]) + 40;
        char *sql = malloc(cap), *w = sql;
        w += sprintf(w, "INSERT %sINTO \"%s\"(", g->np ? "" : "OR IGNORE ", t->name);
        for (int k = 0; k < t->npk; k++) w += sprintf(w, "%s\"%s\"", k ? "," : "", t->pk[k]);
        for (int i = 0; i < g->np; i++) w += sprintf(w, ",\"%s\"", t->cell[g->p[i].cell]);
        w += sprintf(w, ") VALUES(");
        for (int k = 0; k < t->npk + g->np; k++) w += sprintf(w, "%s?", k ? "," : "");
        w += sprintf(w, ")");
        if (g->np) {
            w += sprintf(w, " ON CONFLICT(");
            for (int k = 0; k < t->npk; k++) w += sprintf(w, "%s\"%s\"", k ? "," : "", t->pk[k]);
            w += sprintf(w, ") DO UPDATE SET ");
            for (int i = 0; i < g->np; i++) w += sprintf(w, "%s\"%s\"=excluded.\"%s\"", i ? "," : "", t->cell[g->p[i].cell], t->cell[g->p[i].cell]);
        }
        rc = exec_stmt(db, sql, t, pv, g->p, g->np); free(sql);
    }
    g->np = 0; g->need_insert = false;
    return rc;
}

static int group_delete (sqlite3 *db, group *g) {
    crdt_value pv[MW_CAT_MAXPK]; if (crdt_pk_decode(g->pk, g->pklen, pv, MW_CAT_MAXPK) != g->t->npk) return SQLITE_CORRUPT;
    char *w = where_clause(g->t, 1); char *sql = sqlite3_mprintf("DELETE FROM \"%w\" WHERE %s", g->t->name, w); free(w);
    int rc = exec_stmt(db, sql, g->t, pv, NULL, 0); sqlite3_free(sql);
    g->np = 0; g->need_insert = false;
    return rc;
}

static int apply_changes (sqlite3 *db, sysch *sc, mw_ovl *ov, const chg *ch, int n, mw_sync_stats *st) {
    const crdt_ops *ops = mw_ovl_ops(); group g = {0}; int rc = SQLITE_OK;
    mw_want *wants = malloc(512 * sizeof *wants);
    for (int i = 0; i < n && rc == SQLITE_OK; i++) {
        const chg *c = &ch[i];
        if (wants && i % 512 == 0) {                                                  // the rows of the next changes, loaded together
            int nw = 0; for (int k = i; k < n && k < i + 512; k++) { syt *tk = by_name(sc, ch[k].tbl, ch[k].tbllen); if (tk) wants[nw++] = (mw_want){ tk->tid, ch[k].pk, ch[k].pklen, false }; }
            mw_ovl_prefetch(ov, wants, nw);
        }
        if (mw_ovl_err(ov)) { rc = SQLITE_IOERR; break; }                            // (a row that could not be read is not a row that is not there: the merge would let a remote change win against a newer cell)
        syt *t = by_name(sc, c->tbl, c->tbllen);
        bool sentinel = c->collen == strlen(CRDT_SENTINEL) && !memcmp(c->col, CRDT_SENTINEL, c->collen);
        int cell = sentinel ? -1 : t ? cell_by_name(t, c->col, c->collen) : -1;
        if (!t || (!sentinel && cell < 0)) { st->ignored++; continue; }
        if (g.t != t || g.pklen != c->pklen || memcmp(g.pk, c->pk, c->pklen)) { rc = group_flush(db, &g); if (rc != SQLITE_OK) break; g.t = t; g.pk = c->pk; g.pklen = c->pklen; }
        crdt_change cc = { t->tid, c->pk, c->pklen, sentinel ? CRDT_COL_SENTINEL : t->cid[cell], &c->val, c->cv, c->cl, c->dv, c->seq, {0} };
        memcpy(cc.site, c->site, 16);
        crdt_actions act;
        if (crdt_merge(ops, ov, &cc, 0, true, &act) != 0) { rc = SQLITE_ERROR; break; }
        if (mw_ovl_err(ov)) { rc = SQLITE_IOERR; break; }
        if (act.n) st->applied++;
        for (int k = 0; k < act.n && rc == SQLITE_OK; k++) {
            if (act.kind[k] == CRDT_ACT_DELETE_ROW) rc = group_delete(db, &g);
            else if (act.kind[k] == CRDT_ACT_INSERT_ROW) g.need_insert = true;
            else if (act.kind[k] == CRDT_ACT_WRITE_COL) {
                int w = -1; for (int q = 0; q < g.np; q++) if (g.p[q].cell == cell) w = q;
                if (w < 0) { if (g.np == g.cap) { g.cap = g.cap ? g.cap * 2 : 8; g.p = realloc(g.p, (size_t)g.cap * sizeof *g.p); } w = g.np++; }
                g.p[w] = (pend){ cell, c->val };
            }
        }
    }
    if (rc == SQLITE_OK) rc = group_flush(db, &g);
    free(g.p); free(wants);
    return rc;
}

int mw_sync_apply (sqlite3 *db, const uint8_t *payload, size_t len, mw_sync_stats *stats) {
    mw_sync_stats local; if (!stats) stats = &local; memset(stats, 0, sizeof *stats);
    mw_meta *m = meta_of(db); if (!m) return SQLITE_MISUSE;
    if (!sqlite3_get_autocommit(db)) return SQLITE_MISUSE;
    mw_meta_ready(m);
    uint8_t *tuples = NULL; size_t tlen = 0; uint32_t nrows = 0;
    int rc = container_decode(payload, len, &tuples, &tlen, &nrows); if (rc != SQLITE_OK) return rc;
    chg *ch = calloc((size_t)nrows + 1, sizeof *ch); int n = 0;
    size_t off = 0;
    while (off < tlen && n < (int)nrows) {
        crdt_value v[NCOLS]; size_t used = 0;
        if (crdt_tuple_decode(tuples + off, tlen - off, NCOLS, v, &used) != NCOLS || v[0].type != CRDT_TEXT || v[1].type != CRDT_BLOB || v[2].type != CRDT_TEXT || v[6].type != CRDT_BLOB || v[6].n != 16) { rc = SQLITE_CORRUPT; break; }
        ch[n++] = (chg){ v[0].p, v[0].n, v[1].p, v[1].n, v[2].p, v[2].n, v[3], v[4].i, v[5].i, v[8].i, v[7].i, v[6].p };
        off += used;
    }
    stats->rows = n;
    for (int attempt = 0; rc == SQLITE_OK; attempt++) {
        mw_ovl *ov = mw_ovl_new(m); sysch sc; if (!ov || sch_load(db, &sc) != SQLITE_OK) { mw_ovl_free(ov); rc = SQLITE_NOMEM; break; }
        sc.meta = m; mw_ovl_set_value_fn(ov, value_cb, &sc);
        stats->applied = stats->ignored = 0;
        int gate = attempt >= 6;                                           // it keeps losing against the commits of this process: they wait until it is through
        if (gate) sqlite3_file_control(db, "main", MW_FCNTL_GATE, &gate);
        rc = sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);
        if (rc == SQLITE_OK) {
            sqlite3_file_control(db, "main", MW_FCNTL_DECLARE, ov);
            rc = apply_changes(db, &sc, ov, ch, n, stats);
            if (rc == SQLITE_OK) rc = sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
            sqlite3_file_control(db, "main", MW_FCNTL_DECLARE, NULL);
            if (rc != SQLITE_OK && !sqlite3_get_autocommit(db)) sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
        }
        if (gate) { int off = 0; sqlite3_file_control(db, "main", MW_FCNTL_GATE, &off); }
        sch_free(&sc); mw_ovl_free(ov);
        if (rc != SQLITE_OK && busyish(rc) && attempt < 1000) { stats->retries++; rc = SQLITE_OK; usleep(200u * (unsigned)(attempt < 50 ? attempt + 1 : 50)); continue; }
        break;
    }
    free(ch); free(tuples);
    return rc;
}

// ---- backfill ----
int mw_sync_backfill (sqlite3 *db) {
    mw_meta *m = meta_of(db); if (!m) return SQLITE_MISUSE;
    if (!sqlite3_get_autocommit(db)) return SQLITE_MISUSE;
    mw_meta_ready(m);
    sysch sc; int rc = sch_load(db, &sc); if (rc != SQLITE_OK) return rc; sc.meta = m;
    const crdt_ops *ops = mw_ovl_ops();
    for (int ti = 0; ti < sc.n && rc == SQLITE_OK; ti++) {
        syt *t = &sc.t[ti];
        size_t cap = 256; for (int k = 0; k < t->npk; k++) cap += 3 * strlen(t->pk[k]) + 16;
        char *sel = malloc(cap), *w = sel, *ord = malloc(cap), *o = ord, *cmp = malloc(cap), *c = cmp, *ph = malloc(cap), *x = ph;
        w += sprintf(w, "SELECT "); for (int k = 0; k < t->npk; k++) w += sprintf(w, "%s\"%s\"", k ? "," : "", t->pk[k]);
        for (int k = 0; k < t->npk; k++) { o += sprintf(o, "%s\"%s\"", k ? "," : "", t->pk[k]); c += sprintf(c, "%s\"%s\"", k ? "," : "", t->pk[k]); x += sprintf(x, "%s?%d", k ? "," : "", k + 1); }
        char *q = sqlite3_mprintf("%s FROM \"%w\" WHERE (%s) > (%s) ORDER BY %s LIMIT 500", sel, t->name, cmp, ph, ord);
        char *q0 = sqlite3_mprintf("%s FROM \"%w\" ORDER BY %s LIMIT 500", sel, t->name, ord);
        free(sel); free(ord); free(cmp); free(ph);
        crdt_value last[MW_CAT_MAXPK]; uint8_t *keep[MW_CAT_MAXPK] = {0}; bool have_last = false;
        for (;;) {
            sqlite3_stmt *st = NULL; rc = sqlite3_prepare_v2(db, have_last ? q : q0, -1, &st, NULL); if (rc != SQLITE_OK) break;
            if (have_last) for (int k = 0; k < t->npk; k++) bind_value(st, k + 1, &last[k]);
            mw_ovl *ov = mw_ovl_new(m); int64_t seq = 0; int rows = 0;
            if (!ov) { sqlite3_finalize(st); rc = SQLITE_NOMEM; break; }
            rc = sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);
            if (rc != SQLITE_OK) { sqlite3_finalize(st); mw_ovl_free(ov); break; }
            while (sqlite3_step(st) == SQLITE_ROW) {
                crdt_value pv[MW_CAT_MAXPK]; for (int k = 0; k < t->npk; k++) pv[k] = column_value(st, k);
                size_t pl = crdt_pk_encode(pv, t->npk, NULL, 0); uint8_t *pk = malloc(pl); crdt_pk_encode(pv, t->npk, pk, pl);
                if (!ops->row_known(ov, t->tid, pk, pl)) crdt_local_insert(ops, ov, t->tid, pk, pl, t->cid, t->nc, 0, &seq, NULL, 0);
                free(pk); rows++;
                for (int k = 0; k < t->npk; k++) { free(keep[k]); keep[k] = NULL; last[k] = pv[k]; if (pv[k].type == CRDT_TEXT || pv[k].type == CRDT_BLOB) { keep[k] = malloc(pv[k].n ? pv[k].n : 1); memcpy(keep[k], pv[k].p, pv[k].n); last[k].p = keep[k]; } }
                have_last = true;
            }
            sqlite3_finalize(st);
            if (mw_ovl_err(ov)) { sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL); rc = SQLITE_IOERR; }          // (a row that could not be read: no metadata is made up for it)
            else if (!mw_ovl_empty(ov)) {
                sqlite3_file_control(db, "main", MW_FCNTL_DECLARE, ov);
                rc = sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS mw_state(k TEXT PRIMARY KEY NOT NULL, v) WITHOUT ROWID; INSERT OR REPLACE INTO mw_state(k, v) VALUES('backfill', strftime('%s','now') || '.' || abs(random()))", NULL, NULL, NULL);       // (a commit has to write a page for the declared metadata to be logged)
                if (rc == SQLITE_OK) rc = sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
                sqlite3_file_control(db, "main", MW_FCNTL_DECLARE, NULL);
            } else rc = sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
            if (rc != SQLITE_OK && !sqlite3_get_autocommit(db)) sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
            mw_ovl_free(ov);
            if (rc != SQLITE_OK || rows < 500) break;
        }
        for (int k = 0; k < MW_CAT_MAXPK; k++) free(keep[k]);
        sqlite3_free(q); sqlite3_free(q0);
    }
    sch_free(&sc);
    return rc;
}
