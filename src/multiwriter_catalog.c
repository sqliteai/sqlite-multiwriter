//
//  multiwriter_catalog.c
//
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include "multiwriter_internal.h"
#include "multiwriter_os.h"
#include "multiwriter_catalog.h"

static uint32_t be32 (const uint8_t *p) { return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | p[3]; }
static int be16 (const uint8_t *p) { return (p[0] << 8) | p[1]; }

typedef struct { char *type; char *name; uint32_t root; char *sql; } srow;
typedef struct { srow *rows; int n, cap; } srows;

// a text or integer column of the local part of a record, as a NUL-terminated copy
static char *rec_col_text (const uint8_t *rec, uint32_t len, int col) {
    uint64_t hs = 0; int hl = 0;
    for (; hl < 9 && (uint32_t)hl < len; hl++) { hs = (hs << 7) | (rec[hl] & 0x7f); if (!(rec[hl] & 0x80)) { hl++; break; } }
    uint32_t pos = (uint32_t)hs, hp = (uint32_t)hl; int idx = 0;
    while (hp < hs && hp < len) {
        uint64_t t = 0; int k = 0; for (; k < 9 && hp + (uint32_t)k < len; k++) { t = (t << 7) | (rec[hp + (uint32_t)k] & 0x7f); if (!(rec[hp + (uint32_t)k] & 0x80)) { k++; break; } }
        hp += (uint32_t)k;
        uint32_t l = t < 12 ? ((const uint8_t[]){0, 1, 2, 3, 4, 6, 8, 8, 0, 0, 0, 0})[t] : (uint32_t)((t - 12) / 2);
        if (idx == col) {
            if (pos + l > len) return NULL;
            if (t >= 13 && (t & 1)) { char *s = malloc(l + 1); memcpy(s, rec + pos, l); s[l] = 0; return s; }
            if (t >= 1 && t <= 6) { int64_t v = (int8_t)rec[pos]; for (uint32_t b = 1; b < l; b++) v = (v << 8) | rec[pos + b]; char *s = malloc(24); snprintf(s, 24, "%lld", (long long)v); return s; }
            if (t == 8) return strdup("0");
            if (t == 9) return strdup("1");
            return NULL;
        }
        pos += l; idx++;
    }
    return NULL;
}

// the full payload of a schema cell: its local part, then the overflow chain
static uint8_t *cell_payload (mw_lane *lane, const uint8_t *cp, const uint8_t *end, uint32_t pgsz, uint32_t *outlen) {
    uint64_t P = 0, rid = 0; int a = 0, b = 0;
    for (; a < 9 && cp + a < end; a++) { P = (P << 7) | (cp[a] & 0x7f); if (!(cp[a] & 0x80)) { a++; break; } }
    for (; b < 9 && cp + a + b < end; b++) { rid = (rid << 7) | (cp[a + b] & 0x7f); if (!(cp[a + b] & 0x80)) { b++; break; } }
    const uint32_t U = pgsz, X = U - 35, M = ((U - 12) * 32 / 255) - 23;
    uint32_t local = (uint32_t)P, ovfl = 0;
    if (P > X) { uint32_t K = M + (uint32_t)((P - M) % (U - 4)); local = K <= X ? K : M; if (cp + a + b + local + 4 > end) return NULL; ovfl = be32(cp + a + b + local); }
    else if (cp + a + b + local > end) return NULL;
    uint8_t *buf = malloc((size_t)P + 1); if (!buf) return NULL;
    memcpy(buf, cp + a + b, local);
    size_t got = local; uint8_t *pg = malloc(pgsz);
    while (ovfl && got < P && pg) {
        if (!mw_rd_snap_page(lane, ovfl, pg)) { free(buf); free(pg); return NULL; }
        size_t take = P - got < pgsz - 4 ? (size_t)(P - got) : pgsz - 4;
        memcpy(buf + got, pg + 4, take); got += take; ovfl = be32(pg);
    }
    free(pg);
    if (got < P) { free(buf); return NULL; }
    *outlen = (uint32_t)P; (void)rid;
    return buf;
}

static void schema_collect (mw_lane *lane, uint32_t pgno, uint32_t pgsz, srows *out, int depth) {
    uint8_t *pg = malloc(pgsz); if (!pg || depth > 8) { free(pg); return; }
    if (!mw_rd_snap_page(lane, pgno, pg)) { free(pg); return; }
    uint32_t base = pgno == 1 ? 100 : 0;
    if (pg[base] == 0x0d) {
        int nc = be16(pg + base + 3);
        for (int i = 0; i < nc; i++) {
            uint32_t off = (uint32_t)be16(pg + base + 8 + 2 * i); if (off >= pgsz) continue;
            uint32_t plen = 0; uint8_t *rec = cell_payload(lane, pg + off, pg + pgsz, pgsz, &plen); if (!rec) continue;
            char *type = rec_col_text(rec, plen, 0), *name = rec_col_text(rec, plen, 1), *root = rec_col_text(rec, plen, 3), *sql = rec_col_text(rec, plen, 4);
            if (type && name && (!strcmp(type, "table") || !strcmp(type, "trigger"))) {
                if (out->n == out->cap) { out->cap = out->cap ? out->cap * 2 : 32; out->rows = realloc(out->rows, (size_t)out->cap * sizeof *out->rows); }
                out->rows[out->n++] = (srow){ type, name, root ? (uint32_t)strtoul(root, NULL, 10) : 0, sql }; type = name = sql = NULL;
            }
            free(type); free(name); free(root); free(sql); free(rec);
        }
    } else if (pg[base] == 0x05) {
        int nc = be16(pg + base + 3);
        for (int i = 0; i < nc; i++) schema_collect(lane, be32(pg + (uint32_t)be16(pg + base + 12 + 2 * i)), pgsz, out, depth + 1);
        schema_collect(lane, be32(pg + base + 8), pgsz, out, depth + 1);
    }
    free(pg);
}

static bool starts_ci (const char *s, const char *p) { while (*s == ' ' || *s == '\n' || *s == '\t' || *s == '\r') s++; return strncasecmp(s, p, strlen(p)) == 0; }

static void tab_free (mw_tab *t) {
    free(t->name); for (int i = 0; i < t->nrec; i++) { free(t->rec_name[i]); if (t->dflt) free(t->dflt[i].p); }
    free(t->rec_name); free(t->dflt);
    for (int i = 0; i < t->nfk; i++) { free(t->fk_parent[i]); free(t->fk_to[i]); }
    free(t->fk_parent); free(t->fk_to);
    for (int i = 0; i < t->npk; i++) free(t->pk_name[i]);
    free(t->pk_name);
}
// the value of a default expression, evaluated by SQLite (a constant: ALTER TABLE ADD COLUMN accepts nothing else)
static void eval_default (sqlite3 *scratch, const char *expr, mw_val *v) {
    memset(v, 0, sizeof *v); v->type = SQLITE_NULL;
    if (!expr) return;
    char *q = sqlite3_mprintf("SELECT %s", expr); sqlite3_stmt *st = NULL;
    if (q && sqlite3_prepare_v2(scratch, q, -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) {
        v->type = sqlite3_column_type(st, 0);
        if (v->type == SQLITE_INTEGER) v->i = sqlite3_column_int64(st, 0);
        else if (v->type == SQLITE_FLOAT) v->d = sqlite3_column_double(st, 0);
        else if (v->type == SQLITE_TEXT || v->type == SQLITE_BLOB) {
            v->n = sqlite3_column_bytes(st, 0); v->p = malloc((size_t)v->n + 1);
            if (v->p) memcpy(v->p, v->type == SQLITE_TEXT ? (const void *)sqlite3_column_text(st, 0) : sqlite3_column_blob(st, 0), (size_t)v->n); else v->type = SQLITE_NULL;
        }
    }
    sqlite3_finalize(st); sqlite3_free(q);
}

// understand one table with SQLite's help
static void tab_parse (sqlite3 *scratch, const srow *r, mw_tab *t) {
    memset(t, 0, sizeof *t); t->root = r->root; t->name = strdup(r->name); t->alias_rec = -1;
    if (!r->sql || !starts_ci(r->sql, "CREATE TABLE") || starts_ci(r->sql, "CREATE VIRTUAL")) return;
    if (strncasecmp(r->name, "sqlite_", 7) == 0) return;
    t->without_rowid = strcasestr(r->sql, "WITHOUT ROWID") != NULL;                                       // (a table named so would fool this; the scratch database rejects the statement then)
    char *err = NULL;
    if (sqlite3_exec(scratch, r->sql, NULL, NULL, &err) != SQLITE_OK) { sqlite3_free(err); return; }
    char *q = sqlite3_mprintf("PRAGMA table_xinfo(\"%w\")", r->name); sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(scratch, q, -1, &st, NULL) != SQLITE_OK) { sqlite3_free(q); return; }
    sqlite3_free(q);
    struct col { char *name; char *type; char *dflt; int pk, hidden; } cols[2048]; int nc = 0;
    while (sqlite3_step(st) == SQLITE_ROW && nc < 2048) {
        cols[nc].name = strdup((const char *)sqlite3_column_text(st, 1)); const char *ty = (const char *)sqlite3_column_text(st, 2); cols[nc].type = strdup(ty ? ty : "");
        { const char *d = (const char *)sqlite3_column_text(st, 4); cols[nc].dflt = d ? strdup(d) : NULL; }
        cols[nc].pk = sqlite3_column_int(st, 5); cols[nc].hidden = sqlite3_column_int(st, 6); nc++;
    }
    sqlite3_finalize(st);
    int npk = 0; for (int i = 0; i < nc; i++) if (cols[i].pk > 0) npk++;
    if (!t->without_rowid && npk == 1) for (int i = 0; i < nc; i++) if (cols[i].pk == 1 && strcasecmp(cols[i].type, "INTEGER") == 0) t->alias_pk = true;
    t->rec_name = calloc((size_t)(nc ? nc : 1), sizeof(char *));
    // the order of the columns in the record: the declaration order; in a WITHOUT ROWID table the primary key columns come first (in the order of the key), then the others
    int order[2048], no = 0;
    if (t->without_rowid) {
        for (int k = 1; k <= npk; k++) for (int i = 0; i < nc; i++) if (cols[i].pk == k) order[no++] = i;
        t->npk_rec = no;
        for (int i = 0; i < nc; i++) if (cols[i].pk <= 0) order[no++] = i;
        // the key must compare as bytes would: the default collation (BINARY) and ascending, or the pages cannot be walked and the rows cannot be told apart by their key
        bool plain = npk > 0; char *iq = sqlite3_mprintf("PRAGMA index_list(\"%w\")", r->name); sqlite3_stmt *is = NULL; char *pkname = NULL;
        if (iq && sqlite3_prepare_v2(scratch, iq, -1, &is, NULL) == SQLITE_OK) while (sqlite3_step(is) == SQLITE_ROW) { const char *org = (const char *)sqlite3_column_text(is, 3); if (org && !strcmp(org, "pk")) pkname = strdup((const char *)sqlite3_column_text(is, 1)); }
        sqlite3_finalize(is); sqlite3_free(iq);
        if (!pkname) plain = false;
        else {
            char *xq = sqlite3_mprintf("PRAGMA index_xinfo(\"%w\")", pkname); sqlite3_stmt *xs = NULL;
            if (xq && sqlite3_prepare_v2(scratch, xq, -1, &xs, NULL) == SQLITE_OK) while (sqlite3_step(xs) == SQLITE_ROW) if (sqlite3_column_int(xs, 5)) { const char *co = (const char *)sqlite3_column_text(xs, 4); if (sqlite3_column_int(xs, 3) || !co || strcasecmp(co, "BINARY")) plain = false; }
            sqlite3_finalize(xs); sqlite3_free(xq);
        }
        free(pkname);
        if (!plain) { for (int i = 0; i < nc; i++) { free(cols[i].name); free(cols[i].type); free(cols[i].dflt); } return; }       // (t->ok stays false: a write to it is not replayed)
    } else for (int i = 0; i < nc; i++) order[no++] = i;
    int pos = 0;
    for (int j = 0; j < no && t->rec_name; j++) {
        int i = order[j];
        if (cols[i].hidden == 2) continue;                                                                   // (generated, virtual: not stored)
        if (t->alias_pk && cols[i].pk == 1) t->alias_rec = pos;
        t->rec_name[pos++] = (cols[i].hidden == 3) ? NULL : strdup(cols[i].name);                            // (generated, stored: in the record, derived)
    }
    t->nrec = pos;
    t->dflt = calloc((size_t)(pos ? pos : 1), sizeof *t->dflt);
    for (int j = 0, k = 0; j < no && t->dflt; j++) { int i = order[j]; if (cols[i].hidden == 2) continue; eval_default(scratch, cols[i].hidden == 3 ? NULL : cols[i].dflt, &t->dflt[k++]); }
    for (int i = 0; i < nc; i++) if (cols[i].pk > 0) { t->pk_name = realloc(t->pk_name, (size_t)(t->npk + 1) * sizeof(char *)); t->pk_name[t->npk++] = strdup(cols[i].name); }
    {   // foreign keys
        char *fq = sqlite3_mprintf("PRAGMA foreign_key_list(\"%w\")", r->name); sqlite3_stmt *fs = NULL;
        if (fq && sqlite3_prepare_v2(scratch, fq, -1, &fs, NULL) == SQLITE_OK)
            while (sqlite3_step(fs) == SQLITE_ROW) {
                const char *pt = (const char *)sqlite3_column_text(fs, 2), *to = (const char *)sqlite3_column_text(fs, 4);
                t->fk_parent = realloc(t->fk_parent, (size_t)(t->nfk + 1) * sizeof(char *)); t->fk_to = realloc(t->fk_to, (size_t)(t->nfk + 1) * sizeof(char *));
                t->fk_parent[t->nfk] = strdup(pt ? pt : ""); t->fk_to[t->nfk] = to ? strdup(to) : NULL; t->nfk++;
            }
        sqlite3_finalize(fs); sqlite3_free(fq);
    }
    for (int i = 0; i < nc; i++) { free(cols[i].name); free(cols[i].type); free(cols[i].dflt); }
    t->ok = t->rec_name != NULL && t->dflt != NULL;
}

// bit of a column among the writable columns of a table (as changed_mask of the rebase counts them), -1 if it has none
static int writable_bit (const mw_tab *t, const char *name) {
    int bit = 0;
    for (int i = 0; i < t->nrec; i++) { if (!t->rec_name[i] || i == t->alias_rec) continue; if (!strcasecmp(t->rec_name[i], name)) return bit; bit++; }
    return -1;
}
// The foreign keys as a graph: which tables are parents, which of their columns are referred to, and a rank (parents before children). A table that refers to itself, or a cycle, is not replayed
// (the order of the rows inside a table would matter).
static void fk_graph (mw_cat *c) {
    for (int i = 0; i < c->n; i++) c->tabs[i].rank = 0;
    for (int i = 0; i < c->n; i++) {
        mw_tab *t = &c->tabs[i];
        for (int k = 0; k < t->nfk; k++) {
            c->has_fk = true;
            mw_tab *p = (mw_tab *)mw_cat_by_name(c, t->fk_parent[k]);
            if (!p) continue;                                           // (a parent that does not exist: SQLite refuses the statement itself)
            if (p == t) { c->rebasable = false; c->why = "the database has a table with a foreign key to itself"; return; }
            p->is_parent = true;
            if (t->fk_to[k]) { int b = writable_bit(p, t->fk_to[k]); if (b >= 0 && b < 64) p->refmask |= 1ull << b; }
            else for (int q = 0; q < p->npk; q++) { int b = writable_bit(p, p->pk_name[q]); if (b >= 0 && b < 64) p->refmask |= 1ull << b; }
        }
    }
    for (int round = 0; round <= c->n; round++) {                       // rank = 1 + the highest rank of the parents (a fixed point; a cycle never settles)
        bool changed = false;
        for (int i = 0; i < c->n; i++) {
            mw_tab *t = &c->tabs[i]; int r = 0;
            for (int k = 0; k < t->nfk; k++) { const mw_tab *p = mw_cat_by_name(c, t->fk_parent[k]); if (p && p->rank + 1 > r) r = p->rank + 1; }
            if (r != t->rank) { t->rank = r; changed = true; }
        }
        if (!changed) break;
        if (round == c->n) { c->rebasable = false; c->why = "the foreign keys of the database are circular"; return; }
    }
    for (int i = 0; i < c->n; i++) if (c->tabs[i].rank > c->maxrank) c->maxrank = c->tabs[i].rank;
}

mw_cat *mw_cat_build (mw_lane *lane) {
    uint32_t pgsz = (uint32_t)lane->db->store->pgsz; srows rows = {0};
    mw_cat *c = calloc(1, sizeof *c); if (!c) return NULL; atomic_init(&c->refs, 1); c->rebasable = true;
    uint8_t *p1 = malloc(pgsz); if (p1 && mw_rd_snap_page(lane, 1, p1)) { c->cookie = be32(p1 + 40); if (be32(p1 + 56) != 1) { c->rebasable = false; c->why = "the text encoding is not UTF-8"; } } free(p1);
    schema_collect(lane, 1, pgsz, &rows, 0);
    sqlite3 *scratch = NULL; sqlite3_open_v2(":memory:", &scratch, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
    c->tabs = calloc((size_t)(rows.n ? rows.n : 1), sizeof *c->tabs);
    for (int i = 0; i < rows.n && c->tabs && scratch; i++) {
        const srow *r = &rows.rows[i];
        if (!strcmp(r->type, "trigger")) { c->rebasable = false; c->why = "the database has a trigger"; continue; }
        if (r->sql && starts_ci(r->sql, "CREATE VIRTUAL")) { c->rebasable = false; c->why = "the database has a virtual table"; }
        tab_parse(scratch, r, &c->tabs[c->n]);
        if (c->tabs[c->n].without_rowid) c->has_wr = true;
        c->n++;
    }
    if (!c->tabs || !scratch) { c->rebasable = false; c->why = "no memory"; }
    else fk_graph(c);
    for (int i = 0; i < rows.n; i++) { free(rows.rows[i].type); free(rows.rows[i].name); free(rows.rows[i].sql); }
    free(rows.rows); sqlite3_close(scratch);
    return c;
}
mw_cat *mw_cat_ref (mw_cat *c) { if (c) atomic_fetch_add(&c->refs, 1); return c; }
void mw_cat_free (mw_cat *c) { if (!c || atomic_fetch_sub(&c->refs, 1) != 1) return; for (int i = 0; i < c->n; i++) tab_free(&c->tabs[i]); free(c->tabs); free(c); }
const mw_tab *mw_cat_by_root (const mw_cat *c, uint32_t root) { for (int i = 0; c && i < c->n; i++) if (c->tabs[i].root == root) return &c->tabs[i]; return NULL; }
const mw_tab *mw_cat_by_name (const mw_cat *c, const char *name) { for (int i = 0; c && i < c->n; i++) if (!strcasecmp(c->tabs[i].name, name)) return &c->tabs[i]; return NULL; }
