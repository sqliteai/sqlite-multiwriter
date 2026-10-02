//
//  multiwriter_catalog.c
//
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include "multiwriter_internal.h"
#include "multiwriter_catalog.h"

static uint32_t be32 (const uint8_t *p) { return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | p[3]; }
static int be16 (const uint8_t *p) { return (p[0] << 8) | p[1]; }

typedef struct { char *name; uint32_t root; char *sql; } srow;
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
            if (type && !strcmp(type, "table") && name && root && sql && strtoul(root, NULL, 10) > 1) {
                if (out->n == out->cap) { out->cap = out->cap ? out->cap * 2 : 32; out->rows = realloc(out->rows, (size_t)out->cap * sizeof *out->rows); }
                out->rows[out->n++] = (srow){ name, (uint32_t)strtoul(root, NULL, 10), sql }; name = sql = NULL;
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

static void tab_free (mw_tab *t) { free(t->name); free(t->cell_rec); for (int i = 0; i < t->ncells; i++) free(t->cell_name[i]); free(t->cell_name); }

// understand one table with SQLite's help
static void tab_parse (sqlite3 *scratch, const srow *r, mw_tab *t) {
    memset(t, 0, sizeof *t); t->root = r->root; t->name = strdup(r->name);
    if (!starts_ci(r->sql, "CREATE TABLE") || starts_ci(r->sql, "CREATE VIRTUAL")) return;
    char *err = NULL;
    if (sqlite3_exec(scratch, r->sql, NULL, NULL, &err) != SQLITE_OK) { sqlite3_free(err); return; }
    t->without_rowid = strcasestr(r->sql, "WITHOUT ROWID") != NULL;                                       // (a table named so would fool this; the scratch database rejects the statement then)
    char *q = sqlite3_mprintf("PRAGMA table_xinfo(\"%w\")", r->name); sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(scratch, q, -1, &st, NULL) != SQLITE_OK) { sqlite3_free(q); return; }
    sqlite3_free(q);
    struct col { char *name; char *type; int pk, hidden; } cols[2048]; int nc = 0;
    while (sqlite3_step(st) == SQLITE_ROW && nc < 2048) {
        cols[nc].name = strdup((const char *)sqlite3_column_text(st, 1)); const char *ty = (const char *)sqlite3_column_text(st, 2); cols[nc].type = strdup(ty ? ty : "");
        cols[nc].pk = sqlite3_column_int(st, 5); cols[nc].hidden = sqlite3_column_int(st, 6); nc++;
    }
    sqlite3_finalize(st);
    int rec_of[2048]; int npk = 0;
    for (int i = 0; i < nc; i++) if (cols[i].pk > 0) npk++;
    t->npk = npk > MW_CAT_MAXPK ? MW_CAT_MAXPK : npk; t->has_pk = npk > 0;
    int pos = 0;
    if (t->without_rowid) {
        for (int k = 1; k <= npk; k++) for (int i = 0; i < nc; i++) if (cols[i].pk == k) rec_of[i] = pos++;
        for (int i = 0; i < nc; i++) if (cols[i].pk == 0) rec_of[i] = cols[i].hidden == 2 ? -1 : pos++;
    } else {
        for (int i = 0; i < nc; i++) rec_of[i] = cols[i].hidden == 2 ? -1 : pos++;
    }
    t->nrec = pos;
    for (int k = 1; k <= t->npk; k++) for (int i = 0; i < nc; i++) if (cols[i].pk == k) t->pk_rec[k - 1] = rec_of[i];
    if (!t->without_rowid && npk == 1) for (int i = 0; i < nc; i++) if (cols[i].pk == 1 && strcasecmp(cols[i].type, "INTEGER") == 0) t->alias_pk = true;
    t->cell_rec = malloc((size_t)nc * sizeof(int)); t->cell_name = malloc((size_t)nc * sizeof(char *));
    for (int i = 0; i < nc; i++) if (cols[i].pk == 0 && cols[i].hidden == 0) { t->cell_rec[t->ncells] = rec_of[i]; t->cell_name[t->ncells] = strdup(cols[i].name); t->ncells++; }
    for (int i = 0; i < nc; i++) { free(cols[i].name); free(cols[i].type); }
    t->tracked = true;
}

mw_cat *mw_cat_build (mw_lane *lane) {
    uint32_t pgsz = (uint32_t)lane->db->store->pgsz; srows rows = {0};
    mw_cat *c = calloc(1, sizeof *c); if (!c) return NULL; atomic_init(&c->refs, 1);
    uint8_t *p1 = malloc(pgsz); if (p1 && mw_rd_snap_page(lane, 1, p1)) c->cookie = be32(p1 + 40); free(p1);
    schema_collect(lane, 1, pgsz, &rows, 0);
    sqlite3 *scratch = NULL; sqlite3_open_v2(":memory:", &scratch, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
    c->tabs = calloc((size_t)(rows.n ? rows.n : 1), sizeof *c->tabs);
    for (int i = 0; i < rows.n && c->tabs && scratch; i++) { tab_parse(scratch, &rows.rows[i], &c->tabs[c->n]); c->n++; }
    for (int i = 0; i < rows.n; i++) { free(rows.rows[i].name); free(rows.rows[i].sql); }
    free(rows.rows); sqlite3_close(scratch);
    return c;
}
mw_cat *mw_cat_ref (mw_cat *c) { if (c) atomic_fetch_add(&c->refs, 1); return c; }
void mw_cat_free (mw_cat *c) { if (!c || atomic_fetch_sub(&c->refs, 1) != 1) return; for (int i = 0; i < c->n; i++) tab_free(&c->tabs[i]); free(c->tabs); free(c); }
const mw_tab *mw_cat_by_root (const mw_cat *c, uint32_t root) { for (int i = 0; c && i < c->n; i++) if (c->tabs[i].root == root) return &c->tabs[i]; return NULL; }
const mw_tab *mw_cat_by_name (const mw_cat *c, const char *name) { for (int i = 0; c && i < c->n; i++) if (!strcasecmp(c->tabs[i].name, name)) return &c->tabs[i]; return NULL; }
