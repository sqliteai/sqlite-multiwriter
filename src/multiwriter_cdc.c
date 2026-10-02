//
//  multiwriter_cdc.c
//  cloudsync
//
//  PROTOTYPE (docs §55): per-cell CRDT versions captured in the VFS and kept in the log-structured store (multiwriter_vsshared.c), for the restricted case of tables
//  whose primary key is the INTEGER PRIMARY KEY rowid alias, with inserts, updates and deletes; single process. At every commit:
//    prepare (lane_publish, before any lock): decode the row changes from the pages (multiwriter_rowdiff.c, the table of a page from the owner map below), and for
//      every changed cell look its previous version up in the store (an insert needs no lookup);
//    apply (publish_impl, right after the pages are installed, under their stripe locks): put the new versions (db_version = the commit's epoch) in the store, and
//      update the owner map from the interior pages the commit wrote and the pages it freed.
//  The owner map (page -> root page of the table) is built from sqlite_schema and the interior pages of every table b-tree, and kept up to date from the commits; it
//  is rebuilt when the schema cookie changes. Enabled with the URI parameter mw_cdc=1. The store starts empty at every open (a cell that is not in it counts as
//  version 1): persistence across restarts, several processes, and the sqlite-sync merge itself are not part of the prototype.
//
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include "multiwriter_internal.h"

#define OWNER_PAGES (1u << 24)                            // pages the owner map can describe (64 GB at 4 KB): one lazily committed anonymous mapping

typedef struct {
    vsh *vs;
    pthread_mutex_t mu;                                  // the build of the map
    _Atomic uint32_t *owner;
    uint32_t cookie; bool built;
    uint32_t roots[256]; bool supported[256]; int nroots; // tables of the schema; supported = INTEGER PRIMARY KEY, rowid table
    _Atomic uint64_t commits, changes, cells, lookups, put_commits, unsupported, unowned, builds, ns_prepare, ns_lookup, ns_apply, ns_build;
} mw_cdc;

static uint64_t now_ns (void) { struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec; }
static uint32_t be32 (const uint8_t *p) { return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | p[3]; }
static int be16 (const uint8_t *p) { return (p[0] << 8) | p[1]; }

int mw_cdc_open (mw_db *db) {
    mw_cdc *c = calloc(1, sizeof *c);
    if (!c) return SQLITE_NOMEM;
    char dir[700]; snprintf(dir, sizeof dir, "%s-mwvs", db->path);
    mkdir(dir, 0755);
    vsh_params p = { dir, 1000000, true, true };
    c->vs = vsh_open(&p);
    c->owner = mmap(NULL, (size_t)OWNER_PAGES * sizeof(uint32_t), PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE, -1, 0);
    if (!c->vs || c->owner == MAP_FAILED) { free(c); return SQLITE_CANTOPEN; }
    vsh_enable_bg_flush(c->vs);
    pthread_mutex_init(&c->mu, NULL);
    db->cdc = c;
    return SQLITE_OK;
}

void mw_cdc_close (mw_db *db) {
    mw_cdc *c = db->cdc;
    if (!c) return;
    if (getenv("MW_CDC_STATS")) {
        vsh_stats s; vsh_stats_get(c->vs, &s);
        fprintf(stderr, "cdc: %llu commits, %llu row changes (%llu in tables it does not handle, %llu in pages of unknown owner), %llu cells put in %llu store commits, %llu lookups; owner map built %llu times (%.1f ms); per commit: prepare %.2f us (of which store lookups %.2f us), apply %.2f us; store: %llu runs, %llu flushes (%.0f ms in the background), %llu merges\n",
                (unsigned long long)c->commits, (unsigned long long)c->changes, (unsigned long long)c->unsupported, (unsigned long long)c->unowned, (unsigned long long)c->cells, (unsigned long long)c->put_commits, (unsigned long long)c->lookups,
                (unsigned long long)c->builds, (double)c->ns_build / 1e6, c->commits ? (double)c->ns_prepare / 1000.0 / (double)c->commits : 0.0, c->commits ? (double)c->ns_lookup / 1000.0 / (double)c->commits : 0.0, c->commits ? (double)c->ns_apply / 1000.0 / (double)c->commits : 0.0,
                (unsigned long long)s.runs, (unsigned long long)s.flushes, (double)s.flush_ns / 1e6, (unsigned long long)s.merges);
    }
    vsh_close(c->vs);
    munmap((void *)c->owner, (size_t)OWNER_PAGES * sizeof(uint32_t));
    pthread_mutex_destroy(&c->mu);
    free(c);
    db->cdc = NULL;
}

static uint32_t old_owner (void *ctx, uint32_t pgno) { mw_cdc *c = ctx; return pgno < OWNER_PAGES ? atomic_load_explicit(&c->owner[pgno], memory_order_relaxed) : 0; }

// ---- the owner map ----
static void own_set (mw_cdc *c, uint32_t pg, uint32_t root) { if (pg && pg < OWNER_PAGES) atomic_store_explicit(&c->owner[pg], root, memory_order_relaxed); }

// text column of a record's local part (copied, NUL-terminated); type codes as in the record format
static bool rec_text (const uint8_t *rec, uint32_t len, int col, char *out, size_t cap) {
    const uint8_t *p = rec, *end = rec + len; uint64_t hs = 0; int sh = 0;
    // header size varint
    int hl = 0; for (; hl < 9 && p + hl < end; hl++) { hs = (hs << 7) | (p[hl] & 0x7f); if (!(p[hl] & 0x80)) { hl++; break; } sh++; }
    uint32_t pos = (uint32_t)hs; uint32_t hp = (uint32_t)hl; int idx = 0;
    while (hp < hs && hp < len) {
        uint64_t t = 0; int k = 0; for (; k < 9 && p + hp + k < end; k++) { t = (t << 7) | (p[hp + k] & 0x7f); if (!(p[hp + k] & 0x80)) { k++; break; } }
        hp += (uint32_t)k;
        uint32_t l = t < 12 ? ((const uint8_t[]){0, 1, 2, 3, 4, 6, 8, 8, 0, 0, 0, 0})[t] : (uint32_t)((t - 12) / 2);
        if (idx == col) {
            if (t >= 13 && (t & 1) && pos + l <= len) { size_t n = l < cap - 1 ? l : cap - 1; memcpy(out, rec + pos, n); out[n] = 0; return true; }
            if (t >= 1 && t <= 6 && pos + l <= len) { int64_t v = (int8_t)rec[pos]; for (uint32_t b = 1; b < l; b++) v = (v << 8) | rec[pos + b]; snprintf(out, cap, "%lld", (long long)v); return true; }
            return false;
        }
        pos += l; idx++;
    }
    (void)sh;
    return false;
}

static bool contains_ci (const char *h, const char *n) { size_t ln = strlen(n); for (; *h; h++) if (strncasecmp(h, n, ln) == 0) return true; return false; }

static void schema_leaf (mw_cdc *c, const uint8_t *pg, uint32_t pgsz, bool page1) {
    uint32_t base = page1 ? 100 : 0; int nc = be16(pg + base + 3);
    for (int i = 0; i < nc; i++) {
        uint32_t off = (uint32_t)be16(pg + base + 8 + 2 * i); if (off >= pgsz) continue;
        const uint8_t *cp = pg + off, *end = pg + pgsz; uint64_t P = 0, rid = 0; int a = 0, b = 0;
        for (; a < 9 && cp + a < end; a++) { P = (P << 7) | (cp[a] & 0x7f); if (!(cp[a] & 0x80)) { a++; break; } }
        for (; b < 9 && cp + a + b < end; b++) { rid = (rid << 7) | (cp[a + b] & 0x7f); if (!(cp[a + b] & 0x80)) { b++; break; } }
        uint32_t local = P > pgsz - 35 ? 0 : (uint32_t)P; if (!local) continue;                       // (a schema row that spills to overflow: not handled)
        const uint8_t *rec = cp + a + b; char type[16], name[128], root[24], sql[1024];
        if (!rec_text(rec, local, 0, type, sizeof type) || strcmp(type, "table") != 0) continue;
        if (!rec_text(rec, local, 3, root, sizeof root)) continue;
        uint32_t r = (uint32_t)strtoul(root, NULL, 10); if (r <= 1 || c->nroots >= 256) continue;
        (void)name; bool ok = rec_text(rec, local, 4, sql, sizeof sql) && contains_ci(sql, "INTEGER PRIMARY KEY") && !contains_ci(sql, "WITHOUT ROWID");
        c->roots[c->nroots] = r; c->supported[c->nroots] = ok; c->nroots++;
    }
}
static void schema_walk (mw_cdc *c, mw_lane *lane, uint32_t pgno, uint32_t pgsz, uint8_t *buf, int depth) {
    uint8_t *pg = malloc(pgsz); if (!pg || depth > 6) { free(pg); return; }
    if (!mw_rd_snap_page(lane, pgno, pg)) { free(pg); return; }
    uint32_t base = pgno == 1 ? 100 : 0;
    if (pg[base] == 0x0d) schema_leaf(c, pg, pgsz, pgno == 1);
    else if (pg[base] == 0x05) { int nc = be16(pg + base + 3); for (int i = 0; i < nc; i++) schema_walk(c, lane, be32(pg + (uint32_t)be16(pg + base + 12 + 2 * i)), pgsz, buf, depth + 1); schema_walk(c, lane, be32(pg + base + 8), pgsz, buf, depth + 1); }
    free(pg);
}
// owner[] for the pages of one table b-tree: interior pages are read, the leaves below the last interior level are only named by their parent
static void tree_walk (mw_cdc *c, mw_lane *lane, uint32_t root, uint32_t pgsz, int levels_left_to_leaf, uint32_t pgno, uint8_t *pg) {
    own_set(c, pgno, root);
    if (levels_left_to_leaf == 0) return;
    if (!mw_rd_snap_page(lane, pgno, pg)) return;
    int nc = be16(pg + 3); uint32_t kids[8192]; int nk = 0;
    for (int i = 0; i < nc && nk < 8000; i++) kids[nk++] = be32(pg + (uint32_t)be16(pg + 12 + 2 * i));
    kids[nk++] = be32(pg + 8);
    for (int i = 0; i < nk; i++) {
        if (levels_left_to_leaf == 1) own_set(c, kids[i], root);
        else tree_walk(c, lane, root, pgsz, levels_left_to_leaf - 1, kids[i], pg);        // (pg is read again by the recursion: it is a scratch page, kids[] is on the stack)
    }
}
static void build (mw_cdc *c, mw_lane *lane, uint32_t cookie) {
    uint64_t t0 = now_ns(); uint32_t pgsz = (uint32_t)lane->db->store->pgsz;
    memset((void *)c->owner, 0, (size_t)lane->dsz_val * sizeof(uint32_t) < (size_t)OWNER_PAGES * 4 ? (size_t)lane->dsz_val * sizeof(uint32_t) + 4096 : (size_t)OWNER_PAGES * 4);
    c->nroots = 0;
    schema_walk(c, lane, 1, pgsz, NULL, 0);
    uint8_t *pg = malloc(pgsz);
    for (int t = 0; t < c->nroots && pg; t++) {
        uint32_t depth = 0, p = c->roots[t];
        for (;;) { if (!mw_rd_snap_page(lane, p, pg)) break; if (pg[0] != 0x05) break; depth++; p = be32(pg + 12 > pgsz ? pg : pg + (uint32_t)be16(pg + 12) ); if (depth > 8) break; }
        tree_walk(c, lane, c->roots[t], pgsz, (int)depth, c->roots[t], pg);
    }
    free(pg);
    c->cookie = cookie; c->built = true; atomic_fetch_add(&c->builds, 1); atomic_fetch_add(&c->ns_build, now_ns() - t0);
}
static int root_index (const mw_cdc *c, uint32_t root) { for (int i = 0; i < c->nroots; i++) if (c->roots[i] == root) return i; return -1; }

// ---- per commit ----
void mw_cdc_prepare (mw_lane *lane, const uint8_t *const *imgs) {
    mw_cdc *c = lane->db->cdc; uint64_t t0 = now_ns();
    lane->cdc_n = 0; lane->cdc_nfreed = 0;
    uint8_t *p1 = malloc((size_t)lane->db->store->pgsz);
    if (!p1 || !mw_rd_snap_page(lane, 1, p1)) { free(p1); return; }
    uint32_t cookie = be32(p1 + 40); free(p1);
    if (!c->built || c->cookie != cookie) { pthread_mutex_lock(&c->mu); if (!c->built || c->cookie != cookie) build(c, lane, cookie); pthread_mutex_unlock(&c->mu); }
    mw_rd_result r; mw_rd_owner own = { c, old_owner };
    mw_rowdiff_compute(lane, imgs, &own, &r);
    atomic_fetch_add(&c->commits, 1); atomic_fetch_add(&c->changes, (uint64_t)r.n);
    uint64_t tl = 0, nl = 0;
    for (int i = 0; i < r.n; i++) {
        const mw_rowchg *ch = &r.chg[i];
        int ri = ch->root ? root_index(c, ch->root) : -1;
        if (ri < 0) { atomic_fetch_add(ch->root ? &c->unsupported : &c->unowned, 1); continue; }
        if (!c->supported[ri]) { atomic_fetch_add(&c->unsupported, 1); continue; }
        int ncols = ch->ncols > 32 ? 32 : ch->ncols;
        int cols[33], nc = 0;
        if (ch->kind == 3) cols[nc++] = 0xFFFF;                                                    // a delete: the row's causal length
        else for (int col = 1; col < ncols; col++) if (ch->kind == 1 || (ch->changed & 0x80000000u) || (ch->changed >> col & 1)) cols[nc++] = col;
        for (int k = 0; k < nc; k++) {
            if (lane->cdc_n == lane->cdc_cap) { int cap = lane->cdc_cap ? lane->cdc_cap * 2 : 256; lane->cdc_keys = realloc(lane->cdc_keys, (size_t)cap * sizeof *lane->cdc_keys); lane->cdc_vals = realloc(lane->cdc_vals, (size_t)cap * sizeof *lane->cdc_vals); lane->cdc_cap = cap; }
            vsh_key key = { (uint64_t)ch->root << 32 | (uint64_t)cols[k], (uint64_t)ch->rowid }; vsh_val old = { 0, 0 };
            if (ch->kind != 1) { uint64_t a = now_ns(); if (!vsh_get(c->vs, key, &old)) old = (vsh_val){ 0, 0 }; tl += now_ns() - a; nl++; }
            lane->cdc_keys[lane->cdc_n] = key; lane->cdc_vals[lane->cdc_n] = (vsh_val){ old.cv + 1, 0 }; lane->cdc_n++;
        }
    }
    lane->cdc_freed = realloc(lane->cdc_freed, (size_t)(r.nfreed + 1) * sizeof(uint32_t)); if (r.nfreed) memcpy(lane->cdc_freed, r.freed, (size_t)r.nfreed * sizeof(uint32_t)); lane->cdc_nfreed = r.nfreed;
    free(r.chg); free(r.freed);
    atomic_fetch_add(&c->lookups, nl); atomic_fetch_add(&c->ns_lookup, tl); atomic_fetch_add(&c->ns_prepare, now_ns() - t0);
}

// under the stripe locks of the pages: the owner map follows the commit (two commits of one interior page apply in epoch order)
void mw_cdc_apply_owner (mw_db *db, mw_lane *lane, const uint32_t *pgnos, const uint8_t *const *images, int n) {
    mw_cdc *c = db->cdc; uint64_t t0 = now_ns();
    uint32_t pgsz = (uint32_t)db->store->pgsz;
    for (int i = 0; i < n; i++) {                                                       // the interior pages this commit wrote: their children belong to the table of the page
        const uint8_t *pg = images[i]; if (pgnos[i] == 1 || pg[0] != 0x05) continue;
        uint32_t r = old_owner(c, pgnos[i]); if (!r) continue;
        int nc = be16(pg + 3);
        own_set(c, be32(pg + 8), r);
        for (int k = 0; k < nc; k++) { uint32_t off = (uint32_t)be16(pg + 12 + 2 * k); if (off + 4 <= pgsz) own_set(c, be32(pg + off), r); }
    }
    for (int i = 0; i < lane->cdc_nfreed; i++) own_set(c, lane->cdc_freed[i], 0);
    lane->cdc_nfreed = 0;
    atomic_fetch_add(&c->ns_apply, now_ns() - t0);
}

// after the record is in the log and before the commit becomes visible: a transaction that starts after this one is visible looks its previous versions up after these
// puts, and one that does not see it conflicts on the pages (the same cell is on the same page)
void mw_cdc_apply_cells (mw_db *db, mw_lane *lane, uint64_t epoch) {
    mw_cdc *c = db->cdc; uint64_t t0 = now_ns();
    if (lane->cdc_n) {
        for (int i = 0; i < lane->cdc_n; i++) lane->cdc_vals[i].dv = (uint32_t)epoch;
        for (;;) { vsh_wait_room(c->vs); vsh_lock(c->vs); if (vsh_commit_locked(c->vs, lane->cdc_keys, lane->cdc_vals, lane->cdc_n)) break; vsh_unlock(c->vs); }
        vsh_unlock_flush(c->vs);
        atomic_fetch_add(&c->cells, (uint64_t)lane->cdc_n); atomic_fetch_add(&c->put_commits, 1);
    }
    lane->cdc_n = 0;
    atomic_fetch_add(&c->ns_apply, now_ns() - t0);
}

int mw_cdc_get (mw_db *db, mw_cdc_cell *cell) {
    mw_cdc *c = db->cdc;
    if (!c) return SQLITE_NOTFOUND;
    vsh_val v;
    cell->found = vsh_get(c->vs, (vsh_key){ (uint64_t)cell->table_root << 32 | (uint64_t)(uint32_t)cell->col, (uint64_t)cell->rowid }, &v) ? 1 : 0;
    cell->cv = cell->found ? v.cv : 0; cell->dv = cell->found ? v.dv : 0;
    return SQLITE_OK;
}
