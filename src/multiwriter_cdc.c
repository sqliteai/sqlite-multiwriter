//
//  multiwriter_cdc.c
//
//  Change capture inside the commit (docs/design.md). At every commit of a private lane:
//    prepare (lane_publish, before any lock; a retried transaction pays it again): the net row changes of the write set against the snapshot, with the table of every page from
//      the owner map and the layout of every table from the catalog;
//    apply (publish_impl): the owner map follows the commit (under the stripe locks of its pages, so two commits of one interior page apply in epoch order); the cells go to the
//      metadata store before the commit becomes visible.
//  The owner map (page -> root page of its table, 4 bytes per page, one lazily committed anonymous mapping) is built from sqlite_schema and, for every table b-tree, its interior
//  pages only; rebuilt when the schema cookie moves; kept up to date from the commits.  Enabled with the URI parameter mw_cdc=1.
//
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <strings.h>
#include <fcntl.h>
#include <unistd.h>
#include "multiwriter_internal.h"
#include "multiwriter_meta.h"

#define OWNER_PAGES (1u << 24)                            // pages the owner map can describe (64 GB at 4 KB)

typedef struct {
    pthread_mutex_t mu;                                  // the (re)build of the map and of the catalog
    _Atomic uint32_t *owner;                             // page -> root page of its table
    _Atomic uint32_t *ovo;                               // overflow page -> the leaf (or index) page holding the cell whose record spills into it
    bool ovfl_complete;                                  // ovo covers every record with overflow of the database (built by a scan on the first overflow page written without its cell)
    mw_cat *cat; uint32_t cookie; bool built;
    mw_meta *meta; bool ready;  // the CRDT metadata; ready once the extensions found in the log at recovery are applied
    mw_cdc_sink_fn sink; void *sink_arg;                  // tests: every commit's changes
    _Atomic uint64_t commits, changes, unowned, builds, ovfl_scans, ovfl_unattributed, ns_prepare, ns_build;
} mw_cdc;

static uint64_t now_ns (void) { struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec; }
static uint32_t be32 (const uint8_t *p) { return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | p[3]; }
static int be16 (const uint8_t *p) { return (p[0] << 8) | p[1]; }

int mw_cdc_open (mw_db *db) {
    mw_cdc *c = calloc(1, sizeof *c);
    if (!c) return SQLITE_NOMEM;
    if (!db->shared) {                                                       // (shared mode: the maps are a file mapped by every process, attached at the first use)
        c->owner = mmap(NULL, (size_t)OWNER_PAGES * sizeof(uint32_t), PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE, -1, 0);
        c->ovo = mmap(NULL, (size_t)OWNER_PAGES * sizeof(uint32_t), PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE, -1, 0);
        if (c->owner == MAP_FAILED || c->ovo == MAP_FAILED) { free(c); return SQLITE_NOMEM; }
    }
    pthread_mutex_init(&c->mu, NULL);
    c->meta = mw_meta_new(db);
    if (!c->meta) { free(c); return SQLITE_NOMEM; }
    mw_meta_attach(c->meta, db->path, db->mode, db->shared ? 2 : db->mp_req ? 3 : 0);
    db->cdc = c;
    return SQLITE_OK;
}

void mw_cdc_close (mw_db *db) {
    mw_cdc *c = db->cdc;
    if (!c) return;
    if (getenv("MW_CDC_STATS")) fprintf(stderr, "cdc: %llu commits, %llu row changes (%llu in pages of unknown owner), catalog/owner map built %llu times (%.1f ms), overflow map scanned %llu times, %llu overflow writes unattributed; prepare %.2f us per commit\n",
        (unsigned long long)c->commits, (unsigned long long)c->changes, (unsigned long long)c->unowned, (unsigned long long)c->builds, (double)c->ns_build / 1e6, (unsigned long long)c->ovfl_scans, (unsigned long long)c->ovfl_unattributed, c->commits ? (double)c->ns_prepare / 1000.0 / (double)c->commits : 0.0);
    mw_cat_free(c->cat); mw_meta_free(c->meta);
    if (db->shared) { if (c->owner) munmap((void *)c->owner, 2 * (size_t)OWNER_PAGES * sizeof(uint32_t)); }
    else { munmap((void *)c->owner, (size_t)OWNER_PAGES * sizeof(uint32_t)); munmap((void *)c->ovo, (size_t)OWNER_PAGES * sizeof(uint32_t)); }
    pthread_mutex_destroy(&c->mu);
    free(c);
    db->cdc = NULL;
}

// ---- the owner maps in shared mode: one file, mapped by every process; reset by the first opener ----
#define OWN_FILE_BYTES (2 * (size_t)OWNER_PAGES * sizeof(uint32_t))
int mw_cdc_shared_create (mw_db *db) {                                      // the first opener of the database
    char *p = sqlite3_mprintf("%s-mwown", db->path); if (!p) return SQLITE_NOMEM;
    unlink(p);
    int fd = open(p, O_RDWR | O_CREAT, 0644); sqlite3_free(p);
    if (fd < 0) return SQLITE_CANTOPEN;
    int rc = ftruncate(fd, (off_t)OWN_FILE_BYTES) == 0 ? SQLITE_OK : SQLITE_IOERR;
    close(fd); return rc;
}
void mw_cdc_shared_unlink (mw_db *db) { char *p = sqlite3_mprintf("%s-mwown", db->path); if (p) { unlink(p); sqlite3_free(p); } }
static int maps_attach (mw_db *db, mw_cdc *c) {
    if (c->owner) return SQLITE_OK;
    pthread_mutex_lock(&c->mu);
    if (!c->owner) {
        char *p = sqlite3_mprintf("%s-mwown", db->path); int fd = p ? open(p, O_RDWR) : -1; sqlite3_free(p);
        if (fd >= 0) { void *m = mmap(NULL, OWN_FILE_BYTES, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0); close(fd); if (m != MAP_FAILED) { c->ovo = (_Atomic uint32_t *)m + OWNER_PAGES; c->owner = m; } }
    }
    pthread_mutex_unlock(&c->mu);
    return c->owner ? SQLITE_OK : SQLITE_CANTOPEN;
}
static bool ovfl_done (mw_db *db, mw_cdc *c) { return db->shared ? atomic_load(&db->shm->own_ovfl_complete) != 0 : c->ovfl_complete; }
static void ovfl_set (mw_db *db, mw_cdc *c, bool v) { if (db->shared) atomic_store(&db->shm->own_ovfl_complete, v ? 1u : 0u); else c->ovfl_complete = v; }

void mw_cdc_set_sink (mw_db *db, mw_cdc_sink_fn fn, void *arg) { mw_cdc *c = db->cdc; if (c) { c->sink = fn; c->sink_arg = arg; } }

// the public form of a sink
static mw_capture_sink g_public;
static void public_sink (void *arg, const mw_chg *chg, int n, const mw_rowdiff_info *info) {
    (void)arg; (void)info;
    mw_capture_row *r = malloc(((size_t)n + 1) * sizeof *r); if (!r) return;
    for (int i = 0; i < n; i++) r[i] = (mw_capture_row){ chg[i].kind, chg[i].tab ? chg[i].tab->name : NULL, chg[i].pk, chg[i].pklen, chg[i].oldpk, chg[i].oldpklen, chg[i].changed, chg[i].rowid };
    g_public.fn(g_public.arg, r, n); free(r);
}
int mw_cdc_set_public_sink (mw_db *db, const mw_capture_sink *s) { if (!db->cdc) return SQLITE_NOTFOUND; g_public = *s; mw_cdc_set_sink(db, s->fn ? public_sink : NULL, NULL); return SQLITE_OK; }

static uint32_t old_owner (void *ctx, uint32_t pgno) { mw_cdc *c = ctx; return pgno < OWNER_PAGES ? atomic_load_explicit(&c->owner[pgno], memory_order_relaxed) : 0; }
static uint32_t ovfl_owner (void *ctx, uint32_t pgno) { mw_cdc *c = ctx; return pgno < OWNER_PAGES ? atomic_load_explicit(&c->ovo[pgno], memory_order_relaxed) : 0; }
static void ovo_set (mw_cdc *c, uint32_t pg, uint32_t leaf) { if (pg && pg < OWNER_PAGES) atomic_store_explicit(&c->ovo[pg], leaf, memory_order_relaxed); }
static void own_set (mw_cdc *c, uint32_t pg, uint32_t root) { if (pg && pg < OWNER_PAGES) atomic_store_explicit(&c->owner[pg], root, memory_order_relaxed); }

// owner[] for the pages of one table b-tree: interior pages are read, the pages below the last interior level are only named by their parent
static void tree_walk (mw_cdc *c, mw_lane *lane, uint32_t root, uint32_t levels_left, uint32_t pgno, uint8_t *pg) {
    own_set(c, pgno, root);
    if (levels_left == 0 || !mw_rd_snap_page(lane, pgno, pg)) return;
    int nc = be16(pg + 3); uint32_t *kids = malloc(((size_t)nc + 1) * sizeof *kids); if (!kids) return;
    for (int i = 0; i < nc; i++) kids[i] = be32(pg + (uint32_t)be16(pg + 12 + 2 * i));
    kids[nc] = be32(pg + 8);
    for (int i = 0; i <= nc; i++) { if (levels_left == 1) own_set(c, kids[i], root); else tree_walk(c, lane, root, levels_left - 1, kids[i], pg); }
    free(kids);
}
static void owner_walk (mw_cdc *c, mw_lane *lane, const mw_cat *cat);
static void build (mw_cdc *c, mw_lane *lane) {
    uint64_t t0 = now_ns();
    mw_cat *cat = mw_cat_build(lane); if (!cat) return;
    if (!lane->db->shared) owner_walk(c, lane, cat);
    mw_cat_free(c->cat); c->cat = cat; c->cookie = cat->cookie; c->built = true;
    atomic_fetch_add(&c->builds, 1); atomic_fetch_add(&c->ns_build, now_ns() - t0);
}
// the owner map of the committed state of the lane (the lane's snapshot): every table b-tree, interior pages only
static void owner_walk (mw_cdc *c, mw_lane *lane, const mw_cat *cat) {
    uint32_t pgsz = (uint32_t)lane->db->store->pgsz;
    size_t n = (size_t)lane->dsz_val * sizeof(uint32_t) + 4096; if (n > (size_t)OWNER_PAGES * 4) n = (size_t)OWNER_PAGES * 4;
    memset((void *)c->owner, 0, n);
    uint8_t *pg = malloc(pgsz);
    for (int t = 0; t < cat->n && pg; t++) {
        uint32_t depth = 0, p = cat->tabs[t].root;
        for (;;) { if (!mw_rd_snap_page(lane, p, pg)) break; if (pg[0] != 0x05 && pg[0] != 0x02) break; depth++; if (depth > 8) break; p = be32(pg + (uint32_t)be16(pg + 12)); }
        tree_walk(c, lane, cat->tabs[t].root, depth, cat->tabs[t].root, pg);
    }
    free(pg);
}

// the overflow owner map by a scan: every record with overflow in every table b-tree, at the lane's snapshot
static void scan_cells (mw_cdc *c, mw_lane *lane, const uint8_t *pg, uint32_t pgno, uint32_t pgsz, bool tableleaf, bool interior) {
    const uint32_t U = pgsz, M = ((U - 12) * 32 / 255) - 23, X = tableleaf ? U - 35 : ((U - 12) * 64 / 255) - 23;
    int hdr = interior ? 12 : 8, nc = be16(pg + 3);
    uint8_t *tmp = malloc(pgsz); if (!tmp) return;
    for (int i = 0; i < nc; i++) {
        uint32_t off = (uint32_t)be16(pg + hdr + 2 * i); if (off < (uint32_t)hdr || off >= pgsz) continue;
        const uint8_t *cp = pg + off, *end = pg + pgsz; if (interior) cp += 4;
        uint64_t P = 0, rid = 0; int a = 0, b = 0;
        for (; a < 9 && cp + a < end; a++) { P = (P << 7) | (cp[a] & 0x7f); if (!(cp[a] & 0x80)) { a++; break; } }
        if (tableleaf) for (; b < 9 && cp + a + b < end; b++) { rid = (rid << 7) | (cp[a + b] & 0x7f); if (!(cp[a + b] & 0x80)) { b++; break; } }
        if (P <= X) continue;
        uint32_t K = M + (uint32_t)((P - M) % (U - 4)), local = K <= X ? K : M;
        if (cp + a + b + local + 4 > end) continue;
        uint32_t ovfl = be32(cp + a + b + local); uint64_t got = local;
        for (int g = 0; ovfl && got < P && g < 100000000; g++) { ovo_set(c, ovfl, pgno); if (!mw_rd_snap_page(lane, ovfl, tmp)) break; got += pgsz - 4; ovfl = be32(tmp); }
    }
    free(tmp);
}
static void scan_tree (mw_cdc *c, mw_lane *lane, const mw_tab *t, uint32_t pgno, uint32_t pgsz, int depth) {
    uint8_t *pg = malloc(pgsz); if (!pg || depth > 12) { free(pg); return; }
    if (mw_rd_snap_page(lane, pgno, pg)) {
        uint8_t k = pg[0];
        if (k == 0x0d && !t->without_rowid) scan_cells(c, lane, pg, pgno, pgsz, true, false);
        else if (k == 0x0a && t->without_rowid) scan_cells(c, lane, pg, pgno, pgsz, false, false);
        else if (k == 0x02 && t->without_rowid) scan_cells(c, lane, pg, pgno, pgsz, false, true);
        if (k == 0x05 || k == 0x02) {
            int nc = be16(pg + 3); uint32_t *kids = malloc(((size_t)nc + 1) * sizeof *kids);
            if (kids) { for (int i = 0; i < nc; i++) kids[i] = be32(pg + (uint32_t)be16(pg + 12 + 2 * i)); kids[nc] = be32(pg + 8); free(pg); pg = NULL; for (int i = 0; i <= nc; i++) scan_tree(c, lane, t, kids[i], pgsz, depth + 1); free(kids); }
        }
    }
    free(pg);
}
static void build_ovfl (mw_cdc *c, mw_lane *lane, const mw_cat *cat) {
    uint32_t pgsz = (uint32_t)lane->db->store->pgsz;
    size_t n = (size_t)lane->dsz_val * sizeof(uint32_t) + 4096; if (n > (size_t)OWNER_PAGES * 4) n = (size_t)OWNER_PAGES * 4;
    memset((void *)c->ovo, 0, n);
    for (int t = 0; t < cat->n; t++) if (cat->tabs[t].tracked) scan_tree(c, lane, &cat->tabs[t], cat->tabs[t].root, pgsz, 0);
    ovfl_set(lane->db, c, true); atomic_fetch_add(&c->ovfl_scans, 1);
}

static uint32_t cookie_of (mw_lane *lane) {                                 // the schema cookie of the lane's snapshot: 4 bytes of page 1
    uint8_t b[4]; mw_store *st = lane->db->store;
    if (lane->cdc_over) { for (int i = 0; i < lane->ws_n; i++) if (lane->ws_pgnos[i] == 1) return be32(lane->cdc_over[i] + 40); }
    if (lane->dsz_val >= 1 && mw_store_read(st, 1, lane->tx.snapshot_epoch, 40, 4, b)) return be32(b);
    uint8_t *p1 = malloc((size_t)st->pgsz); uint32_t k = 0;
    if (p1 && mw_rd_snap_page(lane, 1, p1)) k = be32(p1 + 40);
    free(p1); return k;
}

// Shared mode: the maps are one for everybody and describe the newest committed state; they are rebuilt (by whoever needs it first) under the publication lock, so that no commit
// updates them meanwhile. The lane's schema must be the maps' schema: an older one (a transaction that started before a schema change) cannot commit anyway.
typedef struct { uint64_t snap, dsz_epoch; uint32_t dsz_val; bool dsz_valid; } snap_save;
static void snap_latest (mw_lane *lane, snap_save *sv) {
    sv->snap = lane->tx.snapshot_epoch; sv->dsz_epoch = lane->dsz_epoch; sv->dsz_val = lane->dsz_val; sv->dsz_valid = lane->dsz_valid;
    uint64_t E = mw_shared_visible_epoch(lane->db);
    lane->tx.snapshot_epoch = E; lane->dsz_val = mw_store_dbsize(lane->db->store, E); lane->dsz_epoch = E; lane->dsz_valid = true;
}
static void snap_restore (mw_lane *lane, const snap_save *sv) { lane->tx.snapshot_epoch = sv->snap; lane->dsz_epoch = sv->dsz_epoch; lane->dsz_val = sv->dsz_val; lane->dsz_valid = sv->dsz_valid; }

static bool shared_maps (mw_cdc *c, mw_lane *lane, const mw_cat *cat) {
    mw_db *db = lane->db; mw_shm *sh = db->shm;
    if (maps_attach(db, c) != SQLITE_OK) return false;
    uint64_t want = (1ull << 32) | cat->cookie, cur = atomic_load_explicit(&sh->own_cookie, memory_order_acquire);
    if (cur == want) return true;
    if (cur != 0 && (int32_t)(cat->cookie - (uint32_t)cur) < 0) return false;
    mw_mp_lock(db);
    cur = atomic_load(&sh->own_cookie); bool ok = true;
    if (cur != want) {
        if (cur != 0 && (int32_t)(cat->cookie - (uint32_t)cur) < 0) ok = false;
        else {
            uint64_t t0 = now_ns(); snap_save sv; snap_latest(lane, &sv);
            if (cookie_of(lane) != cat->cookie) ok = false;                  // the schema moved on since this lane's snapshot
            else { owner_walk(c, lane, cat); ovfl_set(db, c, false); atomic_store_explicit(&sh->own_cookie, want, memory_order_release); atomic_fetch_add(&c->builds, 1); atomic_fetch_add(&c->ns_build, now_ns() - t0); }
            snap_restore(lane, &sv);
        }
    }
    mw_mp_unlock(db);
    return ok;
}
static void shared_build_ovfl (mw_cdc *c, mw_lane *lane, const mw_cat *cat) {
    mw_db *db = lane->db;
    mw_mp_lock(db);
    if (!ovfl_done(db, c)) { snap_save sv; snap_latest(lane, &sv); build_ovfl(c, lane, cat); snap_restore(lane, &sv); }
    mw_mp_unlock(db);
}

// the catalog current for this lane's snapshot (a reference the caller drops)
static mw_cat *catalog_for (mw_cdc *c, mw_lane *lane) {
    uint32_t cookie = cookie_of(lane);
    pthread_mutex_lock(&c->mu);
    if (!c->built || c->cookie != cookie) build(c, lane);
    mw_cat *cat = mw_cat_ref(c->cat);
    pthread_mutex_unlock(&c->mu);
    return cat;
}

static void build_delta (mw_lane *lane, mw_cdc *c, const uint32_t *purge, int npurge);
static void ensure_ready (mw_db *db, mw_cdc *c);

// ---- DDL in the commit ----
typedef struct { const char **skip; int nskip; const char **skip_old; int nskip_old; uint32_t *purge; int npurge; } ddl_plan;
// What the schema change of this commit does to the tables, by name. A table that is in both catalogs is the same table: its rows are diffed by content (cells of an old and a new
// layout are matched by column name; VACUUM rebuilds every table with the same rows, which nets to nothing). A table that is gone: its cells go too (DROP TABLE is not a delete of its rows
// at the peers; sqlite-sync's cleanup does the same), and its rows are not diffed. A table that is new: its rows are inserts.
static void plan_ddl (const mw_cat *oc, const mw_cat *nc, ddl_plan *pl) {
    int cap = oc->n + 1;
    pl->skip_old = calloc((size_t)cap, sizeof *pl->skip_old); pl->purge = calloc((size_t)cap, sizeof *pl->purge);
    if (!pl->skip_old || !pl->purge) return;
    for (int i = 0; i < oc->n; i++) { const mw_tab *o = &oc->tabs[i]; if (!o->tracked || mw_cat_by_name(nc, o->name)) continue; pl->skip_old[pl->nskip_old++] = o->name; if (o->synced) pl->purge[pl->npurge++] = o->tid; }
}

// ---- per commit ----
void mw_cdc_prepare (mw_lane *lane, const uint8_t *const *imgs) {
    mw_cdc *c = lane->db->cdc; uint64_t t0 = now_ns();
    mw_rd_result_free(&lane->cdc_res); lane->cdc_nfreed = 0;
    mw_cat_free(lane->cdc_cat); lane->cdc_cat = NULL;                           // (a retried commit prepares again: the catalog of the earlier attempt goes)
    mw_cat *cat = catalog_for(c, lane), *newcat = NULL;
    lane->cdc_ng = 0;
    free(lane->cdc_ext); lane->cdc_ext = NULL; lane->cdc_ext_len = 0;
    if (lane->db->shared && cat && !shared_maps(c, lane, cat)) { mw_cat_free(cat); lane->cdc_cat = NULL; return; }       // (this lane's schema is stale: its commit is refused)
    ddl_plan plan = {0};
    const uint8_t *p1new = NULL; for (int i = 0; i < lane->ws_n; i++) if (lane->ws_pgnos[i] == 1) p1new = imgs[i];
    if (p1new && cat && be32(p1new + 40) != cat->cookie) {                       // the schema changes in this commit: the rows of the new tables, the dropped tables, VACUUM
        lane->cdc_over = imgs; newcat = mw_cat_build(lane); lane->cdc_over = NULL;
        if (newcat) plan_ddl(cat, newcat, &plan);
    }
    free(plan.skip);                                                            // (set below for VACUUM)
    plan.skip = NULL; plan.nskip = 0;
    if (lane->cdc_vacuum && newcat) { plan.skip = calloc((size_t)(cat->n + 1), sizeof *plan.skip); if (plan.skip) for (int i = 0; i < cat->n; i++) plan.skip[plan.nskip++] = cat->tabs[i].name; }       // VACUUM: every table is rebuilt with the same rows
    mw_rd_owner own = { c, old_owner, ovfl_owner, newcat ? newcat : cat, newcat ? cat : NULL, NULL, 0 };
    lane->cdc_skip = plan.skip; lane->cdc_nskip = plan.nskip; lane->cdc_skip_old = plan.skip_old; lane->cdc_nskip_old = plan.nskip_old;
    mw_rowdiff_compute(lane, imgs, &own, &lane->cdc_res);
    if (lane->cdc_res.unknown_ovfl && !ovfl_done(lane->db, c)) {                          // an overflow page written without its cell and nobody to ask: scan once, then ask again
        if (lane->db->shared) shared_build_ovfl(c, lane, newcat ? newcat : cat);
        else { pthread_mutex_lock(&c->mu); if (!c->ovfl_complete) build_ovfl(c, lane, newcat ? newcat : cat); pthread_mutex_unlock(&c->mu); }
        mw_rd_result_free(&lane->cdc_res); mw_rowdiff_compute(lane, imgs, &own, &lane->cdc_res);
    }
    lane->cdc_skip = NULL; lane->cdc_nskip = 0; lane->cdc_skip_old = NULL; lane->cdc_nskip_old = 0;
    if (lane->cdc_res.unknown_ovfl) atomic_fetch_add(&c->ovfl_unattributed, (uint64_t)lane->cdc_res.unknown_ovfl);
    atomic_fetch_add(&c->commits, 1); atomic_fetch_add(&c->changes, (uint64_t)lane->cdc_res.n);
    for (int i = 0; i < lane->cdc_res.n; i++) if (!lane->cdc_res.chg[i].tab) atomic_fetch_add(&c->unowned, 1);
    if (c->sink) c->sink(c->sink_arg, lane->cdc_res.chg, lane->cdc_res.n, &lane->cdc_res.info);
    if (lane->cdc_decl) {                                                      // a merge of remote changes: the metadata is the one the merge produced
        free(lane->cdc_ext); lane->cdc_ext = NULL; lane->cdc_ext_len = 0;
        ensure_ready(lane->db, c);
        if (mw_ovl_encode(lane->cdc_decl, &lane->cdc_ext, &lane->cdc_ext_len) != 0) { lane->cdc_ext = NULL; lane->cdc_ext_len = 0; }
    } else build_delta(lane, c, plan.purge, plan.npurge);
    { mw_ovl *act = lane->cdc_decl ? lane->cdc_decl : lane->cdc_ovl; if (act && lane->cdc_ext_len) lane->cdc_ng = mw_ovl_groups(act, (const uint32_t **)&lane->cdc_gbucket, (const uint32_t **)&lane->cdc_goff, (const uint64_t **)&lane->cdc_gseen); }
    free(plan.skip); free(plan.skip_old); free(plan.purge);
    // the rows point into the catalog they were decoded with: it stays alive until the apply step of the same commit
    if (newcat) { mw_cat_free(cat); lane->cdc_cat = newcat; } else lane->cdc_cat = cat;
    atomic_fetch_add(&c->ns_prepare, now_ns() - t0);
}

// under the stripe locks of the pages: the owner map follows the commit
void mw_cdc_apply_owner (mw_db *db, mw_lane *lane, const uint32_t *pgnos, const uint8_t *const *images, int n) {
    mw_cdc *c = db->cdc;
    if (!c->owner && maps_attach(db, c) != SQLITE_OK) return;
    uint32_t pgsz = (uint32_t)db->store->pgsz;
    for (int i = 0; i < n; i++) {                                                       // the interior pages this commit wrote: their children belong to the table of the page
        const uint8_t *pg = images[i]; if (pgnos[i] == 1 || (pg[0] != 0x05 && pg[0] != 0x02)) continue;
        uint32_t r = old_owner(c, pgnos[i]); if (!r) continue;
        int nc = be16(pg + 3);
        own_set(c, be32(pg + 8), r);
        for (int k = 0; k < nc; k++) { uint32_t off = (uint32_t)be16(pg + 12 + 2 * k); if (off + 4 <= pgsz) own_set(c, be32(pg + off), r); }
    }
    for (int i = 0; i < lane->cdc_res.nfreed; i++) { own_set(c, lane->cdc_res.freed[i], 0); ovo_set(c, lane->cdc_res.freed[i], 0); }
    for (int i = 0; i < lane->cdc_res.novupd; i++) ovo_set(c, lane->cdc_res.ovupd[i].page, lane->cdc_res.ovupd[i].leaf);
}

// ---- the CRDT metadata of the commit ----
static void ensure_ready (mw_db *db, mw_cdc *c) { (void)db; if (!c->ready) { mw_meta_ready(c->meta); c->ready = true; } }

// the changes of the commit as local changes of the CRDT, into the lane's overlay; the overlay is then encoded as the record extension
static void build_delta (mw_lane *lane, mw_cdc *c, const uint32_t *purge, int npurge) {
    free(lane->cdc_ext); lane->cdc_ext = NULL; lane->cdc_ext_len = 0;
    if (!lane->cdc_ovl) lane->cdc_ovl = mw_ovl_new(c->meta);
    if (!lane->cdc_ovl) return;
    mw_ovl *o = lane->cdc_ovl; mw_ovl_clear(o);
    ensure_ready(lane->db, c);
    const crdt_ops *ops = mw_ovl_ops();
    int64_t seq = 0; uint32_t *cols = NULL; int ccap = 0;
    for (int i = 0; i < npurge; i++) mw_ovl_purge(o, purge[i]);
    // order: deletes (a key that is deleted and written again in one commit is a new life of the row), key changes, inserts, updates
    for (int pass = 0; pass < 4; pass++) for (int i = 0; i < lane->cdc_res.n; i++) {
        const mw_chg *x = &lane->cdc_res.chg[i]; const mw_tab *t = x->tab;
        if (!t || !t->synced || !x->pk) continue;
        int want = x->kind == 3 ? 0 : (x->kind == 2 && x->oldpk) ? 1 : x->kind == 1 ? 2 : 3;
        if (want != pass) continue;
        int nc = 0; if (t->ncells > ccap) { ccap = t->ncells + 16; cols = realloc(cols, (size_t)ccap * sizeof *cols); if (!cols) return; }
        if (x->kind == 2 && !x->oldpk) {
            if (x->wide) { for (int k = 0; k < t->ncells; k++) if (x->wide[k / 64] & (1ull << (k % 64))) cols[nc++] = t->cell_id[k]; }
            else for (int k = 0; k < t->ncells && k < 63; k++) if (x->changed & (1ull << k)) cols[nc++] = t->cell_id[k];
            if (!x->wide && t->ncells > 63 && (x->changed >> 63)) { nc = 0; for (int k = 0; k < t->ncells; k++) cols[nc++] = t->cell_id[k]; }     // (not decidable per cell)
        }
        if (x->kind == 3) crdt_local_delete(ops, o, t->tid, x->pk, x->pklen, 0, &seq, NULL, 0);
        else if (x->kind == 1) crdt_local_insert(ops, o, t->tid, x->pk, x->pklen, t->cell_id, t->ncells, 0, &seq, NULL, 0);
        else if (x->oldpk) crdt_local_rekey(ops, o, t->tid, x->oldpk, x->oldpklen, x->pk, x->pklen, t->cell_id, t->ncells, 0, &seq, NULL, 0);
        else if (nc) crdt_local_update(ops, o, t->tid, x->pk, x->pklen, cols, nc, 0, &seq, NULL, 0);
    }
    free(cols);
    if (mw_ovl_encode(o, &lane->cdc_ext, &lane->cdc_ext_len) != 0) { lane->cdc_ext = NULL; lane->cdc_ext_len = 0; }
}

void mw_cdc_apply_cells (mw_db *db, mw_lane *lane, uint64_t epoch) {
    mw_cdc *c = db->cdc;
    mw_ovl *act = lane->cdc_decl ? lane->cdc_decl : lane->cdc_ovl;
    lane->cdc_ng = 0;
    if (act && lane->cdc_ext_len && !db->shared) { mw_meta_apply(c->meta, act, epoch); if (mw_meta_dirty(c->meta) >= 2048) mw_meta_kick(c->meta); }
    if (lane->cdc_ovl) mw_ovl_clear(lane->cdc_ovl);
    free(lane->cdc_ext); lane->cdc_ext = NULL; lane->cdc_ext_len = 0;
    mw_cat_free(lane->cdc_cat); lane->cdc_cat = NULL;
    mw_rd_result_free(&lane->cdc_res);
}

void mw_cdc_lane_free (mw_lane *lane) {
    mw_rd_result_free(&lane->cdc_res); mw_cat_free(lane->cdc_cat); lane->cdc_cat = NULL;
    free(lane->cdc_ext); lane->cdc_ext = NULL; lane->cdc_ext_len = 0;
    mw_ovl_free(lane->cdc_ovl); lane->cdc_ovl = NULL;
}

mw_meta *mw_cdc_meta (mw_db *db) { mw_cdc *c = db->cdc; return c ? c->meta : NULL; }

// the last user connection is closing: the metadata goes to the file now (the log is about to be dropped), and the store's own connections close
void mw_cdc_quiesce (mw_db *db) { mw_cdc *c = db->cdc; if (c) { if (!c->ready) { mw_meta_ready(c->meta); c->ready = true; } mw_meta_quiesce(c->meta); } }
// the point up to which the log may be compacted without losing metadata
void mw_cdc_kick_flush (mw_db *db) { mw_cdc *c = db->cdc; if (c) mw_meta_kick(c->meta); }
uint64_t mw_cdc_safe_epoch (mw_db *db) { mw_cdc *c = db->cdc; if (c && !c->ready) { mw_meta_ready(c->meta); c->ready = true; } if (!c || !c->ready) return c ? 0 : UINT64_MAX; uint64_t e = mw_meta_safe_epoch(c->meta); if (e != UINT64_MAX) mw_meta_kick(c->meta); return e; }
void mw_cdc_ensure_schema (sqlite3 *conn, mw_db *db) { if (db->cdc) (void)mw_meta_schema(conn); }

// The commit's new pages were renumbered above the end of the file (other commits extended it): what the capture knows about new pages by number is stale. The overflow owners of the
// new pages are not recorded (a later overflow-only update of them is found by the scan), and the pages the commit allocated and freed again are not on the free list of anybody.
void mw_cdc_relocated (mw_lane *lane) {
    mw_cdc *c = lane->db->cdc; if (!c) return;
    lane->cdc_res.novupd = 0;
    int k = 0; for (int i = 0; i < lane->cdc_res.nfreed; i++) if (lane->cdc_res.freed[i] <= lane->dsz_val) lane->cdc_res.freed[k++] = lane->cdc_res.freed[i];
    lane->cdc_res.nfreed = k;
    ovfl_set(lane->db, c, false);
}

void mw_cdc_set_cache_mb (mw_db *db, int mb) { mw_cdc *c = db->cdc; if (c && mb > 0) mw_meta_set_cache_mb(c->meta, mb); }
