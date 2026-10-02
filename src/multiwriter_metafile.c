//
//  multiwriter_metafile.c
//
//  The metadata in the database file: three ordinary tables (mw_state, mw_sites, mw_cells) written in batches by a flusher thread through a connection of its own (a normal commit of
//  the engine, so it is logged and atomic: the batch and the epoch it covers, meta_epoch, land together), read on a cache miss through a small pool of read-only connections.
//  Recovery: the extensions of the commits above meta_epoch are applied again (they are absolute states of cells, so applying a commit twice, or a flush that already holds it, is harmless).
//
#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include "multiwriter_meta_priv.h"
#include "multiwriter_internal.h"

static const char *SCHEMA_SQL =
    "CREATE TABLE IF NOT EXISTS mw_state(k TEXT PRIMARY KEY NOT NULL, v) WITHOUT ROWID;"
    "CREATE TABLE IF NOT EXISTS mw_sites(ord INTEGER PRIMARY KEY, id BLOB NOT NULL);"
    "CREATE TABLE IF NOT EXISTS mw_cells(tbl INTEGER NOT NULL, pk BLOB NOT NULL, col INTEGER NOT NULL, cv INTEGER NOT NULL, dv INTEGER NOT NULL, seq INTEGER NOT NULL, site INTEGER NOT NULL, PRIMARY KEY(tbl, pk, col)) WITHOUT ROWID;"
    "CREATE INDEX IF NOT EXISTS mw_cells_sen ON mw_cells(tbl, pk) WHERE col = -1;";

static uint64_t now_ns (void) { struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec; }
static bool busyish (int rc) { int p = rc & 0xff; return p == SQLITE_BUSY || p == SQLITE_LOCKED; }

// The index that lets the export find the cells changed since a db_version costs a random insert per cell at every flush: it is created by the first export (the databases that never
// synchronise do not pay for it), and the flush maintains it from then on.
int mw_meta_export_index (sqlite3 *c) { return sqlite3_exec(c, "CREATE INDEX IF NOT EXISTS mw_cells_dv ON mw_cells(dv, seq)", NULL, NULL, NULL); }

int mw_meta_schema (sqlite3 *c) {
    sqlite3_stmt *st = NULL; int have = 0;
    if (sqlite3_prepare_v2(c, "SELECT count(*) FROM sqlite_schema WHERE name IN ('mw_state','mw_sites','mw_cells','mw_cells_sen')", -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) have = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);
    if (have == 4) return SQLITE_OK;
    int rc = SQLITE_BUSY;
    for (int i = 0; i < 100 && busyish(rc); i++) {
        rc = sqlite3_exec(c, "BEGIN", NULL, NULL, NULL);
        if (rc == SQLITE_OK) rc = sqlite3_exec(c, SCHEMA_SQL, NULL, NULL, NULL);
        if (rc == SQLITE_OK) rc = sqlite3_exec(c, "COMMIT", NULL, NULL, NULL);
        if (rc != SQLITE_OK) { sqlite3_exec(c, "ROLLBACK", NULL, NULL, NULL); if (busyish(rc)) usleep(1000 * (unsigned)(i + 1)); }
    }
    return rc;
}

// ---- connections ----
static void uri_escape (const char *s, char *out) {
    for (; *s; s++) {
        if ((*s >= 'a' && *s <= 'z') || (*s >= 'A' && *s <= 'Z') || (*s >= '0' && *s <= '9') || strchr("/-_.~", *s)) *out++ = *s;
        else out += sprintf(out, "%%%02X", (unsigned char)*s);
    }
    *out = 0;
}

int mw_meta_attach (mw_meta *m, const char *path, int mode, int mpmode) {
    char *esc = malloc(strlen(path) * 3 + 1); if (!esc) return SQLITE_NOMEM;
    uri_escape(path, esc);
    size_t n = strlen(esc) + 120; m->uri = malloc(n); if (!m->uri) { free(esc); return SQLITE_NOMEM; }
    snprintf(m->uri, n, "file:%s?mw=%d&mw_mp=%d&mw_sys=1", esc, mode, mpmode);
    free(esc); m->attached = true; m->tables_ok = true;                    // (the tables are created by the first connection; a miss before that just finds nothing)
    return SQLITE_OK;
}

static sqlite3 *open_conn (mw_meta *m) {
    sqlite3 *c = NULL;
    if (sqlite3_open_v2(m->uri, &c, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL) != SQLITE_OK) { sqlite3_close(c); return NULL; }
    sqlite3_busy_timeout(c, 10000);
    return c;
}

void mw_metafile_free (mw_meta *m) {
    for (int i = 0; i < MW_RDN; i++) { sqlite3_finalize(m->rds[i]); sqlite3_close(m->rd[i]); m->rds[i] = NULL; m->rd[i] = NULL; }
    sqlite3_close(m->wr); m->wr = NULL;
    free(m->uri); m->uri = NULL;
}

// ---- reading ----
int mw_metafile_load (mw_meta *m, uint32_t tbl, const void *pk, size_t pklen, mw_mcell **cells, int *n) {
    *cells = NULL; *n = 0;
    if (!m->attached || !m->tables_ok) return 0;
    int slot = (int)(((uintptr_t)pthread_self() >> 4) % MW_RDN);
    for (int i = 0; i < MW_RDN; i++) { int k = (slot + i) % MW_RDN; if (pthread_mutex_trylock(&m->rdmu[k]) == 0) { slot = k; goto locked; } }
    pthread_mutex_lock(&m->rdmu[slot]);
locked:
    if (!m->rd[slot]) m->rd[slot] = open_conn(m);
    if (m->rd[slot] && !m->rds[slot]) sqlite3_prepare_v2(m->rd[slot], "SELECT col, cv, dv, seq, site FROM mw_cells WHERE tbl = ?1 AND pk = ?2", -1, &m->rds[slot], NULL);
    int rc = 0;                                                                 // (no table yet, or a statement that cannot be prepared: the row is not in the file)
    if (m->rds[slot]) {
        sqlite3_stmt *st = m->rds[slot];
        sqlite3_bind_int64(st, 1, tbl); sqlite3_bind_blob(st, 2, pk, (int)pklen, SQLITE_STATIC);
        int cap = 0, cnt = 0; mw_mcell *c = NULL; rc = 0; int r;
        while ((r = sqlite3_step(st)) == SQLITE_ROW) {
            if (cnt == cap) { cap = cap ? cap * 2 : 8; mw_mcell *nc = realloc(c, (size_t)cap * sizeof *c); if (!nc) { rc = -1; break; } c = nc; }
            c[cnt++] = (mw_mcell){ sqlite3_column_int64(st, 1), sqlite3_column_int64(st, 2), (uint32_t)sqlite3_column_int64(st, 0), (uint32_t)sqlite3_column_int64(st, 4), (uint32_t)sqlite3_column_int64(st, 3) };
        }
        if (r != SQLITE_DONE && rc == 0) rc = -1;
        sqlite3_reset(st); sqlite3_clear_bindings(st);
        if (rc == 0) {
            if (m->shared) {
                mw_shm *sh = m->db->shm; uint32_t np = atomic_load_explicit(&sh->npurge, memory_order_acquire);
                for (uint32_t q = 0; q < np; q++) if (atomic_load(&sh->purge[q].tbl) == tbl) { uint64_t ep = atomic_load(&sh->purge[q].epoch); int k = 0; for (int i = 0; i < cnt; i++) if (c[i].dv >= (int64_t)ep) c[k++] = c[i]; cnt = k; }
            } else {
                pthread_mutex_lock(&m->purge_mu);                                     // cells older than a drop of the table that the flush has not deleted yet are dead
                for (int q = 0; q < m->npurge; q++) if (m->purge[q].tbl == tbl) { int k = 0; for (int i = 0; i < cnt; i++) if (c[i].dv >= (int64_t)m->purge[q].epoch) c[k++] = c[i]; cnt = k; }
                pthread_mutex_unlock(&m->purge_mu);
            }
            *cells = c; *n = cnt;
        } else free(c);
    }
    pthread_mutex_unlock(&m->rdmu[slot]);
    return rc;
}

// many rows, one read transaction (a read transaction per row costs more than the lookup: the page cache is dropped when the database changed)
int mw_metafile_load_many (mw_meta *m, int n, const uint32_t *tbl, const uint8_t *const *pk, const size_t *pklen, mw_mcell **cells, int *ncells) {
    for (int i = 0; i < n; i++) { cells[i] = NULL; ncells[i] = 0; }
    if (!m->attached || !m->tables_ok) return 0;
    int slot = (int)(((uintptr_t)pthread_self() >> 4) % MW_RDN);
    for (int i = 0; i < MW_RDN; i++) { int k = (slot + i) % MW_RDN; if (pthread_mutex_trylock(&m->rdmu[k]) == 0) { slot = k; goto locked; } }
    pthread_mutex_lock(&m->rdmu[slot]);
locked:
    if (!m->rd[slot]) m->rd[slot] = open_conn(m);
    if (m->rd[slot] && !m->rds[slot]) sqlite3_prepare_v2(m->rd[slot], "SELECT col, cv, dv, seq, site FROM mw_cells WHERE tbl = ?1 AND pk = ?2", -1, &m->rds[slot], NULL);
    int rc = 0;
    if (m->rds[slot]) {
        sqlite3_stmt *st = m->rds[slot];
        bool txn = sqlite3_exec(m->rd[slot], "BEGIN", NULL, NULL, NULL) == SQLITE_OK;
        for (int i = 0; i < n && rc == 0; i++) {
            sqlite3_bind_int64(st, 1, tbl[i]); sqlite3_bind_blob(st, 2, pk[i], (int)pklen[i], SQLITE_STATIC);
            int cap = 0, cnt = 0; mw_mcell *c = NULL; int r;
            while ((r = sqlite3_step(st)) == SQLITE_ROW) {
                if (cnt == cap) { cap = cap ? cap * 2 : 4; mw_mcell *nc = realloc(c, (size_t)cap * sizeof *c); if (!nc) { rc = -1; break; } c = nc; }
                c[cnt++] = (mw_mcell){ sqlite3_column_int64(st, 1), sqlite3_column_int64(st, 2), (uint32_t)sqlite3_column_int64(st, 0), (uint32_t)sqlite3_column_int64(st, 4), (uint32_t)sqlite3_column_int64(st, 3) };
            }
            if (r != SQLITE_DONE && rc == 0) rc = -1;
            sqlite3_reset(st); sqlite3_clear_bindings(st);
            if (rc == 0) { cells[i] = c; ncells[i] = cnt; } else free(c);
        }
        if (txn) sqlite3_exec(m->rd[slot], "COMMIT", NULL, NULL, NULL);
    }
    pthread_mutex_unlock(&m->rdmu[slot]);
    if (rc == 0) {                                                          // cells older than a drop of the table that the flush has not deleted yet are dead
        for (int i = 0; i < n; i++) {
            if (m->shared) { mw_shm *sh = m->db->shm; uint32_t np = atomic_load_explicit(&sh->npurge, memory_order_acquire);
                for (uint32_t q = 0; q < np; q++) if (atomic_load(&sh->purge[q].tbl) == tbl[i]) { uint64_t ep = atomic_load(&sh->purge[q].epoch); int k = 0; for (int x = 0; x < ncells[i]; x++) if (cells[i][x].dv >= (int64_t)ep) cells[i][k++] = cells[i][x]; ncells[i] = k; } }
            else { pthread_mutex_lock(&m->purge_mu);
                for (int q = 0; q < m->npurge; q++) if (m->purge[q].tbl == tbl[i]) { int k = 0; for (int x = 0; x < ncells[i]; x++) if (cells[i][x].dv >= (int64_t)m->purge[q].epoch) cells[i][k++] = cells[i][x]; ncells[i] = k; }
                pthread_mutex_unlock(&m->purge_mu); }
        }
    } else for (int i = 0; i < n; i++) { free(cells[i]); cells[i] = NULL; ncells[i] = 0; }
    return rc;
}

// the filter of deleted rows from the file (the partial index over the causal-length entries makes this a scan of the deleted rows only)
int mw_metafile_load_tombstones (mw_meta *m) {
    if (!m->attached) return 0;
    sqlite3 *c = open_conn(m); if (!c) return -1;
    sqlite3_stmt *st = NULL; int n = 0;
    if (sqlite3_prepare_v2(c, "SELECT tbl, pk FROM mw_cells WHERE col = -1", -1, &st, NULL) == SQLITE_OK)
        while (sqlite3_step(st) == SQLITE_ROW) { mw_meta_bloom_add(m, (uint32_t)sqlite3_column_int64(st, 0), sqlite3_column_blob(st, 1), (size_t)sqlite3_column_bytes(st, 1)); n++; }
    sqlite3_finalize(st); sqlite3_close(c);
    return 0;
}

// ---- recovery / first use ----
// What the file tables say: the epoch they cover, the site ids they hold (installed through mw_meta_site_install: ord 0 is this database's own id)
void mw_metafile_load_state (mw_meta *m, uint64_t *F, uint64_t *hwm, uint32_t *sites_flushed, bool *have_own, uint8_t own[16]) {
    *F = 0; *hwm = 0; *sites_flushed = 0; *have_own = false;
    if (!m->attached) return;
    sqlite3 *c = open_conn(m); if (!c) return;
    sqlite3_stmt *st = NULL; int have = 0;
    if (sqlite3_prepare_v2(c, "SELECT count(*) FROM sqlite_schema WHERE name IN ('mw_state','mw_sites','mw_cells')", -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) have = sqlite3_column_int(st, 0);
    sqlite3_finalize(st); st = NULL;
    if (have == 3) {
        m->tables_ok = true;
        if (sqlite3_prepare_v2(c, "SELECT v FROM mw_state WHERE k = 'meta_epoch'", -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) *F = (uint64_t)sqlite3_column_int64(st, 0);
        sqlite3_finalize(st); st = NULL;
        if (sqlite3_prepare_v2(c, "SELECT v FROM mw_state WHERE k = 'dv_hwm'", -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) *hwm = (uint64_t)sqlite3_column_int64(st, 0);
        sqlite3_finalize(st); st = NULL;
        if (*hwm < *F) *hwm = *F;
        if (sqlite3_prepare_v2(c, "SELECT ord, id FROM mw_sites ORDER BY ord", -1, &st, NULL) == SQLITE_OK) {
            while (sqlite3_step(st) == SQLITE_ROW) {
                uint32_t ord = (uint32_t)sqlite3_column_int64(st, 0);
                if (sqlite3_column_bytes(st, 1) != 16) continue;
                if (ord == 0) { memcpy(own, sqlite3_column_blob(st, 1), 16); *have_own = true; }
                if (!m->shared) mw_meta_site_install(m, ord, sqlite3_column_blob(st, 1)); else mm_site_install(m, ord, sqlite3_column_blob(st, 1));
                if (ord + 1 > *sites_flushed) *sites_flushed = ord + 1;
            }
        }
        sqlite3_finalize(st);
    }
    sqlite3_close(c);
}

int mw_meta_ready (mw_meta *m) {
    if (m->shared) return mm_ready(m);
    if (atomic_load(&m->ready)) return 0;
    pthread_mutex_lock(&m->file_mu);
    if (atomic_load(&m->ready)) { pthread_mutex_unlock(&m->file_mu); return 0; }
    uint64_t F, hwm; uint8_t own[16]; bool have_own;
    mw_metafile_load_state(m, &F, &hwm, &m->sites_flushed, &have_own, own);
    if (have_own) memcpy(m->sites[0], own, 16);
    m->origin = (int64_t)hwm;                                   // this incarnation's epochs start again at 1: its db_versions go on from the largest the file has seen
    atomic_store(&m->hwm, hwm);
    atomic_store(&m->flushed, F);
    mw_metafile_load_tombstones(m);
    struct mw_db *db = m->db;
    for (int i = 0; i < db->nrext; i++) mw_meta_replay(m, db->rext[i].epoch, db->rext[i].data, db->rext[i].len);      // (all of them: the epochs of the log are of this incarnation, the file's flushed point is of an older one)
    for (int i = 0; i < db->nrext; i++) free(db->rext[i].data);
    free(db->rext); db->rext = NULL; db->nrext = db->caprext = 0;
    atomic_store(&m->ready, true);
    pthread_mutex_unlock(&m->file_mu);
    if (atomic_load(&m->ndirty)) mw_meta_kick(m);
    return 0;
}

// ---- flushing ----
void *mw_fbatch_alloc (fbatch *b, size_t n) {
    n = (n + 7) & ~(size_t)7;
    if (!b->nblocks || b->used + n > b->blockcap) {
        size_t cap = n > (1u << 20) ? n : (1u << 20);
        if (b->nblocks == b->capblocks) { int nc = b->capblocks ? b->capblocks * 2 : 16; uint8_t **nb = realloc(b->blocks, (size_t)nc * sizeof *nb); if (!nb) return NULL; b->blocks = nb; b->capblocks = nc; }
        uint8_t *blk = malloc(cap); if (!blk) return NULL;
        b->blocks[b->nblocks++] = blk; b->used = 0; b->blockcap = cap;
    }
    void *p = b->blocks[b->nblocks - 1] + b->used; b->used += n; return p;
}
fitem *mw_fbatch_add (fbatch *b, uint32_t tbl, const uint8_t *pk, uint32_t pklen, bool drop, int ncells) {
    if (b->n == b->cap) { int nc = b->cap ? b->cap * 2 : 1024; fitem *nv = realloc(b->v, (size_t)nc * sizeof *nv); if (!nv) return NULL; b->v = nv; b->cap = nc; }
    uint8_t *k = mw_fbatch_alloc(b, pklen ? pklen : 1); mw_mcell *c = mw_fbatch_alloc(b, (size_t)(ncells ? ncells : 1) * sizeof(mw_mcell));
    if (!k || !c) return NULL;
    memcpy(k, pk, pklen);
    fitem *it = &b->v[b->n++]; *it = (fitem){ tbl, k, pklen, drop, ncells, c };
    return it;
}
void mw_fbatch_free (fbatch *b) { for (int i = 0; i < b->nblocks; i++) free(b->blocks[i]); free(b->blocks); free(b->v); memset(b, 0, sizeof *b); }

// The dirty lists are taken away from the stripes whole (a moment under the lock) and then read in blocks, the lock released between them: a flusher that holds a stripe for the whole
// of a long list (a million rows when it lags) is what the writers of that stripe would wait for. The entries of a taken list cannot be freed (they are dirty) and nobody else touches
// their `dnext` (a writer links an entry only when it is not dirty).
#define DIRTY_BLOCK 256
static int collect (mw_meta *m, uint64_t F, fbatch *out, mentry **det) {
    for (int s = 0; s < STRIPES; s++) {
        stripe *st = &m->st[s];
        pthread_mutex_lock(&st->mu); det[s] = st->dirty; st->dirty = NULL; pthread_mutex_unlock(&st->mu);
        for (mentry *e = det[s]; e; ) {
            pthread_mutex_lock(&st->mu);
            for (int blk = 0; e && blk < DIRTY_BLOCK; blk++, e = e->dnext) {
                if (e->tbl == 0xFFFFFFFFu) continue;                                  // (its table was dropped meanwhile)
                int nc = 0; for (int i = 0; i < e->n; i++) if (e->cells[i].dv > (int64_t)F) nc++;
                bool drop = e->drop_ver > F;
                e->fver = e->ver;
                if (!nc && !drop) continue;
                fitem *it = mw_fbatch_add(out, e->tbl, e->pk, e->pklen, drop, nc);
                if (!it) { pthread_mutex_unlock(&st->mu); mw_fbatch_free(out); return -1; }
                int k = 0; for (int i = 0; i < e->n; i++) if (e->cells[i].dv > (int64_t)F) it->c[k++] = e->cells[i];
            }
            pthread_mutex_unlock(&st->mu);
        }
    }
    return 0;
}
// What is done with the taken lists: after a flush that wrote them the entries nobody changed since are clean; the others (and all of them when the flush failed) are dirty again.
static void collect_done (mw_meta *m, mentry **det, bool ok) {
    uint64_t cleaned = 0;
    for (int s = 0; s < STRIPES; s++) {
        stripe *st = &m->st[s];
        for (mentry *e = det[s], *nx; e; ) {
            pthread_mutex_lock(&st->mu);
            for (int blk = 0; e && blk < DIRTY_BLOCK; blk++, e = nx) {
                nx = e->dnext;
                if (e->tbl == 0xFFFFFFFFu) { free(e->cells); free(e); cleaned++; }
                else if (ok && e->fver == e->ver) { e->dnext = NULL; e->in_dirty = false; cleaned++; }
                else { e->dnext = st->dirty; st->dirty = e; }
            }
            pthread_mutex_unlock(&st->mu);
        }
        det[s] = NULL;
    }
    atomic_fetch_sub(&m->ndirty, cleaned);
}

// the batch in the order of the table's key (table, then key bytes): inserts into the b-tree then walk it instead of jumping about. Sorting fitems directly chases a pointer per
// comparison; the sort is on a small array of (table, first 8 key bytes, item) and only keys that agree on those look at the rest.
typedef struct { uint32_t tbl; uint64_t pfx; fitem *it; } skey;
static int fitem_full (const fitem *x, const fitem *y) {
    uint32_t m = x->pklen < y->pklen ? x->pklen : y->pklen; int c = memcmp(x->pk, y->pk, m);
    return c ? c : (x->pklen < y->pklen ? -1 : x->pklen > y->pklen);
}
static int skey_cmp (const void *a, const void *b) {
    const skey *x = a, *y = b;
    if (x->tbl != y->tbl) return x->tbl < y->tbl ? -1 : 1;
    if (x->pfx != y->pfx) return x->pfx < y->pfx ? -1 : 1;
    return fitem_full(x->it, y->it);
}
static void sort_items (fitem *v, int n) {
    if (n < 2) return;
    skey *a = malloc((size_t)n * sizeof *a), *b = malloc((size_t)n * sizeof *b); fitem *copy = malloc((size_t)n * sizeof *copy);
    if (!a || !b || !copy) { free(a); free(b); free(copy); return; }
    for (int i = 0; i < n; i++) {
        uint64_t p = 0; for (int q = 0; q < 8; q++) p = (p << 8) | (q < (int)v[i].pklen ? v[i].pk[q] : 0);
        a[i] = (skey){ v[i].tbl, p, &v[i] };
    }
    // least significant digit first, a byte at a time (stable): the key bytes, then the table
    for (int pass = 0; pass < 12; pass++) {
        size_t cnt[257] = {0};
        for (int i = 0; i < n; i++) { unsigned d = pass < 8 ? (unsigned)((a[i].pfx >> (8 * pass)) & 0xff) : (unsigned)((a[i].tbl >> (8 * (pass - 8))) & 0xff); cnt[d + 1]++; }
        bool skip = false; for (int d = 0; d < 256; d++) if (cnt[d + 1] == (size_t)n) skip = true;      // (every item has this digit: nothing to move)
        if (skip) continue;
        for (int d = 0; d < 256; d++) cnt[d + 1] += cnt[d];
        for (int i = 0; i < n; i++) { unsigned d = pass < 8 ? (unsigned)((a[i].pfx >> (8 * pass)) & 0xff) : (unsigned)((a[i].tbl >> (8 * (pass - 8))) & 0xff); b[cnt[d]++] = a[i]; }
        skey *t = a; a = b; b = t;
    }
    for (int i = 0; i < n; ) {                                                  // runs that agree on table and first 8 bytes: the rest of the key decides (they are short)
        int j = i + 1; while (j < n && a[j].tbl == a[i].tbl && a[j].pfx == a[i].pfx) j++;
        if (j - i > 1) qsort(&a[i], (size_t)(j - i), sizeof *a, skey_cmp);
        i = j;
    }
    for (int i = 0; i < n; i++) copy[i] = *a[i].it;
    memcpy(v, copy, (size_t)n * sizeof *v);
    free(a); free(b); free(copy);
}

// One transaction of the flush: the items [i0, i1) and, in the last one, the sites, the flushed point and the high-water mark. A flush is several of them (a transaction of hundreds of
// thousands of cells would hold its whole write set in memory and in the log): the points move only with the last, and what the others wrote is what a replay would write again.
static int write_batch (mw_meta *m, fitem *v, int i0, int i1, bool last, uint64_t V, uint64_t hwm, uint32_t nsites, uint32_t sflushed, const struct mw_purge *purge, int npurge) {
    int n = i1;
    sqlite3 *c = m->wr; int rc;
    rc = mw_meta_schema(c); if (rc != SQLITE_OK) return rc;
    m->tables_ok = true;
    if ((rc = sqlite3_exec(c, "BEGIN", NULL, NULL, NULL)) != SQLITE_OK) return rc;
    sqlite3_stmt *del = NULL, *ins = NULL, *site = NULL, *state = NULL, *hw = NULL;
    sqlite3_prepare_v2(c, "DELETE FROM mw_cells WHERE tbl = ?1 AND pk = ?2 AND col <> -1", -1, &del, NULL);
    sqlite3_prepare_v2(c, "INSERT OR REPLACE INTO mw_cells(tbl, pk, col, cv, dv, seq, site) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)", -1, &ins, NULL);
    sqlite3_prepare_v2(c, "INSERT OR REPLACE INTO mw_sites(ord, id) VALUES(?1, ?2)", -1, &site, NULL);
    sqlite3_prepare_v2(c, "INSERT OR REPLACE INTO mw_state(k, v) VALUES('meta_epoch', ?1)", -1, &state, NULL);
    sqlite3_prepare_v2(c, "INSERT OR REPLACE INTO mw_state(k, v) VALUES('dv_hwm', ?1)", -1, &hw, NULL);
    rc = (del && ins && site && state && hw) ? SQLITE_OK : SQLITE_ERROR;
    for (int i = 0; i0 == 0 && i < npurge && rc == SQLITE_OK; i++) { char q[80]; snprintf(q, sizeof q, "DELETE FROM mw_cells WHERE tbl = %u AND dv < %llu", purge[i].tbl, (unsigned long long)purge[i].epoch); rc = sqlite3_exec(c, q, NULL, NULL, NULL); }
    uint64_t cells = 0;
    for (int i = i0; i < n && rc == SQLITE_OK; i++) {
        if (v[i].drop) { sqlite3_bind_int64(del, 1, v[i].tbl); sqlite3_bind_blob(del, 2, v[i].pk, (int)v[i].pklen, SQLITE_STATIC); if (sqlite3_step(del) != SQLITE_DONE) rc = sqlite3_errcode(c); sqlite3_reset(del); }
        for (int k = 0; k < v[i].n && rc == SQLITE_OK; k++) {
            const mw_mcell *x = &v[i].c[k];
            sqlite3_bind_int64(ins, 1, v[i].tbl); sqlite3_bind_blob(ins, 2, v[i].pk, (int)v[i].pklen, SQLITE_STATIC);
            sqlite3_bind_int64(ins, 3, x->col == CRDT_COL_SENTINEL ? -1 : (int64_t)x->col); sqlite3_bind_int64(ins, 4, x->cv); sqlite3_bind_int64(ins, 5, x->dv);
            sqlite3_bind_int64(ins, 6, x->seq); sqlite3_bind_int64(ins, 7, x->site);
            if (sqlite3_step(ins) != SQLITE_DONE) rc = sqlite3_errcode(c); sqlite3_reset(ins); cells++;
        }
    }
    for (uint32_t o = sflushed; last && o < nsites && rc == SQLITE_OK; o++) {
        uint8_t id[16]; if (!mw_meta_site_id(m, o, id)) continue;
        sqlite3_bind_int64(site, 1, o); sqlite3_bind_blob(site, 2, id, 16, SQLITE_STATIC);
        if (sqlite3_step(site) != SQLITE_DONE) rc = sqlite3_errcode(c); sqlite3_reset(site);
    }
    for (int i = 0; i < n; i++) for (int k = 0; k < v[i].n; k++) if ((uint64_t)v[i].c[k].dv > hwm) hwm = (uint64_t)v[i].c[k].dv;
    if (V > hwm) hwm = V;
    if (last && rc == SQLITE_OK) { sqlite3_bind_int64(state, 1, (int64_t)V); if (sqlite3_step(state) != SQLITE_DONE) rc = sqlite3_errcode(c); sqlite3_reset(state); }
    if (last && rc == SQLITE_OK) { sqlite3_bind_int64(hw, 1, (int64_t)hwm); if (sqlite3_step(hw) != SQLITE_DONE) rc = sqlite3_errcode(c); sqlite3_reset(hw); }
    if (last) m->new_hwm = hwm;
    sqlite3_finalize(del); sqlite3_finalize(ins); sqlite3_finalize(site); sqlite3_finalize(state); sqlite3_finalize(hw);
    if (rc == SQLITE_OK) rc = sqlite3_exec(c, "COMMIT", NULL, NULL, NULL);
    if (rc != SQLITE_OK) sqlite3_exec(c, "ROLLBACK", NULL, NULL, NULL);
    else atomic_fetch_add(&m->flushed_cells, cells);
    return rc;
}

// Writes what changed since the last flush to the file tables, in one commit together with the epoch it covers. wait: another process may be flushing (shared mode): wait for it
// (a barrier: when this returns, everything committed before the call is in the file), otherwise skip.
static int flush_impl (mw_meta *m, bool wait) {
    if (!m->attached) return 0;
    mw_meta_ready(m);
    mw_shm *sh = m->shared ? m->db->shm : NULL;
    pthread_mutex_lock(&m->file_mu);
    if (sh && !mw_mp_meta_lock(m->db, 1, wait)) { pthread_mutex_unlock(&m->file_mu); return 0; }
    uint64_t t0 = now_ns();
    int64_t origin = mw_meta_origin(m);
    uint64_t Ve = sh ? atomic_load_explicit(&sh->committed_epoch, memory_order_acquire) : atomic_load(&m->db->epoch);
    uint64_t V = Ve + (uint64_t)origin;                                          // (db_versions from here on)
    uint64_t F = sh ? atomic_load(&sh->meta_flushed) : atomic_load(&m->flushed);
    uint64_t Fe = F > (uint64_t)origin ? F - (uint64_t)origin : 0;               // the same point as an epoch of this incarnation
    uint32_t nsites = sh ? atomic_load(&sh->nsites) : 0, sflushed = sh ? atomic_load(&sh->sites_flushed) : m->sites_flushed;
    if (!sh) { pthread_mutex_lock(&m->site_mu); nsites = m->nsites; pthread_mutex_unlock(&m->site_mu); }
    int rc = SQLITE_OK;
    mentry *det[STRIPES]; bool taken = false;
    struct mw_purge *purge = NULL; int npurge = 0;
    if (sh) { uint32_t np = atomic_load_explicit(&sh->npurge, memory_order_acquire); if (np) { purge = malloc(np * sizeof *purge); if (purge) for (uint32_t i = 0; i < np; i++) purge[npurge++] = (struct mw_purge){ atomic_load(&sh->purge[i].tbl), atomic_load(&sh->purge[i].epoch) }; } }
    else { pthread_mutex_lock(&m->purge_mu); if (m->npurge) { purge = malloc((size_t)m->npurge * sizeof *purge); if (purge) { memcpy(purge, m->purge, (size_t)m->npurge * sizeof *purge); npurge = m->npurge; } } pthread_mutex_unlock(&m->purge_mu); }
    bool dirty = sh ? atomic_load(&sh->meta_last) > Fe : atomic_load(&m->ndirty) != 0;
    if (!dirty && nsites <= sflushed && npurge == 0) goto out;
    if (!m->wr) m->wr = open_conn(m);
    if (!m->wr) { rc = SQLITE_CANTOPEN; goto out; }
    fbatch fb = {0};
    if (!sh) { memset(det, 0, sizeof det); taken = true; }
    if ((sh ? mm_collect(m, F, Fe, &fb) : collect(m, F, &fb, det)) != 0) { rc = SQLITE_NOMEM; goto out; }
    fitem *v = fb.v; int n = fb.n;
    sort_items(v, n);
    uint64_t hw0 = sh ? atomic_load(&sh->dv_hwm) : atomic_load(&m->hwm);
    rc = SQLITE_OK;
    for (int i0 = 0; i0 <= n && rc == SQLITE_OK; ) {
        int i1 = i0, cells = 0; while (i1 < n && cells < 20000) cells += v[i1++].n ? v[i1 - 1].n : 1;
        bool last = i1 >= n;
        rc = SQLITE_BUSY;
        for (int attempt = 0; attempt < 200 && busyish(rc); attempt++) {
            rc = write_batch(m, v, i0, i1, last, V, hw0, nsites, sflushed, purge, npurge);
            if (busyish(rc)) { atomic_fetch_add(&m->flush_retries, 1); usleep(500 * (unsigned)(attempt < 20 ? attempt + 1 : 20)); }
        }
        if (last) break;
        i0 = i1;
    }
    mw_fbatch_free(&fb);
    if (rc == SQLITE_OK) {
        if (sh) {
            atomic_store(&sh->sites_flushed, nsites); atomic_store(&sh->dv_hwm, m->new_hwm);
            atomic_store_explicit(&sh->meta_flushed, V, memory_order_release);
            if (atomic_load(&sh->meta_last) <= Ve) atomic_store(&sh->meta_dirty, 0);
            else { uint64_t d = atomic_load(&sh->meta_dirty); atomic_store(&sh->meta_dirty, d > (uint64_t)n ? d - (uint64_t)n : 1); }
        } else {
            pthread_mutex_lock(&m->purge_mu);                                         // (a drop that happened again meanwhile has a newer epoch and stays)
            for (int i = 0; i < npurge; i++) for (int q = 0; q < m->npurge; q++) if (m->purge[q].tbl == purge[i].tbl && m->purge[q].epoch == purge[i].epoch) { m->purge[q] = m->purge[--m->npurge]; break; }
            pthread_mutex_unlock(&m->purge_mu);
            atomic_store(&m->flushed, V); atomic_store(&m->hwm, m->new_hwm); m->sites_flushed = nsites;
            collect_done(m, det, true); taken = false;
        }
        atomic_fetch_add(&m->n_flushes, 1);
    }
    atomic_fetch_add(&m->flush_ns, now_ns() - t0);
    m->last_flush_ns = now_ns();
out:
    if (taken) collect_done(m, det, false);                                      // (failed: the entries are dirty again)
    free(purge);
    if (sh) mw_mp_meta_unlock(m->db, 1);
    pthread_mutex_unlock(&m->file_mu);
    return rc;
}
int mw_meta_flush (mw_meta *m) { return flush_impl(m, true); }

uint64_t mw_meta_epoch (mw_meta *m) { return m->shared ? atomic_load_explicit(&m->db->shm->committed_epoch, memory_order_acquire) : atomic_load(&m->db->epoch); }
uint64_t mw_meta_dirty (mw_meta *m) {
    if (!m->shared) return atomic_load(&m->ndirty);
    mw_shm *sh = m->db->shm;                                                   // dirty: a commit with metadata is newer than the last flush (the count is a measure for the flusher's pace only)
    uint64_t F = atomic_load(&sh->meta_flushed), o = atomic_load(&sh->dv_origin);          // (the flushed point is a db_version, the newest commit with metadata an epoch)
    if (atomic_load(&sh->meta_last) <= (F > o ? F - o : 0)) return 0;
    uint64_t d = atomic_load(&sh->meta_dirty); return d ? d : 1;
}
uint64_t mw_meta_flushed (mw_meta *m) { return m->shared ? atomic_load(&m->db->shm->meta_flushed) : atomic_load(&m->flushed); }
uint64_t mw_meta_safe_epoch (mw_meta *m) {                                     // an epoch of this incarnation
    if (mw_meta_dirty(m) == 0) return UINT64_MAX;
    uint64_t F = mw_meta_flushed(m), o = (uint64_t)mw_meta_origin(m);
    return F > o ? F - o : 0;
}
uint64_t mw_meta_dv (mw_meta *m, uint64_t epoch) { return epoch + (uint64_t)mw_meta_origin(m); }

// ---- the flusher thread ----
static void *flusher_main (void *arg) {
    mw_meta *m = arg;
    for (;;) {
        pthread_mutex_lock(&m->th_mu);
        if (!m->th_stop && !m->kicked) {
            struct timespec until; clock_gettime(CLOCK_REALTIME, &until); until.tv_nsec += 100 * 1000000L; if (until.tv_nsec >= 1000000000L) { until.tv_sec++; until.tv_nsec -= 1000000000L; }
            pthread_cond_timedwait(&m->th_cv, &m->th_mu, &until);
        }
        bool stop = m->th_stop, kicked = m->kicked; m->kicked = false; __atomic_store_n(&m->kick_pending, false, __ATOMIC_RELAXED);
        pthread_mutex_unlock(&m->th_mu);
        if (stop) break;
        uint64_t d;
        const char *er = getenv("MW_META_FLUSH_ROWS"), *em = getenv("MW_META_FLUSH_MS");           // (tests: flush very often)
        uint64_t rows = er ? (uint64_t)atoll(er) : 16384, ms = em ? (uint64_t)atoll(em) : 250;
        d = mw_meta_dirty(m);
        uint64_t age = now_ns() - m->last_flush_ns;
        // a flush is a transaction of its own (a log record, a sync): small ones at a high rate cost the writers more than they save, so a kick is heard only when the last flush is not too recent
        if (d && ((kicked && (age > 20000000ull || d >= 4 * rows)) || d >= rows || age > ms * 1000000ull)) flush_impl(m, false);
    }
    return NULL;
}

void mw_meta_kick (mw_meta *m) {
    if (!m->attached || !(m->shared ? atomic_load(&m->db->shm->meta_state) == 2 : atomic_load(&m->ready))) return;
    if (m->th_running && __atomic_load_n(&m->kick_pending, __ATOMIC_RELAXED)) return;                    // (the thread has been asked already and has not looked yet: nothing to add)
    pthread_mutex_lock(&m->th_mu);
    m->kicked = true; __atomic_store_n(&m->kick_pending, true, __ATOMIC_RELAXED);
    if (!m->th_running && !m->th_stop) { if (pthread_create(&m->th, NULL, flusher_main, m) == 0) m->th_running = true; }
    pthread_cond_signal(&m->th_cv);
    pthread_mutex_unlock(&m->th_mu);
}

void mw_meta_quiesce (mw_meta *m) {
    if (!m->attached) return;
    if (atomic_exchange(&m->quiescing, 1)) return;                           // (closing our own connections releases the database, which asks us to quiesce again)
    pthread_mutex_lock(&m->th_mu);
    m->th_stop = true; pthread_cond_broadcast(&m->th_cv);
    bool run = m->th_running; pthread_mutex_unlock(&m->th_mu);
    if (run) { pthread_join(m->th, NULL); m->th_running = false; }
    if (m->shared ? atomic_load(&m->db->shm->meta_state) == 2 : atomic_load(&m->ready)) mw_meta_flush(m);
    pthread_mutex_lock(&m->th_mu); m->th_stop = false; pthread_mutex_unlock(&m->th_mu);       // (a later open starts the thread again)
    sqlite3 *conns[MW_RDN + 1]; sqlite3_stmt *stmts[MW_RDN]; int nc = 0;
    pthread_mutex_lock(&m->file_mu);
    for (int i = 0; i < MW_RDN; i++) { pthread_mutex_lock(&m->rdmu[i]); stmts[i] = m->rds[i]; m->rds[i] = NULL; if (m->rd[i]) conns[nc++] = m->rd[i]; m->rd[i] = NULL; pthread_mutex_unlock(&m->rdmu[i]); }
    if (m->wr) conns[nc++] = m->wr;
    m->wr = NULL;
    pthread_mutex_unlock(&m->file_mu);
    atomic_store(&m->quiescing, 0);
    // the last connection to close releases the database for good, and that frees this store: nothing of it may be touched after the closes
    for (int i = 0; i < MW_RDN; i++) sqlite3_finalize(stmts[i]);
    for (int i = 0; i < nc; i++) sqlite3_close(conns[i]);
}
