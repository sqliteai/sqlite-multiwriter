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
    "CREATE TABLE IF NOT EXISTS mw_rows(tbl INTEGER NOT NULL, pk BLOB NOT NULL, dv INTEGER NOT NULL, cells BLOB NOT NULL, PRIMARY KEY(tbl, pk)) WITHOUT ROWID;";

static uint64_t now_ns (void) { struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec; }
static bool busyish (int rc) { int p = rc & 0xff; return p == SQLITE_BUSY || p == SQLITE_LOCKED; }

// The index that lets the export find the cells changed since a db_version costs a random insert per cell at every flush: it is created by the first export (the databases that never
// synchronise do not pay for it), and the flush maintains it from then on.
int mw_meta_export_index (sqlite3 *c) { return sqlite3_exec(c, "CREATE INDEX IF NOT EXISTS mw_rows_dv ON mw_rows(dv)", NULL, NULL, NULL); }

// ---- the row as the file holds it ----
// One row of mw_rows per row of a user table: its key, the largest db_version of its cells (what the export looks for) and all its cells, packed: a format byte, the number of cells, then
// per cell the column (+1, so the sentinel is 0), version, db_version, sequence and site as varints. The cells are the complete state of the row: a flush replaces them all.
#define ROW_FORMAT 1
static size_t put_var (uint8_t *p, uint64_t v) { size_t n = 0; while (v >= 0x80) { p[n++] = (uint8_t)(v | 0x80); v >>= 7; } p[n++] = (uint8_t)v; return n; }
static bool get_var (const uint8_t **p, const uint8_t *end, uint64_t *v) {
    uint64_t r = 0; int sh = 0;
    while (*p < end && sh < 64) { uint8_t b = *(*p)++; r |= (uint64_t)(b & 0x7f) << sh; if (!(b & 0x80)) { *v = r; return true; } sh += 7; }
    return false;
}
// the packed cells in `out` (room for 1 + 10 + 45 bytes a cell); returns the length
static size_t row_pack (const mw_mcell *c, int n, uint8_t *out) {
    size_t w = 0; out[w++] = ROW_FORMAT; w += put_var(out + w, (uint64_t)n);
    for (int i = 0; i < n; i++) {
        w += put_var(out + w, (uint64_t)(uint32_t)(c[i].col + 1u)); w += put_var(out + w, (uint64_t)c[i].cv); w += put_var(out + w, (uint64_t)c[i].dv);
        w += put_var(out + w, c[i].seq); w += put_var(out + w, c[i].site);
    }
    return w;
}
#define ROW_PACK_MAX(n) (16 + (size_t)(n) * 48)
// the cells of a packed row, appended to `*c` (grown as needed; `*n` the count so far); false when the blob is not one
static bool row_unpack (const uint8_t *p, size_t len, mw_mcell **c, int *n, int *cap) {
    const uint8_t *end = p + len; uint64_t cnt;
    if (len < 2 || *p++ != ROW_FORMAT || !get_var(&p, end, &cnt) || cnt > (1u << 20)) return false;
    if (*n + (int)cnt > *cap) { int nc = (*n + (int)cnt) * 2 + 4; mw_mcell *nm = realloc(*c, (size_t)nc * sizeof **c); if (!nm) return false; *c = nm; *cap = nc; }
    for (uint64_t i = 0; i < cnt; i++) {
        uint64_t col, cv, dv, seq, site;
        if (!get_var(&p, end, &col) || !get_var(&p, end, &cv) || !get_var(&p, end, &dv) || !get_var(&p, end, &seq) || !get_var(&p, end, &site)) return false;
        (*c)[(*n)++] = (mw_mcell){ (int64_t)cv, (int64_t)dv, (uint32_t)(col - 1u), (uint32_t)site, (uint32_t)seq };
    }
    return true;
}
// (tests) the packed form of cells: the blob, malloc'ed
uint8_t *mw_meta_row_pack (const mw_mcell *c, int n, size_t *len) { uint8_t *b = malloc(ROW_PACK_MAX(n)); if (!b) return NULL; *len = row_pack(c, n, b); return b; }
// for the export and the virtual table of tests: the cells of the blob of a row
bool mw_meta_row_cells (const void *blob, size_t len, mw_mcell **c, int *n) { int cap = 0; *c = NULL; *n = 0; if (row_unpack(blob, len, c, n, &cap)) return true; free(*c); *c = NULL; *n = 0; return false; }

int mw_meta_schema (sqlite3 *c) {
    sqlite3_stmt *st = NULL; int have = 0;
    if (sqlite3_prepare_v2(c, "SELECT count(*) FROM sqlite_schema WHERE name IN ('mw_state','mw_sites','mw_rows')", -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) have = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);
    if (have == 3) return SQLITE_OK;
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
    for (int i = 0; i < MW_PAR - 1; i++) { sqlite3_close(m->wrp[i]); m->wrp[i] = NULL; }
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
    if (m->rd[slot] && !m->rds[slot]) sqlite3_prepare_v2(m->rd[slot], "SELECT cells FROM mw_rows WHERE tbl = ?1 AND pk = ?2", -1, &m->rds[slot], NULL);
    int rc = 0;                                                                 // (no table yet, or a statement that cannot be prepared: the row is not in the file)
    if (m->rds[slot]) {
        sqlite3_stmt *st = m->rds[slot];
        sqlite3_bind_int64(st, 1, tbl); sqlite3_bind_blob(st, 2, pk, (int)pklen, SQLITE_STATIC);
        int cap = 0, cnt = 0; mw_mcell *c = NULL; rc = 0; int r;
        while ((r = sqlite3_step(st)) == SQLITE_ROW) { if (!row_unpack(sqlite3_column_blob(st, 0), (size_t)sqlite3_column_bytes(st, 0), &c, &cnt, &cap)) { rc = -1; break; } }
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
    if (m->rd[slot] && !m->rds[slot]) sqlite3_prepare_v2(m->rd[slot], "SELECT cells FROM mw_rows WHERE tbl = ?1 AND pk = ?2", -1, &m->rds[slot], NULL);
    int rc = 0;
    if (m->rds[slot]) {
        sqlite3_stmt *st = m->rds[slot];
        bool txn = sqlite3_exec(m->rd[slot], "BEGIN", NULL, NULL, NULL) == SQLITE_OK;
        for (int i = 0; i < n && rc == 0; i++) {
            sqlite3_bind_int64(st, 1, tbl[i]); sqlite3_bind_blob(st, 2, pk[i], (int)pklen[i], SQLITE_STATIC);
            int cap = 0, cnt = 0; mw_mcell *c = NULL; int r;
            while ((r = sqlite3_step(st)) == SQLITE_ROW) { if (!row_unpack(sqlite3_column_blob(st, 0), (size_t)sqlite3_column_bytes(st, 0), &c, &cnt, &cap)) { rc = -1; break; } }
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

// the filter of the keys the file knows (every row of a table has a causal-length entry): a scan of the keys
int mw_metafile_load_tombstones (mw_meta *m) {
    if (!m->attached) return 0;
    sqlite3 *c = open_conn(m); if (!c) return -1;
    sqlite3_stmt *st = NULL; int n = 0;
    if (sqlite3_prepare_v2(c, "SELECT tbl, pk FROM mw_rows", -1, &st, NULL) == SQLITE_OK)
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
    if (sqlite3_prepare_v2(c, "SELECT count(*) FROM sqlite_schema WHERE name IN ('mw_state','mw_sites','mw_rows')", -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) have = sqlite3_column_int(st, 0);
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
                bool changed = e->drop_ver > F; for (int i = 0; i < e->n && !changed; i++) if (e->cells[i].dv > (int64_t)F) changed = true;
                e->fver = e->ver;
                if (!changed) continue;
                fitem *it = mw_fbatch_add(out, e->tbl, e->pk, e->pklen, false, e->n);                    // (the whole state of the row: the file replaces its copy)
                if (!it) { pthread_mutex_unlock(&st->mu); mw_fbatch_free(out); return -1; }
                for (int i = 0; i < e->n; i++) it->c[i] = e->cells[i];
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
static int write_batch (mw_meta *m, sqlite3 *c, fitem *v, int i0, int i1, bool last, uint64_t V, uint64_t hwm, uint32_t nsites, uint32_t sflushed, const struct mw_purge *purge, int npurge) {
    int n = i1;
    int rc;
    if (!atomic_load_explicit(&m->schema_seen, memory_order_relaxed)) { rc = mw_meta_schema(c); if (rc != SQLITE_OK) return rc; atomic_store(&m->schema_seen, true); }
    m->tables_ok = true;
    if ((rc = sqlite3_exec(c, "BEGIN", NULL, NULL, NULL)) != SQLITE_OK) return rc;
    sqlite3_stmt *del = NULL, *ins = NULL, *site = NULL, *state = NULL, *hw = NULL;
    sqlite3_prepare_v2(c, "DELETE FROM mw_rows WHERE tbl = ?1 AND pk = ?2", -1, &del, NULL);
    sqlite3_prepare_v2(c, "INSERT OR REPLACE INTO mw_rows(tbl, pk, dv, cells) VALUES(?1, ?2, ?3, ?4)", -1, &ins, NULL);
    sqlite3_prepare_v2(c, "INSERT OR REPLACE INTO mw_sites(ord, id) VALUES(?1, ?2)", -1, &site, NULL);
    sqlite3_prepare_v2(c, "INSERT OR REPLACE INTO mw_state(k, v) VALUES('meta_epoch', ?1)", -1, &state, NULL);
    sqlite3_prepare_v2(c, "INSERT OR REPLACE INTO mw_state(k, v) VALUES('dv_hwm', ?1)", -1, &hw, NULL);
    rc = (del && ins && site && state && hw) ? SQLITE_OK : SQLITE_ERROR;
    for (int i = 0; i0 == 0 && i < npurge && rc == SQLITE_OK; i++) { char q[80]; snprintf(q, sizeof q, "DELETE FROM mw_rows WHERE tbl = %u AND dv < %llu", purge[i].tbl, (unsigned long long)purge[i].epoch); rc = sqlite3_exec(c, q, NULL, NULL, NULL); }
    uint64_t cells = 0;
    uint8_t *pack = NULL; size_t packcap = 0;
    for (int i = i0; i < n && rc == SQLITE_OK; i++) {
        if (!v[i].n) { sqlite3_bind_int64(del, 1, v[i].tbl); sqlite3_bind_blob(del, 2, v[i].pk, (int)v[i].pklen, SQLITE_STATIC); if (sqlite3_step(del) != SQLITE_DONE) rc = sqlite3_errcode(c); sqlite3_reset(del); continue; }
        size_t need = ROW_PACK_MAX(v[i].n);
        if (need > packcap) { uint8_t *np = realloc(pack, need * 2); if (!np) { rc = SQLITE_NOMEM; break; } pack = np; packcap = need * 2; }
        size_t len = row_pack(v[i].c, v[i].n, pack);
        int64_t dvmax = 0; for (int k = 0; k < v[i].n; k++) if (v[i].c[k].dv > dvmax) dvmax = v[i].c[k].dv;
        sqlite3_bind_int64(ins, 1, v[i].tbl); sqlite3_bind_blob(ins, 2, v[i].pk, (int)v[i].pklen, SQLITE_STATIC); sqlite3_bind_int64(ins, 3, dvmax); sqlite3_bind_blob(ins, 4, pack, (int)len, SQLITE_STATIC);
        if (sqlite3_step(ins) != SQLITE_DONE) rc = sqlite3_errcode(c); sqlite3_reset(ins); cells += (uint64_t)v[i].n;
    }
    free(pack);
    for (uint32_t o = sflushed; last && o < nsites && rc == SQLITE_OK; o++) {
        uint8_t id[16]; if (!mw_meta_site_id(m, o, id)) continue;
        sqlite3_bind_int64(site, 1, o); sqlite3_bind_blob(site, 2, id, 16, SQLITE_STATIC);
        if (sqlite3_step(site) != SQLITE_DONE) rc = sqlite3_errcode(c); sqlite3_reset(site);
    }
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
// the items [i0, i1) in transactions of about 20000 cells (the first also does the purges, the last the sites and the points when `last`)
static int write_range (mw_meta *m, sqlite3 *c, fitem *v, int i0, int n, bool last_range, uint64_t V, uint64_t hwm, uint32_t nsites, uint32_t sflushed, const struct mw_purge *purge, int npurge) {
    int rc = SQLITE_OK;
    for (int a = i0; a <= n && rc == SQLITE_OK; ) {
        int b = a, cells = 0; while (b < n && cells < 20000) cells += v[b++].n ? v[b - 1].n : 1;
        bool last = last_range && b >= n;
        rc = SQLITE_BUSY;
        for (int attempt = 0; attempt < 200 && busyish(rc); attempt++) {
            rc = write_batch(m, c, v, a, b, last, V, hwm, nsites, sflushed, purge, npurge);
            if (busyish(rc)) { atomic_fetch_add(&m->flush_retries, 1); usleep(500 * (unsigned)(attempt < 20 ? attempt + 1 : 20)); }
        }
        if (b >= n) break;
        a = b;
    }
    return rc;
}
typedef struct { mw_meta *m; sqlite3 *c; fitem *v; int i0, i1; uint64_t V, hwm; int rc; } slice_job;
static void *slice_main (void *arg) {
    slice_job *j = arg;
    // a slice never touches the purges, the sites or the points (those are the serial part): `n` bounds the range, an empty range writes nothing
    j->rc = j->i0 >= j->i1 ? SQLITE_OK : write_range(j->m, j->c, j->v, j->i0 == 0 ? 0 : j->i0, j->i1, false, j->V, j->hwm, 0, 0, NULL, 0);
    return NULL;
}

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
    for (int i = 0; i < n; i++) for (int k = 0; k < v[i].n; k++) if ((uint64_t)v[i].c[k].dv > hw0) hw0 = (uint64_t)v[i].c[k].dv;
    rc = SQLITE_OK;
    uint64_t total = 0; for (int i = 0; i < n; i++) total += v[i].n ? (uint64_t)v[i].n : 1;
    static int pmin = -1; if (pmin < 0) { const char *e = getenv("MW_META_PAR_MIN"); pmin = e ? atoi(e) : 200000; }
    int par = m->par; if (total < (uint64_t)pmin || n < par) par = 1;
    if (par > 1) {
        // the batch is cut in key ranges written at the same time, each by a connection of its own: the b-tree leaves of different ranges are different pages, so the commits do not conflict
        // (but for the pages above them). The purges first, alone; the sites and the points last, after every range is on disk: the points move only when all of it is.
        if (npurge) rc = write_range(m, m->wr, v, 0, 0, false, V, hw0, nsites, sflushed, purge, npurge);
        slice_job job[MW_PAR]; pthread_t th[MW_PAR]; bool started[MW_PAR] = {false};
        int cut[MW_PAR + 1]; cut[0] = 0; uint64_t acc = 0; int q = 1;
        for (int i = 0; i < n && q < par; i++) { acc += v[i].n ? (uint64_t)v[i].n : 1; if (acc >= total * (uint64_t)q / (uint64_t)par) cut[q++] = i + 1; }
        while (q <= par) cut[q++] = n;
        for (int k = 0; k < par && rc == SQLITE_OK; k++) {
            if (k > 0 && !m->wrp[k - 1]) m->wrp[k - 1] = open_conn(m);
            job[k] = (slice_job){ m, k == 0 ? m->wr : m->wrp[k - 1], v, cut[k], cut[k + 1], V, hw0, SQLITE_OK };
            if (k > 0 && (!job[k].c || pthread_create(&th[k], NULL, slice_main, &job[k]) != 0)) { if (!job[k].c) rc = SQLITE_CANTOPEN; else slice_main(&job[k]); }   // (no thread: this one does it)
            else if (k > 0) started[k] = true;
        }
        if (rc == SQLITE_OK) slice_main(&job[0]);
        for (int k = 1; k < par; k++) if (started[k]) pthread_join(th[k], NULL);
        for (int k = 0; k < par && rc == SQLITE_OK; k++) rc = job[k].rc;
        if (busyish(rc)) rc = write_range(m, m->wr, v, 0, n, true, V, hw0, nsites, sflushed, NULL, 0);     // (a range lost against the others too often: the rows are replaced, so writing all of them again, alone, is the same)
        else if (rc == SQLITE_OK) rc = write_range(m, m->wr, v, n, n, true, V, hw0, nsites, sflushed, NULL, 0);
    } else rc = write_range(m, m->wr, v, 0, n, true, V, hw0, nsites, sflushed, purge, npurge);
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
// rows that may wait for the flusher before the writers are slowed down: a row waiting takes about 256 bytes
uint64_t mw_meta_dirty_limit (mw_meta *m) { uint64_t l = (uint64_t)m->cap_bytes * 4 / 256; return l < 65536 ? 65536 : l; }
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
        const char *ek = getenv("MW_META_KICK_MS"); uint64_t kick_ns = (ek ? (uint64_t)atoll(ek) : 20) * 1000000ull;
        // a flush is a transaction of its own (a log record, a sync): small ones at a high rate cost the writers more than they save, so a kick is heard only when the last flush is not too recent
        if (d && ((kicked && (age > kick_ns || d >= 4 * rows)) || d >= rows || age > ms * 1000000ull)) flush_impl(m, false);
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
    sqlite3 *conns[MW_RDN + MW_PAR]; sqlite3_stmt *stmts[MW_RDN]; int nc = 0;
    pthread_mutex_lock(&m->file_mu);
    for (int i = 0; i < MW_RDN; i++) { pthread_mutex_lock(&m->rdmu[i]); stmts[i] = m->rds[i]; m->rds[i] = NULL; if (m->rd[i]) conns[nc++] = m->rd[i]; m->rd[i] = NULL; pthread_mutex_unlock(&m->rdmu[i]); }
    if (m->wr) conns[nc++] = m->wr;
    m->wr = NULL;
    for (int i = 0; i < MW_PAR - 1; i++) if (m->wrp[i]) { conns[nc++] = m->wrp[i]; m->wrp[i] = NULL; }
    pthread_mutex_unlock(&m->file_mu);
    atomic_store(&m->quiescing, 0);
    // the last connection to close releases the database for good, and that frees this store: nothing of it may be touched after the closes
    for (int i = 0; i < MW_RDN; i++) sqlite3_finalize(stmts[i]);
    for (int i = 0; i < nc; i++) sqlite3_close(conns[i]);
}

// ---- mw_cells: the cells of mw_rows as the rows of a table ----
// A read-only virtual table (eponymous: there is nothing to create) with the columns the file tables used to have, for people and tools that want to look at the metadata in SQL:
// SELECT * FROM mw_cells; col is -1 for a row's own entry (the causal length).
typedef struct { sqlite3_vtab base; sqlite3 *db; } cells_vt;
typedef struct { sqlite3_vtab_cursor base; sqlite3_stmt *st; mw_mcell *c; int n, i; sqlite3_int64 rowid; bool eof; } cells_cur;
static int cv_connect (sqlite3 *db, void *aux, int argc, const char *const *argv, sqlite3_vtab **out, char **err) {
    (void)aux; (void)argc; (void)argv; (void)err;
    int rc = sqlite3_declare_vtab(db, "CREATE TABLE x(tbl INTEGER, pk BLOB, col INTEGER, cv INTEGER, dv INTEGER, seq INTEGER, site INTEGER)");
    if (rc != SQLITE_OK) return rc;
    cells_vt *v = sqlite3_malloc(sizeof *v); if (!v) return SQLITE_NOMEM;
    memset(v, 0, sizeof *v); v->db = db; *out = &v->base;
    return SQLITE_OK;
}
static int cv_disconnect (sqlite3_vtab *vt) { sqlite3_free(vt); return SQLITE_OK; }
static int cv_bestindex (sqlite3_vtab *vt, sqlite3_index_info *info) { (void)vt; info->estimatedCost = 1e9; info->estimatedRows = 1000000; return SQLITE_OK; }
static int cv_open (sqlite3_vtab *vt, sqlite3_vtab_cursor **out) { (void)vt; cells_cur *c = sqlite3_malloc(sizeof *c); if (!c) return SQLITE_NOMEM; memset(c, 0, sizeof *c); *out = &c->base; return SQLITE_OK; }
static int cv_close (sqlite3_vtab_cursor *cur) { cells_cur *c = (cells_cur *)cur; sqlite3_finalize(c->st); free(c->c); sqlite3_free(c); return SQLITE_OK; }
static int cv_advance (cells_cur *c) {                       // to the next row of mw_rows that has cells
    for (;;) {
        int r = sqlite3_step(c->st);
        if (r == SQLITE_DONE) { c->eof = true; return SQLITE_OK; }
        if (r != SQLITE_ROW) return r;
        free(c->c); c->c = NULL; c->n = 0; c->i = 0;
        if (!mw_meta_row_cells(sqlite3_column_blob(c->st, 2), (size_t)sqlite3_column_bytes(c->st, 2), &c->c, &c->n)) return SQLITE_CORRUPT;
        if (c->n) return SQLITE_OK;
    }
}
static int cv_filter (sqlite3_vtab_cursor *cur, int idxn, const char *idxs, int argc, sqlite3_value **argv) {
    (void)idxn; (void)idxs; (void)argc; (void)argv;
    cells_cur *c = (cells_cur *)cur; cells_vt *v = (cells_vt *)cur->pVtab;
    sqlite3_finalize(c->st); c->st = NULL; c->eof = false; c->rowid = 0;
    int rc = sqlite3_prepare_v2(v->db, "SELECT tbl, pk, cells FROM mw_rows", -1, &c->st, NULL);
    if (rc != SQLITE_OK) { c->eof = true; return SQLITE_OK; }                  // (no table yet: no cells)
    return cv_advance(c);
}
static int cv_next (sqlite3_vtab_cursor *cur) {
    cells_cur *c = (cells_cur *)cur; c->rowid++;
    if (++c->i < c->n) return SQLITE_OK;
    return cv_advance(c);
}
static int cv_eof (sqlite3_vtab_cursor *cur) { return ((cells_cur *)cur)->eof; }
static int cv_column (sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col) {
    cells_cur *c = (cells_cur *)cur; const mw_mcell *x = &c->c[c->i];
    switch (col) {
        case 0: sqlite3_result_int64(ctx, sqlite3_column_int64(c->st, 0)); break;
        case 1: sqlite3_result_blob(ctx, sqlite3_column_blob(c->st, 1), sqlite3_column_bytes(c->st, 1), SQLITE_TRANSIENT); break;
        case 2: sqlite3_result_int64(ctx, x->col == CRDT_COL_SENTINEL ? -1 : (int64_t)x->col); break;
        case 3: sqlite3_result_int64(ctx, x->cv); break;
        case 4: sqlite3_result_int64(ctx, x->dv); break;
        case 5: sqlite3_result_int64(ctx, x->seq); break;
        default: sqlite3_result_int64(ctx, x->site); break;
    }
    return SQLITE_OK;
}
static int cv_rowid (sqlite3_vtab_cursor *cur, sqlite3_int64 *r) { *r = ((cells_cur *)cur)->rowid; return SQLITE_OK; }
static const sqlite3_module cells_module = { 0, NULL, cv_connect, cv_bestindex, cv_disconnect, NULL, cv_open, cv_close, cv_filter, cv_next, cv_eof, cv_column, cv_rowid };
int mw_meta_register_views (sqlite3 *c) { return sqlite3_create_module(c, "mw_cells", &cells_module, NULL); }
