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
#include "multiwriter_runstore.h"
#include "multiwriter_internal.h"

static const char *SCHEMA_SQL =
    "CREATE TABLE IF NOT EXISTS mw_state(k TEXT PRIMARY KEY NOT NULL, v) WITHOUT ROWID;"
    "CREATE TABLE IF NOT EXISTS mw_sites(ord INTEGER PRIMARY KEY, id BLOB NOT NULL);"
    "CREATE TABLE IF NOT EXISTS mw_runs(run INTEGER PRIMARY KEY, age INTEGER NOT NULL, lvl INTEGER NOT NULL, nrows INTEGER NOT NULL, nblk INTEGER NOT NULL, dvmax INTEGER NOT NULL, metalen INTEGER NOT NULL, metaloc BLOB NOT NULL);"
    "CREATE TABLE IF NOT EXISTS mw_slots(slot INTEGER PRIMARY KEY, data BLOB NOT NULL);"
    "CREATE TABLE IF NOT EXISTS mw_free(chunk INTEGER PRIMARY KEY, bits BLOB NOT NULL);"
    "CREATE TABLE IF NOT EXISTS mw_resv(pid INTEGER PRIMARY KEY, slots BLOB NOT NULL);"
    "CREATE TABLE IF NOT EXISTS mw_drops(tbl INTEGER PRIMARY KEY, dv INTEGER NOT NULL);";

_Atomic uint64_t mw_ft[10];                                         // flush phases in ns (MW_CDC_STATS): collect, sort, reserve, build+slots, commit, finish, collect_done
static uint64_t now_ns (void) { struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec; }
static bool busyish (int rc) { int p = rc & 0xff; return p == SQLITE_BUSY || p == SQLITE_LOCKED; }

// The index that lets the export find the cells changed since a db_version costs a random insert per cell at every flush: it is created by the first export (the databases that never
// synchronise do not pay for it), and the flush maintains it from then on.
int mw_meta_export_index (sqlite3 *c) { (void)c; return SQLITE_OK; }                    // (the blocks know the newest db_version they hold: there is no index to build)

// ---- the row as the file holds it ----
// The cells of a row of a user table, packed (a row of a block of a run): its key, the largest db_version of its cells (what the export looks for) and all its cells, packed: a format byte, the number of cells, then
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
    if (sqlite3_prepare_v2(c, "SELECT count(*) FROM sqlite_schema WHERE name IN ('mw_state','mw_sites','mw_runs','mw_slots','mw_free','mw_drops','mw_resv')", -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) have = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);
    if (have == 7) return SQLITE_OK;
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
    sqlite3_close(m->mrd); m->mrd = NULL; sqlite3_close(m->mwr); m->mwr = NULL;
    free(m->uri); m->uri = NULL;
}

// ---- reading ----
static bool mf_hard (int rc) { return rc != SQLITE_OK && rc != SQLITE_DONE && rc != SQLITE_ROW && (rc & 0xff) != SQLITE_ERROR; }
// n keys, one read transaction of one connection of the pool (a read transaction per row costs more than the lookup: the page cache is dropped when the database changed)
static int load_keys (mw_meta *m, int n, const uint32_t *tbl, const uint8_t *const *pk, const size_t *pklen, mw_mcell **cells, int *ncells) {
    for (int i = 0; i < n; i++) { cells[i] = NULL; ncells[i] = 0; }
    if (!m->attached || !atomic_load(&m->tables_ok)) return 0;
    int slot = (int)(((uintptr_t)pthread_self() >> 4) % MW_RDN);
    for (int i = 0; i < MW_RDN; i++) { int k = (slot + i) % MW_RDN; if (pthread_mutex_trylock(&m->rdmu[k]) == 0) { slot = k; goto locked; } }
    pthread_mutex_lock(&m->rdmu[slot]);
locked:
    if (!m->rd[slot]) m->rd[slot] = open_conn(m);
    int rc = 0;
    if (!m->rd[slot]) rc = -1;                                                  // (a failure to open is not "no rows": the caller would build the metadata of a row from nothing)
    else if (!m->rds[slot]) {
        int prc = sqlite3_prepare_v2(m->rd[slot], rsx_blk_sql(), -1, &m->rds[slot], NULL);
        if (prc != SQLITE_OK) { sqlite3_finalize(m->rds[slot]); m->rds[slot] = NULL; if (mf_hard(prc)) rc = -1; }       // (no table yet: the rows are not in the file)
    }
    if (rc == 0 && m->rds[slot]) {
        sqlite3 *c = m->rd[slot];
        bool txn = sqlite3_exec(c, "BEGIN", NULL, NULL, NULL) == SQLITE_OK;
        mw_rman *man = NULL;
        rc = rsx_man(m->rsx, c, &man);
        if (rc == 0) { rc = rsx_get_many(m->rsx, man, m->rds[slot], n, tbl, pk, pklen, cells, ncells); rsx_man_release(man); }
        if (txn) sqlite3_exec(c, "COMMIT", NULL, NULL, NULL);
    }
    pthread_mutex_unlock(&m->rdmu[slot]);
    if (rc == 0) {                                                          // cells older than a drop of the table that the flush has not written yet are dead
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
int mw_metafile_load (mw_meta *m, uint32_t tbl, const void *pk, size_t pklen, mw_mcell **cells, int *n) {
    const uint8_t *p = pk; return load_keys(m, 1, &tbl, &p, &pklen, cells, n);
}
int mw_metafile_load_many (mw_meta *m, int n, const uint32_t *tbl, const uint8_t *const *pk, const size_t *pklen, mw_mcell **cells, int *ncells) {
    return load_keys(m, n, tbl, pk, pklen, cells, ncells);
}

// the filter of the keys the file knows (every row of a table has a causal-length entry): a scan of the keys
static void bloom_cb (void *ctx, uint32_t tbl, const uint8_t *pk, size_t pklen) { mw_meta_bloom_add(ctx, tbl, pk, pklen); }
int mw_metafile_load_tombstones (mw_meta *m) {
    if (!m->attached) return 0;
    sqlite3 *c = open_conn(m); if (!c) return -1;
    int rc = rsx_scan_keys(m->rsx, c, bloom_cb, m);
    sqlite3_close(c);
    return rc ? -1 : 0;
}

// ---- recovery / first use ----
// What the file tables say: the epoch they cover, the site ids they hold (installed through mw_meta_site_install: ord 0 is this database's own id)
// one value of mw_state: a key that is not there is 0; a failure to read is an error (starting from nothing instead would give the database a new identity and a fresh epoch)
static int mf_state (sqlite3 *c, const char *k, uint64_t *v) {
    sqlite3_stmt *st = NULL; char sql[100]; snprintf(sql, sizeof sql, "SELECT v FROM mw_state WHERE k = '%s'", k);
    int rc = sqlite3_prepare_v2(c, sql, -1, &st, NULL);
    if (rc == SQLITE_OK) { rc = sqlite3_step(st); if (rc == SQLITE_ROW) { *v = (uint64_t)sqlite3_column_int64(st, 0); rc = SQLITE_OK; } else if (rc == SQLITE_DONE) rc = SQLITE_OK; }
    sqlite3_finalize(st);
    return mf_hard(rc) ? rc : SQLITE_OK;
}
int mw_metafile_load_state (mw_meta *m, uint64_t *F, uint64_t *hwm, uint32_t *sites_flushed, bool *have_own, uint8_t own[16]) {
    *F = 0; *hwm = 0; *sites_flushed = 0; *have_own = false;
    if (!m->attached) return SQLITE_OK;
    sqlite3 *c = open_conn(m); if (!c) return SQLITE_CANTOPEN;
    sqlite3_stmt *st = NULL; int have = 0;
    int rc = sqlite3_prepare_v2(c, "SELECT count(*) FROM sqlite_schema WHERE name IN ('mw_state','mw_sites','mw_runs')", -1, &st, NULL);
    if (rc == SQLITE_OK) { rc = sqlite3_step(st); if (rc == SQLITE_ROW) { have = sqlite3_column_int(st, 0); rc = SQLITE_OK; } }
    sqlite3_finalize(st); st = NULL;
    if (rc != SQLITE_OK) { sqlite3_close(c); return mf_hard(rc) ? rc : SQLITE_IOERR; }
    if (have == 3) {
        m->tables_ok = true;
        rc = mf_state(c, "meta_epoch", F);
        if (rc == SQLITE_OK) rc = mf_state(c, "dv_hwm", hwm);
        if (rc != SQLITE_OK) { sqlite3_close(c); return rc; }
        if (*hwm < *F) *hwm = *F;
        rc = sqlite3_prepare_v2(c, "SELECT ord, id FROM mw_sites ORDER BY ord", -1, &st, NULL);
        if (rc == SQLITE_OK) {
            int r;
            while ((r = sqlite3_step(st)) == SQLITE_ROW) {
                uint32_t ord = (uint32_t)sqlite3_column_int64(st, 0);
                if (sqlite3_column_bytes(st, 1) != 16) continue;
                if (ord == 0) { memcpy(own, sqlite3_column_blob(st, 1), 16); *have_own = true; }
                if (!m->shared) mw_meta_site_install(m, ord, sqlite3_column_blob(st, 1)); else mm_site_install(m, ord, sqlite3_column_blob(st, 1));
                if (ord + 1 > *sites_flushed) *sites_flushed = ord + 1;
            }
            rc = r == SQLITE_DONE ? SQLITE_OK : r;
        }
        sqlite3_finalize(st);
        if (rc != SQLITE_OK) { sqlite3_close(c); return rc; }
    }
    sqlite3_close(c);
    return SQLITE_OK;
}

int mw_meta_ready (mw_meta *m) {
    if (m->shared) return mm_ready(m);
    if (atomic_load(&m->ready)) return 0;
    pthread_mutex_lock(&m->file_mu);
    if (atomic_load(&m->ready)) { pthread_mutex_unlock(&m->file_mu); return 0; }
    uint64_t F, hwm; uint8_t own[16]; bool have_own;
    int lrc = mw_metafile_load_state(m, &F, &hwm, &m->sites_flushed, &have_own, own);
    if (lrc != SQLITE_OK) { pthread_mutex_unlock(&m->file_mu); return lrc; }
    if (have_own) memcpy(m->sites[0], own, 16);
    m->origin = (int64_t)hwm;                                   // this incarnation's epochs start again at 1: its db_versions go on from the largest the file has seen
    atomic_store(&m->hwm, hwm);
    atomic_store(&m->flushed, F);
    if (mw_metafile_load_tombstones(m) != 0) { pthread_mutex_unlock(&m->file_mu); return SQLITE_IOERR_READ; }       // (without the filter an insert of a key that had a life would pass for a first one)
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
        size_t mn = b->blockmin ? b->blockmin : (1u << 20), cap = n > mn ? n : mn;
        if (b->nblocks == b->capblocks) { int nc = b->capblocks ? b->capblocks * 2 : 16; uint8_t **nb = realloc(b->blocks, (size_t)nc * sizeof *nb); if (!nb) return NULL; b->blocks = nb; b->capblocks = nc; }
        uint8_t *blk = malloc(cap); if (!blk) return NULL;
        b->blocks[b->nblocks++] = blk; b->used = 0; b->blockcap = cap;
    }
    void *p = b->blocks[b->nblocks - 1] + b->used; b->used += n; return p;
}
fitem *mw_fbatch_add_row (fbatch *b, uint32_t tbl, const uint8_t *pk, uint32_t pklen, const mw_mcell *c, int n) {
    if (b->n == b->cap) { int nc = b->cap ? b->cap * 2 : 1024; fitem *nv = realloc(b->v, (size_t)nc * sizeof *nv); if (!nv) return NULL; b->v = nv; b->cap = nc; }
    size_t need = ROW_PACK_MAX(n);
    if (n && need > b->tmpcap) { uint8_t *nt = realloc(b->tmp, need * 2); if (!nt) return NULL; b->tmp = nt; b->tmpcap = need * 2; }
    size_t len = n ? row_pack(c, n, b->tmp) : 0;
    uint8_t *k = mw_fbatch_alloc(b, (size_t)pklen + len + 1);                 // (the key and, after it, the packed cells: one place)
    if (!k) return NULL;
    memcpy(k, pk, pklen); if (len) memcpy(k + pklen, b->tmp, len);
    int64_t dv = 0; for (int i = 0; i < n; i++) if (c[i].dv > dv) dv = c[i].dv;
    fitem *it = &b->v[b->n++]; *it = (fitem){ tbl, k, pklen, (uint32_t)n, (uint32_t)len, k + pklen, dv };
    return it;
}
void mw_fbatch_free (fbatch *b) { for (int i = 0; i < b->nblocks; i++) free(b->blocks[i]); free(b->blocks); free(b->v); free(b->tmp); memset(b, 0, sizeof *b); }

// The changes that wait for the flusher are kept by the stripes as packed rows, in the order they happened (multiwriter_meta.c: install_row). A flush takes each stripe's buffer whole, a
// moment under its lock, and reads nothing else: the entries of the table are not visited (visiting every entry twice, once to read it and once to clean it, was a quarter of the
// flusher's time: a cache miss each). The items are listed for the sort; the bytes they point to stay in the taken buffers until the flush is over.
static int collect (mw_meta *m, uint64_t F, fbatch *out, dpend *det) {
    (void)F;
    size_t total = 0;
    for (int s = 0; s < STRIPES; s++) {
        stripe *st = &m->st[s];
        pthread_mutex_lock(&st->mu); det[s].b = st->pend; det[s].seq = st->seq; memset(&st->pend, 0, sizeof st->pend); st->pend.blockmin = det[s].b.blockmin; pthread_mutex_unlock(&st->mu);
        total += (size_t)det[s].b.n;
    }
    out->v = malloc((total ? total : 1) * sizeof *out->v); if (!out->v) return -1;
    out->cap = (int)(total ? total : 1);
    for (int s = 0; s < STRIPES; s++) { if (det[s].b.n) memcpy(out->v + out->n, det[s].b.v, (size_t)det[s].b.n * sizeof *out->v); out->n += det[s].b.n; }
    return 0;
}
// What is done with the taken buffers: a flush that wrote them moves the stripes' flushed points (their entries are clean now, and the flusher frees the ones over the cache's share); one
// that did not puts them back in front of what came meanwhile.
static void collect_done (mw_meta *m, dpend *det, bool ok) {
    uint64_t done = 0;
    for (int s = 0; s < STRIPES; s++) {
        stripe *st = &m->st[s]; dpend *d = &det[s];
        if (ok) {
            pthread_mutex_lock(&st->mu); if (d->seq > st->flushed_seq) st->flushed_seq = d->seq; pthread_mutex_unlock(&st->mu);
            done += (uint64_t)d->b.n;
            mw_fbatch_free(&d->b);
        } else if (d->b.n) {
            pthread_mutex_lock(&st->mu);
            fbatch *a = &d->b, *z = &st->pend;                                   // a (older) then z (newer), in one buffer
            int nn = a->n + z->n; fitem *nv = malloc((size_t)(nn ? nn : 1) * sizeof *nv);
            uint8_t **nb = malloc((size_t)(a->nblocks + z->nblocks + 1) * sizeof *nb);
            if (nv && nb) {
                memcpy(nv, a->v, (size_t)a->n * sizeof *nv); memcpy(nv + a->n, z->v, (size_t)z->n * sizeof *nv);
                memcpy(nb, a->blocks, (size_t)a->nblocks * sizeof *nb); memcpy(nb + a->nblocks, z->blocks, (size_t)z->nblocks * sizeof *nb);
                fbatch merged = *z; merged.v = nv; merged.n = merged.cap = nn; merged.blocks = nb; merged.nblocks = merged.capblocks = a->nblocks + z->nblocks;
                free(a->v); free(a->blocks); free(a->tmp); free(z->v); free(z->blocks); *z = merged; memset(a, 0, sizeof *a);
            } else { free(nv); free(nb); done += 0; }                          // (no memory: the taken changes are lost to this process's file until the rows change again; the log still has them)
            pthread_mutex_unlock(&st->mu);
            if (a->n) mw_fbatch_free(a);
        }
        if (ok) mw_meta_trim(m, s);
    }
    if (done) atomic_fetch_sub(&m->ndirty, done);
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
    int c = fitem_full(x->it, y->it);
    return c ? c : (x->it < y->it ? -1 : x->it > y->it);                       // (the same key twice: the order they were made in)
}
// Sorts, and keeps the last of the items that have one key (a row changed twice between two flushes: only its newest state goes to the file). Returns the number of items left.
static int sort_items (fitem *v, int n) {
    if (n < 2) return n;
    skey *a = malloc((size_t)n * sizeof *a), *b = malloc((size_t)n * sizeof *b); fitem *copy = malloc((size_t)n * sizeof *copy);
    if (!a || !b || !copy) { free(a); free(b); free(copy); return n; }
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
    int k = 0;
    for (int i = 0; i < n; i++) {
        if (i + 1 < n && a[i].tbl == a[i + 1].tbl && a[i].pfx == a[i + 1].pfx && fitem_full(a[i].it, a[i + 1].it) == 0) continue;       // (a later one with the same key follows)
        copy[k++] = *a[i].it;
    }
    memcpy(v, copy, (size_t)k * sizeof *v);
    free(a); free(b); free(copy);
    return k;
}

// One transaction of the flush: the items [i0, i1) as a run of level 0 and, in the last one, the sites, the flushed point and the high-water mark. A flush is several of them (a transaction of
// hundreds of thousands of rows would hold its whole write set in memory and in the log): the points move only with the last, and what the others wrote is what a replay would write again.
static int write_batch (mw_meta *m, sqlite3 *c, fitem *v, int i0, int i1, bool last, uint64_t V, uint64_t hwm, uint32_t nsites, uint32_t sflushed, const struct mw_purge *purge, int npurge) {
    int rc;
    if (!atomic_load_explicit(&m->schema_seen, memory_order_relaxed)) { rc = mw_meta_schema(c); if (rc != SQLITE_OK) return rc; atomic_store(&m->schema_seen, true); }
    atomic_store(&m->tables_ok, true);
    uint64_t tp0 = now_ns();
    if (i1 > i0 && (rc = rsx_reserve_items(m->rsx, c, v, i0, i1)) != SQLITE_OK) return rc;
    atomic_fetch_add(&mw_ft[2], now_ns() - tp0);              // (the slots the blocks will be written to: taken before the transaction)
    rsx_wlock(m->rsx);
    if ((rc = sqlite3_exec(c, "BEGIN", NULL, NULL, NULL)) != SQLITE_OK) { rsx_wunlock(m->rsx); return rc; }
    rsx_tx *t = rsx_tx_begin(m->rsx, c);
    if (!t) { sqlite3_exec(c, "ROLLBACK", NULL, NULL, NULL); rsx_wunlock(m->rsx); return SQLITE_NOMEM; }
    sqlite3_stmt *site = NULL, *state = NULL, *hw = NULL;
    sqlite3_prepare_v2(c, "INSERT OR REPLACE INTO mw_sites(ord, id) VALUES(?1, ?2)", -1, &site, NULL);
    sqlite3_prepare_v2(c, "INSERT OR REPLACE INTO mw_state(k, v) VALUES('meta_epoch', ?1)", -1, &state, NULL);
    sqlite3_prepare_v2(c, "INSERT OR REPLACE INTO mw_state(k, v) VALUES('dv_hwm', ?1)", -1, &hw, NULL);
    rc = (site && state && hw) ? SQLITE_OK : SQLITE_ERROR;
    for (int i = 0; i0 == 0 && i < npurge && rc == SQLITE_OK; i++) rc = rsx_tx_drop_table(t, c, purge[i].tbl, (int64_t)purge[i].epoch);
    uint64_t cells = 0;
    tp0 = now_ns();
    if (rc == SQLITE_OK) rc = rsx_tx_add_items(t, c, v, i0, i1);
    atomic_fetch_add(&mw_ft[3], now_ns() - tp0);
    for (int i = i0; i < i1; i++) cells += v[i].n;
    for (uint32_t o = sflushed; last && o < nsites && rc == SQLITE_OK; o++) {
        uint8_t id[16]; if (!mw_meta_site_id(m, o, id)) continue;
        sqlite3_bind_int64(site, 1, o); sqlite3_bind_blob(site, 2, id, 16, SQLITE_STATIC);
        if (sqlite3_step(site) != SQLITE_DONE) rc = sqlite3_errcode(c); sqlite3_reset(site);
    }
    if (V > hwm) hwm = V;
    if (last && rc == SQLITE_OK) { sqlite3_bind_int64(state, 1, (int64_t)V); if (sqlite3_step(state) != SQLITE_DONE) rc = sqlite3_errcode(c); sqlite3_reset(state); }
    if (last && rc == SQLITE_OK) { sqlite3_bind_int64(hw, 1, (int64_t)hwm); if (sqlite3_step(hw) != SQLITE_DONE) rc = sqlite3_errcode(c); sqlite3_reset(hw); }
    if (last) m->new_hwm = hwm;
    if (rc == SQLITE_OK) rc = rsx_tx_finish(t, c);
    sqlite3_finalize(site); sqlite3_finalize(state); sqlite3_finalize(hw);
    tp0 = now_ns();
    if (rc == SQLITE_OK) rc = sqlite3_exec(c, "COMMIT", NULL, NULL, NULL);
    atomic_fetch_add(&mw_ft[4], now_ns() - tp0);
    if (rc != SQLITE_OK) sqlite3_exec(c, "ROLLBACK", NULL, NULL, NULL);
    else atomic_fetch_add(&m->flushed_cells, cells);
    rsx_tx_end(t, rc == SQLITE_OK);
    rsx_wunlock(m->rsx);
    return rc;
}

// the items [i0, n) in transactions of RUN_ROWS rows (the first also does the purges, the last the sites and the points when `last_range`)
#define RUN_ROWS 32768
static int write_range (mw_meta *m, sqlite3 *c, fitem *v, int i0, int n, bool last_range, uint64_t V, uint64_t hwm, uint32_t nsites, uint32_t sflushed, const struct mw_purge *purge, int npurge) {
    int rc = SQLITE_OK, need_slots_retries = 0;
    for (int a = i0; a <= n && rc == SQLITE_OK; ) {
        int b = a + RUN_ROWS < n ? a + RUN_ROWS : n;
        bool last = last_range && b >= n;
        rc = SQLITE_BUSY;
        for (int attempt = 0; attempt < 200 && busyish(rc); attempt++) {
            rc = write_batch(m, c, v, a, b, last, V, hwm, nsites, sflushed, purge, npurge);
            if (rc == RSX_NEED_SLOTS) { (void)rsx_reserve_items(m->rsx, c, v, a, b); rc = SQLITE_BUSY; attempt--; if (++need_slots_retries > 20) { rc = SQLITE_FULL; break; } continue; }      // (more incompressible than we thought: more slots, and again)
            if (busyish(rc)) { atomic_fetch_add(&m->flush_retries, 1); usleep(500 * (unsigned)(attempt < 20 ? attempt + 1 : 20)); }
        }
        if (b >= n) break;
        a = b;
    }
    return rc;
}
void mw_meta_run_stats (mw_meta *m, uint64_t out[13]) { rsx_stats st; rsx_stats_get(m->rsx, &st); out[11] = st.merge_retries; out[12] = st.swept_slots; { int l0, all; rsx_backlog(m->rsx, &l0, &all); out[9] = (uint64_t)l0; out[10] = (uint64_t)all; } out[0] = st.gets; out[1] = st.run_probes; out[2] = st.bloom_skips; out[3] = st.blk_reads; out[4] = st.cache_hits; out[5] = st.merges; out[6] = st.merged_rows; out[7] = st.runs_written; out[8] = (uint64_t)st.nruns; }

// ---- the merger thread (one process) ----
static void *merger_main (void *arg) {
    mw_meta *m = arg;
    for (;;) {
        pthread_mutex_lock(&m->mth_mu);
        while (!m->mth_stop && !m->mkick) {
            struct timespec until; clock_gettime(CLOCK_REALTIME, &until); until.tv_nsec += 500 * 1000000L; if (until.tv_nsec >= 1000000000L) { until.tv_sec++; until.tv_nsec -= 1000000000L; }
            if (pthread_cond_timedwait(&m->mth_cv, &m->mth_mu, &until) != 0) break;
        }
        bool stop = m->mth_stop; m->mkick = false; pthread_mutex_unlock(&m->mth_mu);
        if (stop) break;
        if (m->shared && !mw_mp_meta_lock(m->db, 2, false)) continue;                          // (several processes: one of them merges at a time; the others' flushes go on)
        if (!m->mrd) m->mrd = open_conn(m);
        if (!m->mwr) m->mwr = open_conn(m);
        if (m->mrd && m->mwr) {                                                                  // (slots of processes that are gone come back: at the start and then every few seconds)
            static uint64_t every = 0; if (!every) { const char *e = getenv("MW_META_SWEEP_MS"); every = (e ? (uint64_t)atoll(e) : 10000) * 1000000ull; }
            if (!m->swept || now_ns() - m->sweep_ns > every) { (void)rsx_sweep(m->rsx, m->mrd, m->mwr); m->swept = true; m->sweep_ns = now_ns(); }
        }
        if (m->mrd && m->mwr) for (;;) {
            pthread_mutex_lock(&m->mth_mu); bool st = m->mth_stop; pthread_mutex_unlock(&m->mth_mu);
            if (st || rsx_merge(m->rsx, m->mrd, m->mwr, m->fanout, m->part_rows) <= 0) break;
        }
        if (m->shared) mw_mp_meta_unlock(m->db, 2);
    }
    return NULL;
}
static void merge_kick (mw_meta *m) {
    if (getenv("MW_META_NOMERGE")) return;
    pthread_mutex_lock(&m->mth_mu);
    m->mkick = true;
    if (!m->mth_running && !m->mth_stop) { if (pthread_create(&m->mth, NULL, merger_main, m) == 0) m->mth_running = true; }
    pthread_cond_signal(&m->mth_cv);
    pthread_mutex_unlock(&m->mth_mu);
}

static uint64_t flush_rows (void);
static int flush_impl (mw_meta *m, bool wait) {
    if (!m->attached) return 0;
    mw_meta_ready(m);
    mw_shm *sh = m->shared ? m->db->shm : NULL;
    pthread_mutex_lock(&m->file_mu);
    if (sh && !mw_mp_meta_lock(m->db, 1, wait)) { pthread_mutex_unlock(&m->file_mu); return 0; }
    if (sh && !wait) {
        // The flushers of all the processes wake up for the same dirty count, and the one that gets the lock takes everything: the others, behind it, would write a run of what
        // the commits since then added (a few rows). They look again with the lock held, and leave it to the next time unless there is enough or it is long since the last flush.
        uint64_t rows = flush_rows();
        uint64_t d = mw_meta_dirty(m), age = now_ns() - atomic_load(&sh->meta_flush_ns);
        if (d < rows / 2 && age < 20ull * 1000000ull) { mw_mp_meta_unlock(m->db, 1); pthread_mutex_unlock(&m->file_mu); return 0; }
    }
    uint64_t t0 = now_ns();
    int64_t origin = mw_meta_origin(m);
    uint64_t Ve = sh ? atomic_load_explicit(&sh->committed_epoch, memory_order_acquire) : atomic_load(&m->db->epoch);
    uint64_t V = Ve + (uint64_t)origin;                                          // (db_versions from here on)
    uint64_t F = sh ? atomic_load(&sh->meta_flushed) : atomic_load(&m->flushed);
    uint64_t Fe = F > (uint64_t)origin ? F - (uint64_t)origin : 0;               // the same point as an epoch of this incarnation
    uint32_t nsites = sh ? atomic_load(&sh->nsites) : 0, sflushed = sh ? atomic_load(&sh->sites_flushed) : m->sites_flushed;
    if (!sh) { pthread_mutex_lock(&m->site_mu); nsites = m->nsites; pthread_mutex_unlock(&m->site_mu); }
    int rc = SQLITE_OK;
    dpend det[STRIPES]; bool taken = false;
    struct mw_purge *purge = NULL; int npurge = 0;
    if (sh) { uint32_t np = atomic_load_explicit(&sh->npurge, memory_order_acquire); if (np) { purge = malloc(np * sizeof *purge); if (purge) for (uint32_t i = 0; i < np; i++) purge[npurge++] = (struct mw_purge){ atomic_load(&sh->purge[i].tbl), atomic_load(&sh->purge[i].epoch) }; } }
    else { pthread_mutex_lock(&m->purge_mu); if (m->npurge) { purge = malloc((size_t)m->npurge * sizeof *purge); if (purge) { memcpy(purge, m->purge, (size_t)m->npurge * sizeof *purge); npurge = m->npurge; } } pthread_mutex_unlock(&m->purge_mu); }
    bool dirty = sh ? atomic_load(&sh->meta_last) > Fe : atomic_load(&m->ndirty) != 0;
    if (!dirty && nsites <= sflushed && npurge == 0) goto out;
    if (!m->wr) m->wr = open_conn(m);
    if (!m->wr) { rc = SQLITE_CANTOPEN; goto out; }
    fbatch fb = {0};
    if (!sh) { memset(det, 0, sizeof det); taken = true; }
    uint64_t tc0 = now_ns();
    if ((sh ? mm_collect(m, F, Fe, &fb) : collect(m, F, &fb, det)) != 0) { rc = SQLITE_NOMEM; goto out; }
    fitem *v = fb.v; int n = fb.n;
    atomic_fetch_add(&mw_ft[0], now_ns() - tc0); tc0 = now_ns();
    n = sort_items(v, n);
    atomic_fetch_add(&mw_ft[1], now_ns() - tc0);
    uint64_t hw0 = sh ? atomic_load(&sh->dv_hwm) : atomic_load(&m->hwm);
    for (int i = 0; i < n; i++) if ((uint64_t)v[i].dv > hw0) hw0 = (uint64_t)v[i].dv;
    if (getenv("MW_EXP_SKIPWRITE")) rc = SQLITE_OK; else          // (experiment: the cost of everything but the file)
    rc = write_range(m, m->wr, v, 0, n, true, V, hw0, nsites, sflushed, purge, npurge);
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
            { uint64_t td = now_ns(); collect_done(m, det, true); atomic_fetch_add(&mw_ft[6], now_ns() - td); } taken = false;
        }
        atomic_fetch_add(&m->n_flushes, 1);
    }
    if (rc == SQLITE_OK && !atomic_load(&m->quiescing) && !getenv("MW_META_NOMERGE")) {         // the runs the flushes made are merged into bigger ones (a few rounds a flush at most)
        merge_kick(m);
    }
    atomic_fetch_add(&m->flush_ns, now_ns() - t0);
    m->last_flush_ns = now_ns();
    if (sh) atomic_store(&sh->meta_flush_ns, m->last_flush_ns);
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
// Rows that wait before the flusher writes them as a run. Every flush is a run to merge, a transaction and a record in the log: at 20k commits a second 16384 rows were 130 runs a second, more than the
// merger could take (it fell behind, the run count slowed the writers, and a run of the benchmark was 8k or 20k tx/s depending on whether it ever fell behind). 131072: stable, 20-29k (16 threads 26.7k -> 29.1k).
static uint64_t flush_rows (void) { static uint64_t r; if (!r) { const char *e = getenv("MW_META_FLUSH_ROWS"); r = e && atoll(e) > 0 ? (uint64_t)atoll(e) : 131072; } return r; }
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
        const char *em = getenv("MW_META_FLUSH_MS");           // (tests: flush very often)
        uint64_t rows = flush_rows(), ms = em ? (uint64_t)atoll(em) : 250;
        d = mw_meta_dirty(m);
        uint64_t lastf = m->last_flush_ns; if (m->shared) { uint64_t o = atomic_load(&m->db->shm->meta_flush_ns); if (o > lastf) lastf = o; }          // (several processes: the newest flush of anybody)
        uint64_t age = now_ns() - lastf;
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
    pthread_mutex_lock(&m->mth_mu); m->mth_stop = true; pthread_cond_broadcast(&m->mth_cv); bool mrun = m->mth_running; pthread_mutex_unlock(&m->mth_mu);
    if (mrun) { pthread_join(m->mth, NULL); m->mth_running = false; }
    if (m->shared ? atomic_load(&m->db->shm->meta_state) == 2 : atomic_load(&m->ready)) mw_meta_flush(m);
    if (m->wr && (m->shared ? atomic_load(&m->db->shm->meta_state) == 2 : atomic_load(&m->ready))) (void)rsx_release_pool(m->rsx, m->wr);      // (the slots we hold go back to the free ones, and our entry in the register of reservations goes)
    pthread_mutex_lock(&m->th_mu); m->th_stop = false; pthread_mutex_unlock(&m->th_mu);       // (a later open starts the thread again)
    pthread_mutex_lock(&m->mth_mu); m->mth_stop = false; pthread_mutex_unlock(&m->mth_mu);
    sqlite3 *conns[MW_RDN + 3]; sqlite3_stmt *stmts[MW_RDN]; int nc = 0;
    pthread_mutex_lock(&m->file_mu);
    for (int i = 0; i < MW_RDN; i++) { pthread_mutex_lock(&m->rdmu[i]); stmts[i] = m->rds[i]; m->rds[i] = NULL; if (m->rd[i]) conns[nc++] = m->rd[i]; m->rd[i] = NULL; pthread_mutex_unlock(&m->rdmu[i]); }
    if (m->wr) conns[nc++] = m->wr;
    m->wr = NULL;
    if (m->mrd) { conns[nc++] = m->mrd; m->mrd = NULL; }
    if (m->mwr) { conns[nc++] = m->mwr; m->mwr = NULL; }
    pthread_mutex_unlock(&m->file_mu);
    atomic_store(&m->quiescing, 0);
    // the last connection to close releases the database for good, and that frees this store: nothing of it may be touched after the closes
    for (int i = 0; i < MW_RDN; i++) sqlite3_finalize(stmts[i]);
    for (int i = 0; i < nc; i++) sqlite3_close(conns[i]);
}

// ---- mw_cells: the cells of the runs as the rows of a table ----
// A read-only virtual table (eponymous: there is nothing to create) with the columns the cells have, for people and tools that want to look at the metadata in SQL:
// SELECT * FROM mw_cells; col is -1 for a row's own entry (the causal length). The current state of all rows is read into memory when a scan starts: it is a tool for looking, not for large databases.
typedef struct { uint32_t tbl; uint8_t *pk; uint32_t pklen; mw_mcell c; } vrow;
typedef struct { sqlite3_vtab base; sqlite3 *db; mw_meta *m; } cells_vt;
typedef struct { sqlite3_vtab_cursor base; vrow *r; int n, cap, i; } cells_cur;
static int cv_connect (sqlite3 *db, void *aux, int argc, const char *const *argv, sqlite3_vtab **out, char **err) {
    (void)argc; (void)argv; (void)err;
    int rc = sqlite3_declare_vtab(db, "CREATE TABLE x(tbl INTEGER, pk BLOB, col INTEGER, cv INTEGER, dv INTEGER, seq INTEGER, site INTEGER)");
    if (rc != SQLITE_OK) return rc;
    cells_vt *v = sqlite3_malloc(sizeof *v); if (!v) return SQLITE_NOMEM;
    memset(v, 0, sizeof *v); v->db = db; v->m = aux; *out = &v->base;
    return SQLITE_OK;
}
static int cv_disconnect (sqlite3_vtab *vt) { sqlite3_free(vt); return SQLITE_OK; }
static int cv_bestindex (sqlite3_vtab *vt, sqlite3_index_info *info) { (void)vt; info->estimatedCost = 1e9; info->estimatedRows = 1000000; return SQLITE_OK; }
static int cv_open (sqlite3_vtab *vt, sqlite3_vtab_cursor **out) { (void)vt; cells_cur *c = sqlite3_malloc(sizeof *c); if (!c) return SQLITE_NOMEM; memset(c, 0, sizeof *c); *out = &c->base; return SQLITE_OK; }
static void cv_clear (cells_cur *c) { for (int i = 0; i < c->n; i++) free(c->r[i].pk); free(c->r); c->r = NULL; c->n = c->cap = c->i = 0; }
static int cv_close (sqlite3_vtab_cursor *cur) { cells_cur *c = (cells_cur *)cur; cv_clear(c); sqlite3_free(c); return SQLITE_OK; }
static int cv_collect (void *ctx, uint32_t tbl, const uint8_t *pk, size_t pklen, const mw_mcell *cells, int n) {
    cells_cur *c = ctx;
    for (int i = 0; i < n; i++) {
        if (c->n == c->cap) { int nc = c->cap ? c->cap * 2 : 256; vrow *nr = realloc(c->r, (size_t)nc * sizeof *nr); if (!nr) return -1; c->r = nr; c->cap = nc; }
        uint8_t *k = malloc(pklen ? pklen : 1); if (!k) return -1; memcpy(k, pk, pklen);
        c->r[c->n++] = (vrow){ tbl, k, (uint32_t)pklen, cells[i] };
    }
    return 0;
}
static int cv_filter (sqlite3_vtab_cursor *cur, int idxn, const char *idxs, int argc, sqlite3_value **argv) {
    (void)idxn; (void)idxs; (void)argc; (void)argv;
    cells_cur *c = (cells_cur *)cur; cells_vt *v = (cells_vt *)cur->pVtab;
    cv_clear(c);
    if (!v->m) return SQLITE_OK;
    return rsx_scan_all(v->m->rsx, v->db, cv_collect, c) == 0 ? SQLITE_OK : SQLITE_CORRUPT;
}
static int cv_next (sqlite3_vtab_cursor *cur) { ((cells_cur *)cur)->i++; return SQLITE_OK; }
static int cv_eof (sqlite3_vtab_cursor *cur) { cells_cur *c = (cells_cur *)cur; return c->i >= c->n; }
static int cv_column (sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col) {
    cells_cur *c = (cells_cur *)cur; const vrow *r = &c->r[c->i]; const mw_mcell *x = &r->c;
    switch (col) {
        case 0: sqlite3_result_int64(ctx, r->tbl); break;
        case 1: sqlite3_result_blob(ctx, r->pk, (int)r->pklen, SQLITE_TRANSIENT); break;
        case 2: sqlite3_result_int64(ctx, x->col == CRDT_COL_SENTINEL ? -1 : (int64_t)x->col); break;
        case 3: sqlite3_result_int64(ctx, x->cv); break;
        case 4: sqlite3_result_int64(ctx, x->dv); break;
        case 5: sqlite3_result_int64(ctx, x->seq); break;
        default: sqlite3_result_int64(ctx, x->site); break;
    }
    return SQLITE_OK;
}
static int cv_rowid (sqlite3_vtab_cursor *cur, sqlite3_int64 *r) { *r = ((cells_cur *)cur)->i; return SQLITE_OK; }
static const sqlite3_module cells_module = { 0, NULL, cv_connect, cv_bestindex, cv_disconnect, NULL, cv_open, cv_close, cv_filter, cv_next, cv_eof, cv_column, cv_rowid };
int mw_meta_register_views (sqlite3 *c, mw_meta *m) { return sqlite3_create_module(c, "mw_cells", &cells_module, m); }

// ---- the export ----
typedef struct { uint8_t *pool; size_t n, cap; uint32_t *tbl, *off, *len; size_t nk, capk; bool bad; } keyset;
static void keyset_add (void *ctx, uint32_t tbl, const uint8_t *pk, size_t pklen) {
    keyset *k = ctx; if (k->bad) return;
    if (k->nk == k->capk) { size_t nc = k->capk ? k->capk * 2 : 1024; uint32_t *a = realloc(k->tbl, nc * 4), *b = realloc(k->off, nc * 4), *c = realloc(k->len, nc * 4); if (a) k->tbl = a; if (b) k->off = b; if (c) k->len = c; if (!a || !b || !c) { k->bad = true; return; } k->capk = nc; }
    if (k->n + pklen > k->cap) { size_t nc = (k->n + pklen) * 2 + 4096; uint8_t *np = realloc(k->pool, nc); if (!np) { k->bad = true; return; } k->pool = np; k->cap = nc; }
    memcpy(k->pool + k->n, pk, pklen); k->tbl[k->nk] = tbl; k->off[k->nk] = (uint32_t)k->n; k->len[k->nk] = (uint32_t)pklen; k->nk++; k->n += pklen;
}
static keyset *g_sort_ks;
static int ks_cmp (const void *a, const void *b) {
    uint32_t i = *(const uint32_t *)a, j = *(const uint32_t *)b; const keyset *k = g_sort_ks;
    rs_key x = { k->tbl[i], k->pool + k->off[i], k->len[i] }, y = { k->tbl[j], k->pool + k->off[j], k->len[j] }; int c = rs_key_cmp(&x, &y);
    return c ? c : (i < j ? -1 : i > j);
}
// the cells with a db_version in (since, upto], read in the snapshot of `c` (a transaction is open on it): the blocks that hold something newer than `since` give the keys, the newest
// state of each key (a key can be in several runs) gives the cells
int mw_metafile_export (mw_meta *m, sqlite3 *c, int64_t since, int64_t upto, mw_xcell **out, size_t *nout, uint8_t **pkpool) {
    *out = NULL; *nout = 0; *pkpool = NULL;
    if (!atomic_load(&m->tables_ok)) return 0;
    mw_rman *man = NULL; int rc = rsx_man(m->rsx, c, &man); if (rc) return -1;
    keyset ks = {0};
    rc = rsx_scan_since(m->rsx, c, man, since, keyset_add, &ks);
    if (rc || ks.bad) { rsx_man_release(man); free(ks.pool); free(ks.tbl); free(ks.off); free(ks.len); return -1; }
    uint32_t *ord = malloc((ks.nk ? ks.nk : 1) * sizeof *ord); if (!ord) { rsx_man_release(man); free(ks.pool); free(ks.tbl); free(ks.off); free(ks.len); return -1; }
    for (size_t i = 0; i < ks.nk; i++) ord[i] = (uint32_t)i;
    g_sort_ks = &ks; qsort(ord, ks.nk, sizeof *ord, ks_cmp);                                                    // (the export is not called from two threads at once: a database has one at a time)
    sqlite3_stmt *st = NULL; rc = ks.nk ? sqlite3_prepare_v2(c, rsx_blk_sql(), -1, &st, NULL) : SQLITE_OK;
    mw_xcell *xs = NULL; size_t nx = 0, capx = 0;
    const int B = 256;
    for (size_t i0 = 0; rc == SQLITE_OK && i0 < ks.nk; ) {
        uint32_t tb[256]; const uint8_t *pk[256]; size_t pl[256]; mw_mcell *cells[256]; int nc[256]; int bn = 0; size_t i = i0;
        while (i < ks.nk && bn < B) {                                                                           // (the same key again: once)
            uint32_t a = ord[i]; if (i > i0 && ks.tbl[a] == tb[bn - 1] && ks.len[a] == pl[bn - 1] && !memcmp(ks.pool + ks.off[a], pk[bn - 1], pl[bn - 1])) { i++; continue; }
            tb[bn] = ks.tbl[a]; pk[bn] = ks.pool + ks.off[a]; pl[bn] = ks.len[a]; bn++; i++;
        }
        if (rsx_get_many(m->rsx, man, st, bn, tb, pk, pl, cells, nc) != 0) { rc = -1; break; }
        for (int j = 0; j < bn; j++) {
            int64_t clv = 1; for (int q = 0; q < nc[j]; q++) if (cells[j][q].col == CRDT_COL_SENTINEL) clv = cells[j][q].cv;
            for (int q = 0; q < nc[j]; q++) {
                if (cells[j][q].dv <= since || cells[j][q].dv > upto) continue;
                if (nx == capx) { size_t n2 = capx ? capx * 2 : 1024; mw_xcell *nxs = realloc(xs, n2 * sizeof *xs); if (!nxs) { rc = -1; break; } xs = nxs; capx = n2; }
                xs[nx++] = (mw_xcell){ tb[j], (uint32_t)(pk[j] - ks.pool), (uint32_t)pl[j], cells[j][q], clv };
            }
            free(cells[j]);
        }
        i0 = i;
    }
    sqlite3_finalize(st); rsx_man_release(man); free(ord); free(ks.tbl); free(ks.off); free(ks.len);
    if (rc) { free(xs); free(ks.pool); return -1; }
    *out = xs; *nout = nx; *pkpool = ks.pool;
    return 0;
}

// back-pressure of the runs: how long (microseconds) a commit waits when the merges are far behind the flushes. Every run is one more place to look in, and the file grows with them: the writers
// slow down to the pace of the merges, the flush itself is never held (the log can only be compacted up to the flushed point).
uint32_t mw_meta_run_pressure (mw_meta *m) {
    static int max_l0 = -1; if (max_l0 < 0) { const char *e = getenv("MW_META_MAX_RUNS"); max_l0 = e && atoi(e) > 0 ? atoi(e) : 48; }
    int l0, all; rsx_backlog(m->rsx, &l0, &all);
    int over = l0 - max_l0, over_all = all - max_l0 * 3; if (over_all > over) over = over_all;
    if (over <= 0) return 0;
    uint32_t w = (uint32_t)over * 150u; return w > 20000u ? 20000u : w;
}

// The memory table forgets everything (a recovery in place rolled the database back to its last durable commit: the cells of the commits that were rolled back must not be flushed, and
// the rest is rebuilt from the file and the log as at an open). The flusher and the merger stop first; the next commit starts them again and brings the state in (mw_meta_ready).
void mw_meta_reset (mw_meta *m) {
    if (!m->attached) return;
    pthread_mutex_lock(&m->th_mu); m->th_stop = true; pthread_cond_broadcast(&m->th_cv); bool run = m->th_running; pthread_mutex_unlock(&m->th_mu);
    if (run) { pthread_join(m->th, NULL); m->th_running = false; }
    pthread_mutex_lock(&m->mth_mu); m->mth_stop = true; pthread_cond_broadcast(&m->mth_cv); bool mrun = m->mth_running; pthread_mutex_unlock(&m->mth_mu);
    if (mrun) { pthread_join(m->mth, NULL); m->mth_running = false; }
    pthread_mutex_lock(&m->file_mu);
    for (int i = 0; i < STRIPES; i++) {
        stripe *s = &m->st[i]; pthread_mutex_lock(&s->mu);
        for (size_t k = 0; k < s->nb; k++) for (mentry *e = s->b[k], *nx; e; e = nx) { nx = e->next; entry_free(s, e); }
        memset(s->b, 0, s->nb * sizeof *s->b); s->n = 0; s->bytes = 0; { size_t bm = s->pend.blockmin; mw_fbatch_free(&s->pend); s->pend.blockmin = bm; } s->flushed_seq = s->seq; s->backoff = 0; s->gen++;
        pthread_mutex_unlock(&s->mu);
    }
    atomic_store(&m->ndirty, 0); atomic_store(&m->rows, 0); atomic_store(&m->bytes, 0);
    pthread_mutex_lock(&m->purge_mu); m->npurge = 0; pthread_mutex_unlock(&m->purge_mu);
    if (m->bloom) memset(m->bloom, 0, ((size_t)1 << 23) / 8);
    m->kicked = false; m->kick_pending = false;
    atomic_store(&m->ready, false);
    pthread_mutex_unlock(&m->file_mu);
    pthread_mutex_lock(&m->th_mu); m->th_stop = false; pthread_mutex_unlock(&m->th_mu);
    pthread_mutex_lock(&m->mth_mu); m->mth_stop = false; pthread_mutex_unlock(&m->mth_mu);
}
