//
//  multiwriter_meta_priv.h: the layout of the metadata store, shared by multiwriter_meta.c (the table) and multiwriter_metafile.c (the file tables, the flusher).
//
#ifndef MW_META_PRIV_H
#define MW_META_PRIV_H
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdatomic.h>
#include "sqlite3.h"
#include "multiwriter_meta.h"

struct mw_db; struct mw_lane;

#define STRIPES 64
#define SEN CRDT_COL_SENTINEL
#define OV_CHG INT64_MIN                 // in an overlay cell: written by this commit (its db_version is the commit's epoch, not known yet)
#define EXT_VERSION 0x4f                // the format of the extension of a commit record (bumped when it changes)
#define F_DROP 1                         // the non-sentinel cells of the row are removed
#define F_ZERO 2                         // the non-sentinel cells get version 0 and the db_version of the commit

typedef struct { uint32_t tbl; uint8_t *pk; uint32_t pklen; uint32_t n; uint32_t bloblen; uint8_t *blob; int64_t dv; } fitem;       // a row to write to the file: its key and its cells already packed (n of them; none: the row goes), dv the largest db_version among them
// the rows of a flush: the items, and the bytes they point to (keys and cells) in blocks of their own (a flush holds hundreds of thousands of rows: no allocation per row)
typedef struct { fitem *v; int n, cap; uint8_t **blocks; int nblocks, capblocks; size_t used, blockcap, blockmin; uint8_t *tmp; size_t tmpcap; } fbatch;

// Entries come from per-stripe free lists in size classes of 32 bytes (a malloc and a free for every row of a flush that writes millions of them a second was a tenth of the flusher's time);
// a list is used under the stripe's lock. Memory taken for the lists goes back when the store is freed.
#define ESLAB_CLASSES 20
#define ESLAB_BYTES (256u << 10)
typedef struct mentry {
    struct mentry *next;
    uint64_t h, ver, dseq;                     // ver: epoch of the last change; dseq: the stripe's sequence number of its last change (it is dirty while that is above the stripe's flushed_seq)
    uint32_t tbl, pklen, n, bloblen, cap; bool inl; uint8_t cls;     // the cells are kept packed (as a row of a run: ~1 byte a cell, not 32); inl: they are in the same allocation as the entry (after the key); cap: room for them
    uint8_t *blob;
    uint8_t pk[];
} mentry;

void mw_meta_trim (mw_meta *m, int stripe_no);                       // drops clean entries of a stripe down to its share of the cache (the flusher calls it)

// A stripe: the table of its entries, and the changes waiting for the flusher (pend: one item for every change, the key and the whole new state of the row already packed, in the order
// they happened; a flush takes the whole buffer in a moment and the entries are never visited again for it). An entry is dirty while its dseq is above flushed_seq.
typedef struct { pthread_mutex_t mu; mentry **b; size_t nb, n; fbatch pend; uint64_t seq, flushed_seq; void *efree[ESLAB_CLASSES]; uint8_t **slabs; int nslabs, capslabs; size_t bytes; size_t hand; uint64_t gen; uint32_t backoff; } stripe;
typedef struct { fbatch b; uint64_t seq; } dpend;

static inline void *stripe_alloc (stripe *s, size_t sz, uint8_t *cls) {
    size_t c = (sz + 31) / 32;
    if (c == 0 || c >= ESLAB_CLASSES) { *cls = 0; return malloc(sz); }
    *cls = (uint8_t)c;
    void *p = s->efree[c];
    if (p) { s->efree[c] = *(void **)p; return p; }
    size_t bs = c * 32, per = ESLAB_BYTES / bs;
    uint8_t *slab = malloc(per * bs); if (!slab) return NULL;
    if (s->nslabs == s->capslabs) { int nc = s->capslabs ? s->capslabs * 2 : 8; uint8_t **ns = realloc(s->slabs, (size_t)nc * sizeof *ns); if (!ns) { free(slab); return NULL; } s->slabs = ns; s->capslabs = nc; }
    s->slabs[s->nslabs++] = slab;
    for (size_t i = 1; i < per; i++) { void *q = slab + i * bs; *(void **)q = s->efree[c]; s->efree[c] = q; }
    return slab;
}
static inline void entry_free (stripe *s, mentry *e) {
    if (!e->inl) free(e->blob);
    if (!e->cls) { free(e); return; }
    *(void **)e = s->efree[e->cls]; s->efree[e->cls] = e;
}                        // a stripe's pending changes taken by a flush, and the sequence number they reach

struct mw_meta {
    struct mw_db *db;
    uint64_t *bloom;                                             // (one process) the filter of the keys that were ever deleted; in shared mode it is in the shared header
    bool shared;                                                 // multi-process shared mode: the state lives in the shared index and the log, not in this table
    stripe st[STRIPES];
    size_t cap_bytes;                                           // the cache budget before clean entries are dropped (URI mw_meta_cache_mb, default 64)
    pthread_mutex_t site_mu; uint8_t (*sites)[16]; uint32_t nsites, capsites;     // ord -> site id (0 = this database)
    int64_t origin; _Atomic uint64_t hwm; uint64_t new_hwm;                         // db_version = epoch + origin; the largest db_version in the file tables
    _Atomic uint64_t flushed;                                    // db_version up to which the cells are in the file
    _Atomic uint64_t hits, misses, rows, bytes;
    _Atomic int quiescing;
    _Atomic uint64_t ndirty;                                     // rows changed since the last flush
    // the file tables (multiwriter_metafile.c)
    char *uri; bool attached; _Atomic bool tables_ok, ready;
    pthread_mutex_t file_mu;                                     // ready / flush / writer connection
    sqlite3 *wr;                                                 // the flusher's connection
    bool swept; uint64_t sweep_ns; int fanout; uint64_t part_rows;                                                                          // merging: runs of a level that make a merge, rows of a part
    sqlite3 *mrd, *mwr; struct mw_rstore *rsx; _Atomic bool schema_seen;
    pthread_t mth; bool mth_running, mth_stop, mkick; pthread_mutex_t mth_mu; pthread_cond_t mth_cv;            // the thread that merges the runs (one process)                           // the others of a parallel flush, and how many ranges a big flush is cut in
    #define MW_RDN 4
    sqlite3 *rd[MW_RDN]; sqlite3_stmt *rds[MW_RDN]; pthread_mutex_t rdmu[MW_RDN];
    pthread_t th; bool th_running; bool th_stop; bool kicked; bool kick_pending; pthread_mutex_t th_mu; pthread_cond_t th_cv;
    struct mw_purge { uint32_t tbl; uint64_t epoch; } *purge; int npurge, cappurge; pthread_mutex_t purge_mu;       // tables dropped since the last flush: the cells of the file older than the drop are dead, and deleted with the next batch
    _Atomic uint64_t last_flush_ns; uint32_t sites_flushed;
    _Atomic uint64_t n_flushes, flushed_cells, flush_ns, flush_retries;
};


uint64_t mw_meta_hash (uint32_t tbl, const void *pk, size_t pklen);
bool     mw_meta_bloom_maybe (mw_meta *m, uint32_t tbl, const void *pk, size_t pklen);
void     mw_meta_bloom_add (mw_meta *m, uint32_t tbl, const void *pk, size_t pklen);
int      mw_metafile_load_many (mw_meta *m, int n, const uint32_t *tbl, const uint8_t *const *pk, const size_t *pklen, mw_mcell **cells, int *ncells);   // one read transaction for all
int      mw_metafile_load_tombstones (mw_meta *m);                              // fills the filter from the file's causal-length entries
int      mw_metafile_load (mw_meta *m, uint32_t tbl, const void *pk, size_t pklen, mw_mcell **cells, int *n);       // the row's cells from the file tables (n = 0: none / no tables)
void     mw_meta_site_install (mw_meta *m, uint32_t ord, const uint8_t id[16]);
void     mw_metafile_free (mw_meta *m);
void     mw_meta_reset (mw_meta *m);                                            // forget the memory table (see multiwriter_metafile.c)
int      mw_metafile_load_state (mw_meta *m, uint64_t *F, uint64_t *hwm, uint32_t *sites_flushed, bool *have_own, uint8_t own[16]);

// ---- the cells of a row, packed (the extension of a commit and the rows of the runs) ----
// A cell is five numbers, (column, version, db_version, site, sequence). The first cell of a row is written whole; each of the others as a control byte that says which numbers repeat (the column
// is the previous one + 1, the version, the db_version and the site are the previous ones, the sequence is the previous one + 1) followed by the numbers that do not. A row inserted by one commit is
// then 7 bytes for its first cell and one for every other (it was 7 for each: the log record of a commit of 100 rows lost 7 KB and a run of the file 3 times what it needed).
static inline size_t cz_var (uint8_t *p, uint64_t v) { size_t n = 0; while (v >= 0x80) { p[n++] = (uint8_t)(v | 0x80); v >>= 7; } p[n++] = (uint8_t)v; return n; }
static inline int cz_rvar (const uint8_t **p, const uint8_t *end, uint64_t *v) {
    uint64_t r = 0; int sh = 0;
    while (*p < end && sh < 64) { uint8_t b = *(*p)++; r |= (uint64_t)(b & 0x7f) << sh; if (!(b & 0x80)) { *v = r; return 0; } sh += 7; }
    return -1;
}
#define CZ_MAX 52                                                        // the most one cell takes
static inline size_t cz_put (uint8_t *out, const uint64_t v[5], const uint64_t *prev) {
    size_t w = 0;
    if (!prev) { for (int i = 0; i < 5; i++) w += cz_var(out + w, v[i]); return w; }
    unsigned ctl = (v[0] == prev[0] + 1) | (unsigned)(v[1] == prev[1]) << 1 | (unsigned)(v[2] == prev[2]) << 2 | (unsigned)(v[3] == prev[3]) << 3 | (unsigned)(v[4] == prev[4] + 1) << 4;
    out[w++] = (uint8_t)ctl;
    for (int i = 0; i < 5; i++) if (!(ctl & (1u << i))) w += cz_var(out + w, v[i]);
    return w;
}
static inline int cz_get (const uint8_t **p, const uint8_t *end, uint64_t v[5], const uint64_t *prev) {
    if (!prev) { for (int i = 0; i < 5; i++) if (cz_rvar(p, end, &v[i])) return -1; return 0; }
    if (*p >= end) return -1;
    unsigned ctl = *(*p)++;
    if (ctl & ~0x1fu) return -1;
    for (int i = 0; i < 5; i++) {
        if (ctl & (1u << i)) v[i] = (i == 1 || i == 2 || i == 3) ? prev[i] : prev[i] + 1;
        else if (cz_rvar(p, end, &v[i])) return -1;
    }
    return 0;
}

#endif

void *mw_fbatch_alloc (fbatch *b, size_t n);
fitem *mw_fbatch_add_packed (fbatch *b, uint32_t tbl, const uint8_t *pk, uint32_t pklen, const uint8_t *blob, size_t len, int n, int64_t dv);   // the same with the cells packed already (mw_meta_pack_row)
// packs the cells of a row (a db_version of OV_CHG becomes `chg_epoch`, if `resolve`) into `out`, which has room for MW_PACK_MAX(n) bytes; *maxdv: the largest db_version of the cells
#define MW_PACK_MAX(n) (16 + (size_t)(n) * 48)
size_t mw_meta_pack_row (const mw_mcell *c, int n, bool resolve, uint64_t chg_epoch, uint8_t *out, int64_t *maxdv);
fitem *mw_fbatch_add_row (fbatch *b, uint32_t tbl, const uint8_t *pk, uint32_t pklen, const mw_mcell *c, int n);   // a new item: the key copied, the cells packed; NULL on memory failure
void   mw_fbatch_free (fbatch *b);

// ---- the shared-mode backend (multiwriter_mmeta.c) ----
#define MW_NBUCKETS ((1u << 21) - 1)
typedef struct { uint32_t tbl; const uint8_t *pk; uint32_t pklen; mw_mcell *c; int n; } mm_row;                 // pk points into the group's bytes
typedef struct { uint8_t *raw; size_t rawlen; mm_row *rows; int n; uint32_t bucket; uint64_t epoch; } mm_group;     // the newest state of a bucket
static inline uint32_t mw_bucket_of (uint32_t tbl, const void *pk, size_t pklen) { return 1 + (uint32_t)((mw_meta_hash(tbl, pk, pklen) >> 7) % MW_NBUCKETS); }
int      mm_head (mw_meta *m, uint32_t bucket, mm_group *g);                       // the head version of a bucket: its rows (n = 0, epoch = 0 when there is none)
void     mm_group_free (mm_group *g);
uint32_t mm_site_ord (mw_meta *m, const uint8_t id[16]);
bool     mm_site_id (mw_meta *m, uint32_t ord, uint8_t out[16]);
void     mm_site_install (mw_meta *m, uint32_t ord, const uint8_t id[16]);
void     mm_site_install_db (struct mw_db *db, uint32_t ord, const uint8_t id[16]);
int      mm_validate (struct mw_db *db, struct mw_lane *lane);
int      mm_install (struct mw_db *db, struct mw_lane *lane, uint64_t epoch, uint64_t ext_loc);
int      mm_replay (struct mw_db *db, uint64_t epoch, const uint8_t *ext, uint32_t len, uint64_t ext_loc);
int      mm_ready (mw_meta *m);
static inline int64_t mw_meta_origin (mw_meta *m);
int      mm_flush (mw_meta *m);
int      mm_collect (mw_meta *m, uint64_t F, uint64_t Fe, fbatch *out);        // the rows whose newest state is in (F, V]: what the next flush writes

// the offset between the epochs of this incarnation of the database and the db_versions of the cells
#include "multiwriter_internal.h"
static inline int64_t mw_meta_origin (mw_meta *m) { return m->shared ? (int64_t)atomic_load_explicit(&m->db->shm->dv_origin, memory_order_acquire) : m->origin; }

