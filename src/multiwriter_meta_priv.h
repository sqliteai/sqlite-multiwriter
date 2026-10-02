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
#define EXT_VERSION 0x4e                // the format of the extension of a commit record (bumped when it changes)
#define F_DROP 1                         // the non-sentinel cells of the row are removed
#define F_ZERO 2                         // the non-sentinel cells get version 0 and the db_version of the commit

typedef struct mentry {
    struct mentry *next, *dnext;
    uint64_t h, ver, fver, drop_ver;           // ver: epoch of the last change; fver: ver as the flush in progress collected it; drop_ver: epoch of the last DROP (the file must lose the old cells too)
    uint32_t tbl, pklen; int n, cap; bool in_dirty;
    mw_mcell *cells;
    uint8_t pk[];
} mentry;

typedef struct { pthread_mutex_t mu; mentry **b; size_t nb, n; mentry *dirty; size_t bytes; size_t hand; uint64_t gen; uint32_t backoff; } stripe;

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
    char *uri; bool attached, tables_ok; _Atomic bool ready;
    pthread_mutex_t file_mu;                                     // ready / flush / writer connection
    sqlite3 *wr;                                                 // the flusher's connection
    #define MW_RDN 4
    sqlite3 *rd[MW_RDN]; sqlite3_stmt *rds[MW_RDN]; pthread_mutex_t rdmu[MW_RDN];
    pthread_t th; bool th_running; bool th_stop; bool kicked; bool kick_pending; pthread_mutex_t th_mu; pthread_cond_t th_cv;
    struct mw_purge { uint32_t tbl; uint64_t epoch; } *purge; int npurge, cappurge; pthread_mutex_t purge_mu;       // tables dropped since the last flush: the cells of the file older than the drop are dead, and deleted with the next batch
    uint64_t last_flush_ns; uint32_t sites_flushed;
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
void     mw_metafile_load_state (mw_meta *m, uint64_t *F, uint64_t *hwm, uint32_t *sites_flushed, bool *have_own, uint8_t own[16]);
#endif

typedef struct { uint32_t tbl; uint8_t *pk; uint32_t pklen; bool drop; int n; mw_mcell *c; } fitem;       // a row to write to the file tables: its cells (drop: the file's other cells of the row go first)
// the rows of a flush: the items, and the bytes they point to (keys and cells) in blocks of their own (a flush holds hundreds of thousands of rows: no allocation per row)
typedef struct { fitem *v; int n, cap; uint8_t **blocks; int nblocks, capblocks; size_t used, blockcap; } fbatch;
void *mw_fbatch_alloc (fbatch *b, size_t n);
fitem *mw_fbatch_add (fbatch *b, uint32_t tbl, const uint8_t *pk, uint32_t pklen, bool drop, int ncells);   // a new item with its key copied and room for ncells cells (c[ ] to be filled); NULL on memory failure
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
