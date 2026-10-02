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

typedef struct { pthread_mutex_t mu; mentry **b; size_t nb, n; mentry *dirty; size_t bytes; size_t hand; uint64_t gen; } stripe;

struct mw_meta {
    struct mw_db *db;
    stripe st[STRIPES];
    size_t cap_rows;                                            // the cache budget (rows) before clean entries are dropped
    pthread_mutex_t site_mu; uint8_t (*sites)[16]; uint32_t nsites, capsites;     // ord -> site id (0 = this database)
    _Atomic uint64_t flushed;                                    // epoch up to which the cells are in the file
    _Atomic uint64_t hits, misses, rows, bytes;
    _Atomic int quiescing;
    _Atomic uint64_t ndirty;                                     // rows changed since the last flush
    // the file tables (multiwriter_metafile.c)
    char *uri; bool attached, tables_ok; _Atomic bool ready;
    pthread_mutex_t file_mu;                                     // ready / flush / writer connection
    sqlite3 *wr;                                                 // the flusher's connection
    #define MW_RDN 4
    sqlite3 *rd[MW_RDN]; sqlite3_stmt *rds[MW_RDN]; pthread_mutex_t rdmu[MW_RDN];
    pthread_t th; bool th_running; bool th_stop; bool kicked; pthread_mutex_t th_mu; pthread_cond_t th_cv;
    struct mw_purge { uint32_t tbl; uint64_t epoch; } *purge; int npurge, cappurge; pthread_mutex_t purge_mu;       // tables dropped since the last flush: the cells of the file older than the drop are dead, and deleted with the next batch
    uint64_t last_flush_ns; uint32_t sites_flushed;
    _Atomic uint64_t n_flushes, flushed_cells, flush_ns, flush_retries;
};


uint64_t mw_meta_hash (uint32_t tbl, const void *pk, size_t pklen);
int      mw_metafile_load (mw_meta *m, uint32_t tbl, const void *pk, size_t pklen, mw_mcell **cells, int *n);       // the row's cells from the file tables (n = 0: none / no tables)
void     mw_meta_site_install (mw_meta *m, uint32_t ord, const uint8_t id[16]);
void     mw_metafile_free (mw_meta *m);
#endif
