//
//  multiwriter_meta.h
//
//  The metadata store of the CRDT (docs/design.md): per row the cells (column version, db_version, site, seq), in memory in front of the tables of the database file.
//  Every commit carries its metadata delta in the extension of its log record (rows touched, cells written); applying a delta is the single way the state changes, whether
//  the commit is ours (at publication), another process's (at catch-up) or the log's (at recovery), so all of them agree.
//
#ifndef MW_META_H
#define MW_META_H

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>
#include "sqlite3.h"
#include "crdt/crdt.h"

typedef struct mw_meta mw_meta;
typedef struct mw_ovl mw_ovl;
struct mw_db;

typedef struct { int64_t cv, dv; uint32_t col, site, seq; } mw_mcell;       // site 0 = this database

mw_meta *mw_meta_new (struct mw_db *db);
void     mw_meta_set_cache_mb (mw_meta *m, int mb);                                 // the memory the row cache may hold (rows that are not waiting for a flush)
void     mw_meta_free (mw_meta *m);

// ---- the transaction's view: an overlay over the store, with the crdt_ops the CRDT core runs on ----
mw_ovl *mw_ovl_new (mw_meta *m);
void    mw_ovl_free (mw_ovl *o);
void    mw_ovl_clear (mw_ovl *o);
const crdt_ops *mw_ovl_ops (void);                                          // the state pointer is the mw_ovl
typedef bool (*mw_value_fn) (void *arg, uint32_t tbl, const void *pk, size_t pklen, uint32_t col, crdt_value *out);
void    mw_ovl_set_value_fn (mw_ovl *o, mw_value_fn fn, void *arg);         // the base-table value of a cell (the merge needs it when the versions tie)
bool    mw_ovl_empty (const mw_ovl *o);
typedef struct { uint32_t tbl; const void *pk; size_t pklen; bool is_new; } mw_want;      // a row the transaction is going to need: is_new: it did not exist before (an insert)
void    mw_ovl_prefetch (mw_ovl *o, const mw_want *w, int n);
void    mw_ovl_hint_new (mw_ovl *o, bool on);
void    mw_ovl_bloom_update (mw_ovl *o);                                         // after a commit: its rows with a causal-length entry go into the filter of deleted rows                                  // the rows the CRDT asks for next are inserts (a key that was never deleted needs no look in the file)                  // loads them together (one read of the file for all the rows that are not in memory)
void    mw_ovl_purge (mw_ovl *o, uint32_t tbl);                              // the table is gone: all its cells go (DROP TABLE)
int     mw_ovl_encode (mw_ovl *o, uint8_t **ext, uint32_t *len);            // the delta as a record extension (malloc'd; len 0 and *ext NULL if there is nothing)

// ---- applying a delta ----
int mw_meta_apply (mw_meta *m, mw_ovl *o, uint64_t epoch);                  // the commit `epoch` made by this process
int mw_meta_replay (mw_meta *m, uint64_t epoch, const uint8_t *ext, uint32_t len);   // a commit seen in the log (recovery, another process): the same effect from the bytes

// ---- the extension, as a stream ----
typedef int  (*mw_ext_row_fn) (void *arg, uint32_t bucket, uint32_t tbl, const uint8_t *pk, size_t pklen, const mw_mcell *cells, int n);   // non-zero stops the walk
typedef void (*mw_ext_purge_fn) (void *arg, uint32_t tbl, uint64_t epoch);
typedef void (*mw_ext_site_fn) (void *arg, uint32_t ord, const uint8_t id[16]);
typedef int (*mw_ext_group_fn) (void *arg, uint32_t bucket, uint32_t off);              // off: where the group starts in the extension
int mw_ext_groups (const uint8_t *ext, uint32_t len, mw_ext_group_fn cb, void *arg);
int mw_ext_purges (const uint8_t *ext, uint32_t len, mw_ext_purge_fn cb, uint64_t epoch, void *arg);
int mw_ovl_groups (const mw_ovl *o, const uint32_t **bucket, const uint32_t **off, const uint64_t **seen);   // after mw_ovl_encode in shared mode: the groups (count returned)
int mw_ext_walk (const uint8_t *ext, uint32_t len, uint64_t epoch, mw_ext_row_fn row_cb, mw_ext_purge_fn purge_cb, mw_ext_site_fn site_cb, void *arg);

// ---- reading ----
int mw_meta_row (mw_meta *m, uint32_t tbl, const void *pk, size_t pklen, mw_mcell **cells, int *n);   // a copy of the row's cells (free() it); n = 0 for an unknown row
uint32_t mw_meta_site_ord (mw_meta *m, const uint8_t id[16]);
bool     mw_meta_site_id (mw_meta *m, uint32_t ord, uint8_t out[16]);
void     mw_meta_stats (mw_meta *m, uint64_t *rows, uint64_t *bytes, uint64_t *hits, uint64_t *misses);
void     mw_meta_flush_stats (mw_meta *m, uint64_t *flushes, uint64_t *cells, uint64_t *ns, uint64_t *retries);

// ---- the file tables and the flusher (multiwriter_metafile.c) ----
int      mw_meta_attach (mw_meta *m, const char *path, int mode, int mpmode);      // where the database is (so the store can open its own connections to it)
int      mw_meta_ready (mw_meta *m);                                              // bring in the file's state and replay the extensions the log held at recovery (idempotent)
int      mw_meta_flush (mw_meta *m);                                              // write what changed since the last flush to the file tables, in one commit
void     mw_meta_kick (mw_meta *m);                                               // ask the flusher thread to flush soon
uint64_t mw_meta_safe_epoch (mw_meta *m);                                         // the log may be compacted up to here without losing metadata (UINT64_MAX: no limit)
void     mw_meta_quiesce (mw_meta *m);                                            // stop the flusher, flush, close the store's connections
int      mw_meta_schema (sqlite3 *conn);
int      mw_meta_export_index (sqlite3 *conn);                                     // the index on db_version, for the export (created by the first one)                                          // create the file tables if they are missing
uint64_t mw_meta_flushed (mw_meta *m);
uint64_t mw_meta_dirty (mw_meta *m);
uint64_t mw_meta_dirty_limit (mw_meta *m);
void mw_meta_run_stats (mw_meta *m, uint64_t out[9]);       // lookups, runs probed, skipped by the filter, blocks read, cache hits, merges, rows merged, runs written, runs now
typedef struct { uint32_t tid; uint32_t pko; uint32_t pkl; mw_mcell c; int64_t cl; } mw_xcell;      // a cell to export: its table, where its key is in the pool, the cell, the causal length of its row
int mw_metafile_export (mw_meta *m, sqlite3 *c, int64_t since, int64_t upto, mw_xcell **out, size_t *n, uint8_t **pkpool);   // (a transaction is open on c; free out and the pool)
int mw_meta_register_views (sqlite3 *c, mw_meta *m);                                      // the read-only virtual table mw_cells (the cells of the runs, unpacked)
uint8_t *mw_meta_row_pack (const mw_mcell *c, int n, size_t *len);
bool mw_meta_row_cells (const void *blob, size_t len, mw_mcell **c, int *n);   // the cells of a packed row of a block (caller frees)
uint64_t mw_meta_epoch (mw_meta *m);
uint64_t mw_meta_dv (mw_meta *m, uint64_t epoch);                               // the db_version of an epoch of this incarnation                                              // the last commit made visible                                              // rows changed since the last flush

#endif
