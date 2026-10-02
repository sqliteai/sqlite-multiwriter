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
void     mw_meta_free (mw_meta *m);

// ---- the transaction's view: an overlay over the store, with the crdt_ops the CRDT core runs on ----
mw_ovl *mw_ovl_new (mw_meta *m);
void    mw_ovl_free (mw_ovl *o);
void    mw_ovl_clear (mw_ovl *o);
const crdt_ops *mw_ovl_ops (void);                                          // the state pointer is the mw_ovl
typedef bool (*mw_value_fn) (void *arg, uint32_t tbl, const void *pk, size_t pklen, uint32_t col, crdt_value *out);
void    mw_ovl_set_value_fn (mw_ovl *o, mw_value_fn fn, void *arg);         // the base-table value of a cell (the merge needs it when the versions tie)
bool    mw_ovl_empty (const mw_ovl *o);
void    mw_ovl_purge (mw_ovl *o, uint32_t tbl);                              // the table is gone: all its cells go (DROP TABLE)
int     mw_ovl_encode (mw_ovl *o, uint8_t **ext, uint32_t *len);            // the delta as a record extension (malloc'd; len 0 and *ext NULL if there is nothing)

// ---- applying a delta ----
int mw_meta_apply (mw_meta *m, mw_ovl *o, uint64_t epoch);                  // the commit `epoch` made by this process
int mw_meta_replay (mw_meta *m, uint64_t epoch, const uint8_t *ext, uint32_t len);   // a commit seen in the log (recovery, another process): the same effect from the bytes

// ---- reading ----
int mw_meta_row (mw_meta *m, uint32_t tbl, const void *pk, size_t pklen, mw_mcell **cells, int *n);   // a copy of the row's cells (free() it); n = 0 for an unknown row
uint32_t mw_meta_site_ord (mw_meta *m, const uint8_t id[16]);
bool     mw_meta_site_id (mw_meta *m, uint32_t ord, uint8_t out[16]);
void     mw_meta_stats (mw_meta *m, uint64_t *rows, uint64_t *bytes, uint64_t *hits, uint64_t *misses);

// ---- the file tables and the flusher (multiwriter_metafile.c) ----
int      mw_meta_attach (mw_meta *m, const char *path, int mode, int mpmode);      // where the database is (so the store can open its own connections to it)
int      mw_meta_ready (mw_meta *m);                                              // bring in the file's state and replay the extensions the log held at recovery (idempotent)
int      mw_meta_flush (mw_meta *m);                                              // write what changed since the last flush to the file tables, in one commit
void     mw_meta_kick (mw_meta *m);                                               // ask the flusher thread to flush soon
uint64_t mw_meta_safe_epoch (mw_meta *m);                                         // the log may be compacted up to here without losing metadata (UINT64_MAX: no limit)
void     mw_meta_quiesce (mw_meta *m);                                            // stop the flusher, flush, close the store's connections
int      mw_meta_schema (sqlite3 *conn);                                          // create the file tables if they are missing
uint64_t mw_meta_flushed (mw_meta *m);
uint64_t mw_meta_dirty (mw_meta *m);
uint64_t mw_meta_epoch (mw_meta *m);                                              // the last commit made visible                                              // rows changed since the last flush

#endif
