//
//  multiwriter_catalog.h
//
//  The schema as the capture needs it: for every table b-tree, how its records are laid out (which record column is which column, which are the primary key) and what the
//  CRDT sees (the cells: the non-key columns, in order). Built from sqlite_schema as the transaction's snapshot has it: the rows are read from the pages, and SQLite itself
//  parses the CREATE TABLE statements (in a scratch in-memory database), so generated columns, WITHOUT ROWID, collations of the key, quoting and so on are never guessed.
//
#ifndef MW_CATALOG_H
#define MW_CATALOG_H

#include <stdbool.h>
#include <stdint.h>
#include <stdatomic.h>

#define MW_CAT_MAXPK 16

typedef struct {
    uint32_t root;
    char *name;
    bool tracked;                       // false: its CREATE TABLE could not be understood (an unknown collation, say): the rows are not captured
    bool without_rowid;                 // the b-tree is an index b-tree holding the whole row (key = the primary key columns)
    bool alias_pk;                      // INTEGER PRIMARY KEY: the key is the rowid (its record column holds NULL)
    bool has_pk;                        // an explicit primary key; without one the identity of a row is its rowid and the table cannot be synchronised
    int nrec;                           // columns stored in a record
    int npk; int pk_rec[MW_CAT_MAXPK];  // the record column of each key column, in key order
    bool synced;                        // the CRDT keeps cells for it: tracked, has an explicit primary key, not an internal table (mw_*, sqlite_*), no id collisions
    uint32_t tid;                       // stable id of the table: hash of its name (the same on every peer, nothing to persist)
    uint32_t *cell_id;                  // stable id of each cell: hash of the column name
    int ncells; int *cell_rec; char **cell_name;     // the cells: non-key, non-generated columns in column order; cell i lives in record column cell_rec[i]
} mw_tab;

typedef struct mw_cat { mw_tab *tabs; int n; uint32_t cookie; _Atomic int refs; } mw_cat;

typedef struct mw_lane mw_lane;
mw_cat *mw_cat_build (mw_lane *lane);                         // the schema at the lane's snapshot; NULL on memory failure
mw_cat *mw_cat_ref (mw_cat *c);                               // a catalog is shared by the lanes: reference counted
void mw_cat_free (mw_cat *c);                                 // drops a reference; the last one frees it
const mw_tab *mw_cat_by_root (const mw_cat *c, uint32_t root);
uint32_t mw_name_id (const char *name);                        // the id of a table or column name (case-insensitive FNV-1a, never the sentinel)
const mw_tab *mw_cat_by_name (const mw_cat *c, const char *name);

#endif
