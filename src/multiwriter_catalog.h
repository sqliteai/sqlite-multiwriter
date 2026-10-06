//
//  multiwriter_catalog.h
//
//  The schema as the logical replay (the rebase, multiwriter_rebase.c) needs it: for every table b-tree, how its records are laid out (which record column is which column) and whether
//  the database can be replayed at all. Built from sqlite_schema as the transaction's snapshot has it: the rows are read from the pages, and SQLite itself parses the CREATE TABLE
//  statements (in a scratch in-memory database), so generated columns, quoting and so on are never guessed.
//
#ifndef MW_CATALOG_H
#define MW_CATALOG_H

#include <stdbool.h>
#include <stdint.h>
#include <stdatomic.h>

typedef struct {
    uint32_t root;
    char *name;
    bool ok;                            // false: its CREATE TABLE could not be understood, or it is an internal table (sqlite_*): its rows are not replayed
    bool without_rowid;                 // the b-tree is an index b-tree holding the whole row
    bool alias_pk;                      // INTEGER PRIMARY KEY: the key is the rowid (its record column holds NULL)
    int nrec;                           // columns stored in a record
    int alias_rec;                      // the record column of the rowid alias (-1: none)
    char **rec_name;                    // the name of the column stored in each record column; NULL for a generated one (derived: not written, not compared)
} mw_tab;

typedef struct mw_cat {
    mw_tab *tabs; int n;
    uint32_t cookie;                    // the schema cookie it was built at
    bool rebasable;                     // a database whose transactions can be replayed: no WITHOUT ROWID table, trigger, foreign key or virtual table
    const char *why;                    // if not: the reason
    _Atomic int refs;
} mw_cat;

typedef struct mw_lane mw_lane;
mw_cat *mw_cat_build (mw_lane *lane);                         // the schema at the lane's snapshot; NULL on memory failure
mw_cat *mw_cat_ref (mw_cat *c);                               // a catalog is shared by the lanes: reference counted
void mw_cat_free (mw_cat *c);                                 // drops a reference; the last one frees it
const mw_tab *mw_cat_by_root (const mw_cat *c, uint32_t root);
const mw_tab *mw_cat_by_name (const mw_cat *c, const char *name);

#endif
