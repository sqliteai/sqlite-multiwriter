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

typedef struct { int type; int64_t i; double d; uint8_t *p; int n; } mw_val;     // a value: type as sqlite3_column_type (SQLITE_NULL, INTEGER, FLOAT, TEXT, BLOB)

typedef struct {
    uint32_t root;
    char *name;
    bool ok;                            // false: its CREATE TABLE could not be understood, or it is an internal table (sqlite_*): its rows are not replayed
    bool without_rowid;                 // the b-tree is an index b-tree holding the whole row
    bool alias_pk;                      // INTEGER PRIMARY KEY: the key is the rowid (its record column holds NULL)
    int nrec;                           // columns stored in a record
    int alias_rec;                      // the record column of the rowid alias (-1: none)
    char **rec_name;                    // the name of the column stored in each record column; NULL for a generated one (derived: not written, not compared)
    mw_val *dflt;                       // the default of each record column (what a row written before an ALTER TABLE ADD COLUMN has in the columns it lacks); NULL: all NULL
    int nfk; char **fk_parent, **fk_to;  // foreign keys of this table: the parent table, and the parent column (NULL: its primary key)
    bool is_parent;                     // some foreign key refers to this table
    uint64_t refmask;                   // the writable columns (as in changed_mask of the rebase) that foreign keys refer to
    int rank;                           // 0 for a table without foreign keys; one more than the highest rank of its parents: parents are inserted before children, children deleted before parents
    char **pk_name; int npk;
} mw_tab;

typedef struct mw_cat {
    mw_tab *tabs; int n;
    uint32_t cookie;                    // the schema cookie it was built at
    bool rebasable;                     // a database whose transactions can be replayed: no trigger, virtual table or self-referencing or circular foreign key
    bool has_fk;                        // some table has a foreign key (the replay then runs with them enforced, if the application's connection does)
    int maxrank;
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
