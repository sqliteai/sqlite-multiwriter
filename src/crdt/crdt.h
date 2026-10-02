//
//  crdt.h
//
//  The CRDT of sqlite-multiwriter: the algorithms of sqlite-sync (causal-length set for rows, last-writer-wins registers for cells), on plain C values and an abstract state.
//  Nothing in here knows about SQLite: values are `crdt_value`, the state is behind the `crdt_state` callbacks, so the same code runs on the shared metadata store, on a
//  test double, and under the differential tests against sqlite-sync itself.
//
#ifndef MW_CRDT_H
#define MW_CRDT_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define CRDT_SENTINEL "__[RIP]__"           // the column name of the row's own entry (its causal length lives there), as in sqlite-sync

typedef enum { CRDT_NULL = 5, CRDT_INTEGER = 1, CRDT_FLOAT = 2, CRDT_TEXT = 3, CRDT_BLOB = 4 } crdt_type;      // (the codes SQLite and sqlite-sync use)
typedef struct { crdt_type type; int64_t i; double d; const void *p; size_t n; } crdt_value;                    // TEXT/BLOB: p, n (not owned)

// ---- primary keys: the byte format of sqlite-sync's cloudsync_pk_encode (one count byte, then per value a type byte with the length of its length/integer field) ----
size_t crdt_pk_encode (const crdt_value *v, int n, uint8_t *out, size_t cap);        // bytes written, or the bytes needed if out == NULL / too small (> cap); 0 on error (more than 255 values)
int    crdt_pk_decode (const uint8_t *buf, size_t len, crdt_value *out, int max);    // number of values (<= max), -1 if malformed; TEXT/BLOB values point into buf


// ---- the state ------------------------------------------------------------------------------------------------------------------------------------------------
// Every row of every table has cells: one per non-key column (col = its index among the non-key columns) and the sentinel (col = CRDT_COL_SENTINEL) that carries the
// row's causal length when the row has ever been deleted. A cell is (col_version, db_version, site, seq); site 0 is this database.
#define CRDT_COL_SENTINEL 0xFFFFFFFFu
typedef struct { int64_t cv, dv, seq; uint32_t site; } crdt_cell;

typedef struct crdt_ops {
    bool (*get) (void *st, uint32_t tbl, const void *pk, size_t pklen, uint32_t col, crdt_cell *out);
    void (*put) (void *st, uint32_t tbl, const void *pk, size_t pklen, uint32_t col, const crdt_cell *c);
    void (*drop_cols) (void *st, uint32_t tbl, const void *pk, size_t pklen);                       // every cell of the row except the sentinel
    void (*zero_cols) (void *st, uint32_t tbl, const void *pk, size_t pklen, int64_t dv);           // col_version = 0 and db_version = dv for every cell except the sentinel
    bool (*row_known) (void *st, uint32_t tbl, const void *pk, size_t pklen);                       // any cell at all, the sentinel included
    // the merge needs the local value of a cell when the versions tie, and the site ids as bytes
    bool (*value) (void *st, uint32_t tbl, const void *pk, size_t pklen, uint32_t col, crdt_value *out);    // false: the base table has no such value
    uint32_t (*site_ord) (void *st, const uint8_t site[16]);                                        // the ordinal of a site id (allocated on first sight)
    bool (*site_bytes) (void *st, uint32_t ord, uint8_t out[16]);
} crdt_ops;

// ---- local changes: what a commit did to one row -----------------------------------------------------------------------------------------------------------------
// Every function updates the state through ops and appends to `out` the cells that were written (for the export and for the write-ahead record). `seq` is the running
// sequence number of the commit (incremented per cell written), dv its db_version. Versions follow sqlite-sync: a live cell is odd, +2 per local update, +1 when it was even.
typedef struct { uint32_t col; crdt_cell cell; } crdt_wcell;
int crdt_local_insert (const crdt_ops *o, void *st, uint32_t tbl, const void *pk, size_t pklen, int ncols, int64_t dv, int64_t *seq, crdt_wcell *out, int max);
int crdt_local_update (const crdt_ops *o, void *st, uint32_t tbl, const void *pk, size_t pklen, const uint32_t *cols, int n, int64_t dv, int64_t *seq, crdt_wcell *out, int max);
int crdt_local_delete (const crdt_ops *o, void *st, uint32_t tbl, const void *pk, size_t pklen, int64_t dv, int64_t *seq, crdt_wcell *out, int max);
// the primary key changed: the old row is deleted, its cells move to the new key with version 1, the new row gets its sentinel (docs of sqlite-sync: PriKey.md)
int crdt_local_rekey (const crdt_ops *o, void *st, uint32_t tbl, const void *oldpk, size_t oldlen, const void *newpk, size_t newlen, const uint32_t *cols, int ncols, int64_t dv, int64_t *seq, crdt_wcell *out, int max);

// ---- remote changes ------------------------------------------------------------------------------------------------------------------------------------------------
typedef struct {
    uint32_t tbl; const void *pk; size_t pklen; uint32_t col;           // col = CRDT_COL_SENTINEL for the row's own entry (a delete, or an insert without columns)
    const crdt_value *value; int64_t cv, cl, dv, seq; uint8_t site[16];
} crdt_change;
typedef enum { CRDT_ACT_NONE = 0, CRDT_ACT_INSERT_ROW, CRDT_ACT_DELETE_ROW, CRDT_ACT_WRITE_COL } crdt_act_kind;       // INSERT_ROW: the row must exist (only its key)
typedef struct { int n; crdt_act_kind kind[2]; uint32_t col; } crdt_actions;       // up to two actions, in order: INSERT_ROW then WRITE_COL, or DELETE_ROW, or WRITE_COL
// Decides a remote change with the causal-length / column-version rules and the value / site tie-breaks of sqlite-sync, records the winner in the state (stored db_version =
// stored_dv) and says what has to be done to the base table. `merge_equal_values`: when the values are equal too, the larger site id wins (sqlite-sync's setting).
int crdt_merge (const crdt_ops *o, void *st, const crdt_change *c, int64_t stored_dv, bool merge_equal_values, crdt_actions *act);
int crdt_value_compare (const crdt_value *incoming, const crdt_value *local);      // sqlite-sync's ordering: > 0 means the incoming value wins a tie of versions

#endif
