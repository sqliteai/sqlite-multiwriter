//
//  crdt_core.c
//
//  The algorithms of sqlite-sync on an abstract state. The reference is sqlite-sync's cloudsync.c (merge_insert, merge_did_cid_win, merge_delete,
//  merge_sentinel_only_insert and the local_mark_* functions); the differential tests in test/oracle_crdt.c compare the outcome with the real thing.
//
#include <string.h>
#include "crdt.h"

#define SEN CRDT_COL_SENTINEL

static int64_t bump (int64_t v) { return (v % 2 == 0) ? v + 1 : v + 2; }          // the next live version: odd, +1 from a tombstone, +2 from a live version

static int emit (crdt_wcell *out, int max, int n, uint32_t col, const crdt_cell *c) { if (n < max) { out[n].col = col; out[n].cell = *c; } return n + 1; }

// the cell of `col` written by a local change: version bumped, this commit's db_version and sequence, site 0
static crdt_cell local_cell (const crdt_ops *o, void *st, uint32_t tbl, const void *pk, size_t pklen, uint32_t col, int64_t dv, int64_t *seq) {
    crdt_cell c = { 0, dv, (*seq)++, 0 }, old;
    c.cv = o->get(st, tbl, pk, pklen, col, &old) ? bump(old.cv) : 1;
    return c;
}

int crdt_local_insert (const crdt_ops *o, void *st, uint32_t tbl, const void *pk, size_t pklen, const uint32_t *cols, int ncols, int64_t dv, int64_t *seq, crdt_wcell *out, int max) {
    int n = 0; crdt_cell c;
    bool known = o->row_known(st, tbl, pk, pklen);
    if (ncols == 0) {                                                  // a table with nothing but its key: the sentinel is the row
        c = local_cell(o, st, tbl, pk, pklen, SEN, dv, seq); o->put(st, tbl, pk, pklen, SEN, &c); n = emit(out, max, n, SEN, &c);
    } else if (known) {                                                // the key was seen before (deleted and inserted again): the causal length goes up
        crdt_cell old;
        if (o->get(st, tbl, pk, pklen, SEN, &old)) {
            c = (crdt_cell){ bump(old.cv), dv, (*seq)++, 0 }; o->put(st, tbl, pk, pklen, SEN, &c); n = emit(out, max, n, SEN, &c);
        }
    }
    for (int i = 0; i < ncols; i++) {
        uint32_t col = cols ? cols[i] : (uint32_t)i;
        c = local_cell(o, st, tbl, pk, pklen, col, dv, seq); o->put(st, tbl, pk, pklen, col, &c); n = emit(out, max, n, col, &c);
    }
    return n;
}

int crdt_local_update (const crdt_ops *o, void *st, uint32_t tbl, const void *pk, size_t pklen, const uint32_t *cols, int nc, int64_t dv, int64_t *seq, crdt_wcell *out, int max) {
    int n = 0;
    for (int i = 0; i < nc; i++) { crdt_cell c = local_cell(o, st, tbl, pk, pklen, cols[i], dv, seq); o->put(st, tbl, pk, pklen, cols[i], &c); n = emit(out, max, n, cols[i], &c); }
    return n;
}

int crdt_local_delete (const crdt_ops *o, void *st, uint32_t tbl, const void *pk, size_t pklen, int64_t dv, int64_t *seq, crdt_wcell *out, int max) {
    crdt_cell old, c = { 2, dv, (*seq)++, 0 };
    if (o->get(st, tbl, pk, pklen, SEN, &old)) c.cv = (old.cv % 2 == 0) ? old.cv + 2 : old.cv + 1;      // (sqlite-sync: same parity +2, other parity +1; 2 for a row that never had a sentinel)
    o->put(st, tbl, pk, pklen, SEN, &c);
    o->drop_cols(st, tbl, pk, pklen);
    return emit(out, max, 0, SEN, &c);
}

int crdt_local_rekey (const crdt_ops *o, void *st, uint32_t tbl, const void *oldpk, size_t oldlen, const void *newpk, size_t newlen, const uint32_t *cols, int ncols, int64_t dv, int64_t *seq, crdt_wcell *out, int max) {
    int n = crdt_local_delete(o, st, tbl, oldpk, oldlen, dv, seq, out, max);
    // the cells of the old key move to the new key, with version 1 and the new commit's db_version (sqlite-sync: local_update_move_meta)
    for (int i = 0; i < ncols; i++) {
        crdt_cell c = { 1, dv, (*seq)++, 0 };
        o->put(st, tbl, newpk, newlen, cols[i], &c); n = emit(out, max, n, cols[i], &c);
    }
    crdt_cell s = local_cell(o, st, tbl, newpk, newlen, SEN, dv, seq);
    o->put(st, tbl, newpk, newlen, SEN, &s);
    return emit(out, max, n, SEN, &s);
}

// ---- remote ------------------------------------------------------------------------------------------------------------------------------------------------------
int crdt_value_compare (const crdt_value *l, const crdt_value *r) {
    if (l == r) return 0;
    if (!l) return -1;
    if (!r) return 1;
    if (l->type != r->type) return (int)r->type - (int)l->type;
    switch (l->type) {
        case CRDT_INTEGER: return l->i < r->i ? -1 : l->i > r->i;
        case CRDT_FLOAT: return l->d < r->d ? -1 : l->d > r->d;
        case CRDT_NULL: return 0;
        case CRDT_TEXT: {
            // strcmp on NUL-terminated text: compare up to the first NUL of either
            const unsigned char *a = l->p, *b = r->p; size_t i = 0;
            if (!a && !b) return 0; if (!a) return -1; if (!b) return 1;
            for (;; i++) { unsigned char x = i < l->n ? a[i] : 0, y = i < r->n ? b[i] : 0; if (x != y) return x < y ? -1 : 1; if (!x) return 0; }
        }
        case CRDT_BLOB: {
            if (!l->p && !r->p) return 0; if (!l->p) return -1; if (!r->p) return 1;
            size_t m = l->n < r->n ? l->n : r->n; int c = memcmp(l->p, r->p, m);
            return c ? c : (int)((int64_t)l->n - (int64_t)r->n);
        }
    }
    return 0;
}

static crdt_cell winner (const crdt_change *c, int64_t stored_dv, uint32_t site) { return (crdt_cell){ c->cv, stored_dv, c->seq, site }; }

// does the incoming change beat the cell it meets? (sqlite-sync: merge_did_cid_win, called when the causal lengths are equal)
static int did_win (const crdt_ops *o, void *st, const crdt_change *c, bool merge_equal_values, bool *win) {
    crdt_cell loc;
    if (!o->get(st, c->tbl, c->pk, c->pklen, c->col, &loc)) { *win = true; return 0; }       // nothing there: it wins
    if (c->cv > loc.cv) { *win = true; return 0; }
    if (c->cv < loc.cv) { *win = false; return 0; }
    crdt_value lv; bool have = o->value(st, c->tbl, c->pk, c->pklen, c->col, &lv);               // equal versions: the values decide
    int r = crdt_value_compare(c->value, have ? &lv : NULL);
    if (!(r == 0 && merge_equal_values)) { *win = r > 0; return 0; }
    uint8_t lsite[16];
    if (!o->site_bytes(st, loc.site, lsite)) return -1;
    *win = memcmp(c->site, lsite, 16) > 0;                                                       // (equal values: the larger site id)
    return 0;
}

int crdt_merge (const crdt_ops *o, void *st, const crdt_change *c, int64_t stored_dv, bool merge_equal_values, crdt_actions *act) {
    memset(act, 0, sizeof *act);
    crdt_cell sc;
    int64_t local_cl = 0;
    if (o->get(st, c->tbl, c->pk, c->pklen, SEN, &sc)) local_cl = sc.cv;
    else if (o->row_known(st, c->tbl, c->pk, c->pklen)) local_cl = 1;                        // (cells but no sentinel: alive, causal length 1)
    uint32_t site = o->site_ord(st, c->site);
    if (c->cl < local_cl) return 0;                                                          // an older life of the row
    bool is_delete = c->cl % 2 == 0;
    if (is_delete) {
        if (local_cl == c->cl) return 0;
        crdt_cell w = winner(c, stored_dv, site);
        o->put(st, c->tbl, c->pk, c->pklen, SEN, &w); o->drop_cols(st, c->tbl, c->pk, c->pklen);          // (the winner clock first, then the other cells go: the max db_version is kept)
        act->kind[act->n++] = CRDT_ACT_DELETE_ROW;
        return 0;
    }
    bool sentinel_only = c->col == SEN;
    if (sentinel_only) {
        if (local_cl == c->cl) return 0;
        o->zero_cols(st, c->tbl, c->pk, c->pklen, stored_dv);
        crdt_cell w = winner(c, stored_dv, site); o->put(st, c->tbl, c->pk, c->pklen, SEN, &w);
        act->kind[act->n++] = CRDT_ACT_INSERT_ROW;
        return 0;
    }
    bool needs_resurrect = c->cl > local_cl && c->cl % 2 == 1;
    bool row_exists = local_cl != 0;
    if (needs_resurrect && (row_exists || c->cl > 1)) {
        o->zero_cols(st, c->tbl, c->pk, c->pklen, stored_dv);
        crdt_cell w = { c->cl, stored_dv, c->seq, site }; o->put(st, c->tbl, c->pk, c->pklen, SEN, &w);    // (the sentinel with the new causal length: the row is alive again)
        act->kind[act->n++] = CRDT_ACT_INSERT_ROW;
    }
    bool win = false;
    if (did_win(o, st, c, merge_equal_values, &win) != 0) return -1;
    if (!(needs_resurrect || !row_exists || win)) return 0;
    crdt_cell w = winner(c, stored_dv, site); o->put(st, c->tbl, c->pk, c->pklen, c->col, &w);
    act->kind[act->n++] = CRDT_ACT_WRITE_COL; act->col = c->col;
    return 0;
}
