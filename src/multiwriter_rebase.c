//
//  multiwriter_rebase.c
//
//  The rebase (docs/design.md, "The rebase"). A commit whose pages conflict with newer commits only on pages that it wrote is not necessarily a logical conflict: two writers may change different
//  rows of one page. With the URI parameter mw_rebase=1 such a commit is not refused: its pages are discarded and its *row changes* (multiwriter_rowdiff.c: from the pages and the snapshot's
//  pages, the rows inserted, changed or deleted) are replayed with ordinary SQL on a helper connection at the latest snapshot, and stock SQLite builds every page again (splits, allocation,
//  indexes, freelist, record encoding). The original SQL is never run again (no side effects twice).
//
//  What is checked, so that the result is a state that a serial execution of the two commits could have produced:
//    - the schema is the one the transaction ran with (the cookie);
//    - every row that the transaction changed or deleted is, at the latest snapshot, exactly what it was at the transaction's snapshot (the whole row, not the cells: the transaction may have read
//      the cells that it did not write). A row that somebody else changed or deleted since is a true conflict: the transaction is refused (SQLITE_BUSY_SNAPSHOT) and the application retries it;
//    - a row that the transaction inserted is not there yet; the constraints (UNIQUE, CHECK, NOT NULL, the key) are evaluated by SQLite on the rows as they are now: a violation is a conflict.
//  What is checked another way: the rows that the transaction read and did not change. A page that it only read is validated as before (a change to it refuses the commit), but a row that it read from a
//  page that it also wrote cannot be told from a row it changed by the pages. The statements say it (mw_lane_reads_unchanged): a transaction that ran a SELECT, or a statement that is not a point statement
//  (by its bytecode), is not rebased (write skew, test mw_rebaseskew).
//  Not rebased (refused as before): DDL, a database with a trigger, a foreign key, a virtual table or a WITHOUT ROWID table, a write to an internal table (sqlite_sequence), a page of unknown owner,
//  a record that does not decode, a row with fewer columns than the table (ALTER TABLE ADD COLUMN), a database that is not UTF-8.
//
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sched.h>
#include "multiwriter_internal.h"

#define MW_REBASE_MAX_ATTEMPTS 200
#define MW_REBASE_GATE_AFTER   1      // lost attempts before the publication gate is closed for the next one: the first attempt runs without it (it is cheap and often wins); the one that follows a loss runs against a frozen state (the commits that were assigned an epoch are in the pages before they are visible: against a moving page a replay loses 97% of the time, measured; with the gate after the first loss 5%, and 1.2-1.4x the throughput on one page)

typedef struct { uint64_t mask; sqlite3_stmt *st; } ucache;
typedef struct { sqlite3_stmt *sel, *ins, *del; ucache upd[8]; int nupd, next_upd; bool ready; } tstmt;      // upd: UPDATE statements by the set of columns they change (few: a transaction changes the same columns again and again)
typedef struct { mw_cat *cat; tstmt *ts; int nts; uint32_t cookie; int sync; } rb_state;

static uint64_t now_ns (void) { struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec; }

static void state_reset_stmts (rb_state *r) {
    for (int i = 0; i < r->nts; i++) { sqlite3_finalize(r->ts[i].sel); sqlite3_finalize(r->ts[i].ins); sqlite3_finalize(r->ts[i].del); for (int k = 0; k < r->ts[i].nupd; k++) sqlite3_finalize(r->ts[i].upd[k].st); }
    free(r->ts); r->ts = NULL; r->nts = 0;
}
void mw_lane_rebase_free (mw_lane *lane) {
    rb_state *r = lane->rb_state;
    if (r) { state_reset_stmts(r); mw_cat_free(r->cat); free(r); lane->rb_state = NULL; }
    if (lane->rb_db) { sqlite3_close(lane->rb_db); lane->rb_db = NULL; }
}

// Percent-escape the characters that would end a URI path.
static char *uri_for (const char *path, bool mp) {
    size_t n = strlen(path);
    char *esc = sqlite3_malloc64(n * 3 + 1), *o = esc;
    if (!esc) return NULL;
    for (size_t i = 0; i < n; i++) {
        if (path[i] == '%' || path[i] == '?' || path[i] == '#') { sprintf(o, "%%%02X", (unsigned char)path[i]); o += 3; }
        else *o++ = path[i];
    }
    *o = 0;
    char *uri = sqlite3_mprintf("file:%s?mw=2&mw_norebase=1&mw_mp=%d", esc, mp ? 1 : 0);
    sqlite3_free(esc);
    return uri;
}
static int helper_open (mw_lane *lane) {
    if (lane->rb_db) return SQLITE_OK;
    char *uri = uri_for(lane->db->path, lane->db->mp_req);
    if (!uri) return SQLITE_NOMEM;
    sqlite3 *h = NULL;
    int rc = sqlite3_open_v2(uri, &h, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI | SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_PRIVATECACHE, MW_VFS_NAME);
    sqlite3_free(uri);
    if (rc != SQLITE_OK) { sqlite3_close(h); return rc; }
    sqlite3_busy_timeout(h, 0);
    sqlite3_db_config(h, SQLITE_DBCONFIG_ENABLE_TRIGGER, 0, NULL);                  // (the effects of a trigger are rows of the write set: they are replayed as rows)
    sqlite3_db_config(h, SQLITE_DBCONFIG_ENABLE_FKEY, 0, NULL);                      // (per batch: group_once)
    lane->rb_db = h;
    return SQLITE_OK;
}

// ---- records ----
typedef struct { int n; uint64_t ty[2048]; uint32_t off[2048], len[2048]; const uint8_t *rec; } rec_t;
static int rd_varint (const uint8_t *p, const uint8_t *end, uint64_t *out) {
    uint64_t v = 0;
    for (int i = 0; i < 9 && p + i < end; i++) {
        if (i == 8) { *out = (v << 8) | p[i]; return 9; }
        v = (v << 7) | (p[i] & 0x7f);
        if (!(p[i] & 0x80)) { *out = v; return i + 1; }
    }
    return 0;
}
static bool rec_parse (rec_t *r, const uint8_t *rec, uint32_t reclen) {
    r->rec = rec; r->n = 0;
    uint64_t hs; int h = rd_varint(rec, rec + reclen, &hs); if (!h || hs > reclen) return false;
    uint32_t pos = (uint32_t)hs, hp = (uint32_t)h;
    while (hp < hs && r->n < 2048) {
        uint64_t t; int k = rd_varint(rec + hp, rec + hs, &t); if (!k) return false; hp += (uint32_t)k;
        uint32_t l = t < 12 ? ((const uint8_t[]){0, 1, 2, 3, 4, 6, 8, 8, 0, 0, 0, 0})[t] : (uint32_t)((t - 12) / 2);
        r->ty[r->n] = t; r->off[r->n] = pos; r->len[r->n] = l; pos += l; r->n++;
    }
    return pos <= reclen;
}
static int bind_col (sqlite3_stmt *st, int idx, const rec_t *r, int col) {
    uint64_t t = r->ty[col]; const uint8_t *p = r->rec + r->off[col]; uint32_t l = r->len[col];
    if (t == 0) return sqlite3_bind_null(st, idx);
    if (t == 8 || t == 9) return sqlite3_bind_int64(st, idx, t == 9);
    if (t >= 1 && t <= 6) { int64_t x = (int8_t)p[0]; for (uint32_t b = 1; b < l; b++) x = (x << 8) | p[b]; return sqlite3_bind_int64(st, idx, x); }
    if (t == 7) { uint64_t u = 0; for (int b = 0; b < 8; b++) u = (u << 8) | p[b]; double d; memcpy(&d, &u, 8); return sqlite3_bind_double(st, idx, d); }
    if (t >= 12 && (t & 1)) return sqlite3_bind_text(st, idx, l ? (const char *)p : "", (int)l, SQLITE_TRANSIENT);
    if (t >= 12) return sqlite3_bind_blob(st, idx, l ? (const void *)p : (const void *)"", (int)l, SQLITE_TRANSIENT);
    return SQLITE_ERROR;
}

// A row written before an ALTER TABLE ADD COLUMN has fewer columns than the table: SQLite reads the missing ones as the default of the column. The record is rebuilt with them, in SQLite's own
// encoding (smallest serial type), so that everything below sees a full record. NULL: no memory.
static int varint_put (uint8_t *o, uint64_t v) {
    if (v >> 56) { o[8] = (uint8_t)(v & 0xff); v >>= 8; for (int i = 7; i >= 0; i--) { o[i] = (uint8_t)((v & 0x7f) | 0x80); v >>= 7; } return 9; }
    int n = 1; for (uint64_t t = v >> 7; t; t >>= 7) n++;
    for (int i = n - 1; i >= 0; i--) { o[i] = (uint8_t)((v & 0x7f) | (i == n - 1 ? 0 : 0x80)); v >>= 7; }
    return n;
}
static int val_serial (const mw_val *v, uint8_t *data) {          // the serial type of a default and its content in `data` (up to 8 bytes; text and blob are copied by the caller)
    switch (v->type) {
        case SQLITE_INTEGER: {
            int64_t x = v->i; if (x == 0) return 8; if (x == 1) return 9;
            int nb = (x >= -128 && x <= 127) ? 1 : (x >= -32768 && x <= 32767) ? 2 : (x >= -8388608 && x <= 8388607) ? 3 : (x >= INT32_MIN && x <= INT32_MAX) ? 4 : (x >= -((int64_t)1 << 47) && x < ((int64_t)1 << 47)) ? 5 : 6;
            int len = nb == 5 ? 6 : nb == 6 ? 8 : nb; uint64_t u = (uint64_t)x;
            for (int i = len - 1; i >= 0; i--) { data[i] = (uint8_t)(u & 0xff); u >>= 8; }
            return nb;
        }
        case SQLITE_FLOAT: { uint64_t u; memcpy(&u, &v->d, 8); for (int i = 7; i >= 0; i--) { data[i] = (uint8_t)(u & 0xff); u >>= 8; } return 7; }
        case SQLITE_TEXT: return 13 + 2 * v->n;
        case SQLITE_BLOB: return 12 + 2 * v->n;
        default: return 0;
    }
}
static uint8_t *rec_pad (const mw_tab *t, const rec_t *r, uint32_t *outlen) {
    int n = r->n; size_t datalen = 0; for (int i = 0; i < n; i++) datalen += r->len[i];
    uint64_t ty[2048]; uint8_t dd[2048][8]; uint32_t dl[2048];
    for (int i = n; i < t->nrec && i < 2048; i++) {
        ty[i] = (uint64_t)val_serial(&t->dflt[i], dd[i]);
        dl[i] = ty[i] < 12 ? ((const uint8_t[]){0, 1, 2, 3, 4, 6, 8, 8, 0, 0, 0, 0})[ty[i]] : (uint32_t)((ty[i] - 12) / 2);
        datalen += dl[i];
    }
    uint8_t hb[2048 * 9 + 16]; size_t hp = 0;
    for (int i = 0; i < t->nrec; i++) hp += (size_t)varint_put(hb + hp, i < n ? r->ty[i] : ty[i]);
    uint8_t tmp[9]; int hl = 1; while (varint_put(tmp, hp + (size_t)hl) != hl) hl = varint_put(tmp, hp + (size_t)hl);
    size_t hs = hp + (size_t)hl;
    uint8_t *buf = malloc(hs + datalen + 1); if (!buf) return NULL;
    varint_put(buf, hs); memcpy(buf + hl, hb, hp);
    size_t o = hs;
    for (int i = 0; i < n; i++) { memcpy(buf + o, r->rec + r->off[i], r->len[i]); o += r->len[i]; }
    for (int i = n; i < t->nrec; i++) {
        const mw_val *v = &t->dflt[i];
        if (v->type == SQLITE_TEXT || v->type == SQLITE_BLOB) memcpy(buf + o, v->p, (size_t)v->n); else memcpy(buf + o, dd[i], dl[i]);
        o += dl[i];
    }
    *outlen = (uint32_t)o; return buf;
}

// ---- the statements of a table ----
static bool name_is (const char *a, const char *b) { return strcasecmp(a, b) == 0; }
static int tstmt_prepare (sqlite3 *h, const mw_tab *t, tstmt *s) {
    const char *rid = NULL; static const char *cand[] = { "rowid", "_rowid_", "oid" };
    for (int c = 0; c < 3 && !rid; c++) { bool used = false; for (int i = 0; i < t->nrec; i++) if (t->rec_name[i] && name_is(t->rec_name[i], cand[c])) used = true; if (!used) rid = cand[c]; }
    if (!rid) return SQLITE_MISUSE;
    char *cols = sqlite3_mprintf(""), *marks = sqlite3_mprintf(""), *conds = sqlite3_mprintf(""); int k = 1;
    for (int i = 0; i < t->nrec; i++) {
        if (!t->rec_name[i] || i == t->alias_rec) continue;
        k++;
        char *a = sqlite3_mprintf("%s,\"%w\"", cols, t->rec_name[i]); sqlite3_free(cols); cols = a;
        a = sqlite3_mprintf("%s,?%d", marks, k); sqlite3_free(marks); marks = a;
        a = sqlite3_mprintf("%s AND \"%w\" COLLATE BINARY IS ?%d", conds, t->rec_name[i], k); sqlite3_free(conds); conds = a;
    }
    char *q[3] = { sqlite3_mprintf("SELECT 1 FROM \"%w\" WHERE %s=?1%s", t->name, rid, conds),
                   sqlite3_mprintf("INSERT INTO \"%w\"(%s%s) VALUES(?1%s)", t->name, rid, cols, marks),
                   sqlite3_mprintf("DELETE FROM \"%w\" WHERE %s=?1", t->name, rid) };
    int rc = SQLITE_OK;
    if (q[0] && sqlite3_prepare_v2(h, q[0], -1, &s->sel, NULL) != SQLITE_OK) rc = SQLITE_ERROR;
    if (rc == SQLITE_OK && q[1] && sqlite3_prepare_v2(h, q[1], -1, &s->ins, NULL) != SQLITE_OK) rc = SQLITE_ERROR;
    if (rc == SQLITE_OK && q[2] && sqlite3_prepare_v2(h, q[2], -1, &s->del, NULL) != SQLITE_OK) rc = SQLITE_ERROR;
    for (int i = 0; i < 3; i++) sqlite3_free(q[i]);
    sqlite3_free(cols); sqlite3_free(marks); sqlite3_free(conds);
    s->ready = rc == SQLITE_OK;
    return rc;
}

static int bind_row (sqlite3_stmt *st, const mw_tab *t, int64_t rowid, const rec_t *r, bool use_mask, uint64_t only) {      // use_mask: bind just the columns of `only` (an UPDATE)
    sqlite3_reset(st); sqlite3_clear_bindings(st);
    sqlite3_bind_int64(st, 1, rowid);
    int k = 1, bit = 0;
    for (int i = 0; i < t->nrec; i++) {
        if (!t->rec_name[i] || i == t->alias_rec) continue;
        bool take = !use_mask || (bit < 64 && (only >> bit & 1));
        bit++;
        if (!take) continue;
        k++;
        if (r && bind_col(st, k, r, i) != SQLITE_OK) return SQLITE_ERROR;
    }
    return SQLITE_OK;
}

// The columns of the record that an update changes, as a bit set over the writable columns (in order); false if the table has too many of them. An UPDATE that names a column that has not changed (an indexed one
// especially) writes the pages of its index: other transactions that only read those pages would be refused for it.
static bool changed_mask (const mw_tab *t, const rec_t *o, const rec_t *n, uint64_t *mask) {
    int bit = 0; *mask = 0;
    for (int i = 0; i < t->nrec; i++) {
        if (!t->rec_name[i] || i == t->alias_rec) continue;
        if (bit >= 64) return false;
        if (o->ty[i] != n->ty[i] || o->len[i] != n->len[i] || (o->len[i] && memcmp(o->rec + o->off[i], n->rec + n->off[i], o->len[i]) != 0)) *mask |= 1ull << bit;
        bit++;
    }
    return true;
}
static sqlite3_stmt *update_stmt (sqlite3 *h, const mw_tab *t, tstmt *ts, uint64_t mask) {
    for (int k = 0; k < ts->nupd; k++) if (ts->upd[k].mask == mask) return ts->upd[k].st;
    const char *rid = "rowid"; static const char *cand[] = { "rowid", "_rowid_", "oid" };
    for (int c = 0; c < 3; c++) { bool used = false; for (int i = 0; i < t->nrec; i++) if (t->rec_name[i] && name_is(t->rec_name[i], cand[c])) used = true; if (!used) { rid = cand[c]; break; } }
    char *sets = sqlite3_mprintf(""); int bit = 0, k = 1;
    for (int i = 0; i < t->nrec; i++) {
        if (!t->rec_name[i] || i == t->alias_rec) continue;
        if (mask >> bit & 1) { k++; char *a = sqlite3_mprintf("%s%s\"%w\"=?%d", sets, *sets ? "," : "", t->rec_name[i], k); sqlite3_free(sets); sets = a; }
        bit++;
    }
    char *q = sqlite3_mprintf("UPDATE \"%w\" SET %s WHERE %s=?1", t->name, sets, rid); sqlite3_free(sets);
    sqlite3_stmt *st = NULL; int rc = q ? sqlite3_prepare_v2(h, q, -1, &st, NULL) : SQLITE_NOMEM; sqlite3_free(q);
    if (rc != SQLITE_OK) return NULL;
    int slot;
    if (ts->nupd < 8) slot = ts->nupd++; else { slot = ts->next_upd++ & 7; sqlite3_finalize(ts->upd[slot].st); }
    ts->upd[slot] = (ucache){ mask, st };
    return st;
}

// ---- the group replay ----
// The replays are serialised, and what is serialised is batched: the connections whose commit conflicted queue a request (their decoded row changes); the first of them becomes the leader, takes everything that is queued
// (up to MW_REBASE_BATCH), and replays all of it in ONE transaction of its helper connection at the latest snapshot, a savepoint for each request, so that a request that is a true conflict is rolled back alone, and
// the others commit together at one epoch. The requests are replayed in the order they queued, each one checked against the rows as the ones before it left them: the result is a serial execution in that order
// (mw_tx_info.commit_order says which, among the commits of one epoch). One commit, one install of the pages, one epoch for all, instead of a lost race and a commit for each.
#define MW_REBASE_BATCH 64
typedef struct rb_req { mw_lane *lane; const mw_rd_result *res; uint32_t cookie; bool fk; int state; bool done; uint64_t epoch; uint32_t order; struct rb_req *next; } rb_req;      // state: 0 waiting, 1 committed, 2 refused (the leader's, provisional until done)

// One row change in the open transaction of the helper: SQLITE_OK replayed, MW_CONFLICT a true conflict (the row changed, a constraint), SQLITE_BUSY lost a race.
static int apply_chg (sqlite3 *h, const mw_tab *lt, tstmt *ts, const mw_chg *c) {
    if (!ts->ready && tstmt_prepare(h, lt, ts) != SQLITE_OK) return MW_CONFLICT;
    rec_t oldr, newr; uint8_t *ob = NULL, *nb = NULL; int rc = MW_CONFLICT, src; uint32_t l;
    if (c->old_rec) {
        if (!rec_parse(&oldr, c->old_rec, c->old_len) || oldr.n > lt->nrec) goto out;
        if (oldr.n < lt->nrec) { ob = rec_pad(lt, &oldr, &l); if (!ob || !rec_parse(&oldr, ob, l)) goto out; }      // (a row written before an ALTER TABLE ADD COLUMN: its missing columns are defaults)
    }
    if (c->new_rec) {
        if (!rec_parse(&newr, c->new_rec, c->new_len) || newr.n > lt->nrec) goto out;
        if (newr.n < lt->nrec) { nb = rec_pad(lt, &newr, &l); if (!nb || !rec_parse(&newr, nb, l)) goto out; }
    }
    if (c->old_rec) {                                                  // it is the row that the transaction saw
        if (bind_row(ts->sel, lt, c->rowid, &oldr, false, 0) != SQLITE_OK) goto out;
        src = sqlite3_step(ts->sel); sqlite3_reset(ts->sel);
        if ((src & 0xff) == SQLITE_BUSY) { rc = SQLITE_BUSY; goto out; }
        if (src != SQLITE_ROW) goto out;
    }
    if (c->kind == 3) { if (bind_row(ts->del, lt, c->rowid, NULL, false, 0) != SQLITE_OK) goto out; src = sqlite3_step(ts->del); sqlite3_reset(ts->del); }
    else if (c->kind == 2) {
        uint64_t mask; if (!changed_mask(lt, &oldr, &newr, &mask)) goto out;
        if (mask == 0) { rc = SQLITE_OK; goto out; }                                                  // (only a derived column changed)
        if (lt->is_parent && (mask & lt->refmask)) goto out;                                          // (a key that a foreign key refers to changes: the ON UPDATE actions are not replayed)
        sqlite3_stmt *us = update_stmt(h, lt, ts, mask); if (!us) goto out;
        if (bind_row(us, lt, c->rowid, &newr, true, mask) != SQLITE_OK) goto out;
        src = sqlite3_step(us); sqlite3_reset(us);
    }
    else { if (bind_row(ts->ins, lt, c->rowid, &newr, false, 0) != SQLITE_OK) goto out; src = sqlite3_step(ts->ins); sqlite3_reset(ts->ins); }
    rc = src == SQLITE_DONE ? SQLITE_OK : (src & 0xff) == SQLITE_BUSY ? SQLITE_BUSY : MW_CONFLICT;
out:
    free(ob); free(nb);
    return rc;
}

// One request. Deletes, then updates, then inserts (a key that one frees and another takes). With foreign keys enforced: updates, deletes, inserts (a child that an action would set to NULL is already updated
// when its parent goes), the deletes from the children to the parents and the inserts from the parents to the children (the rank of the table), so that the rows that the actions would take are already gone,
// and a child that appeared meanwhile is dealt with by SQLite as it would have been in a serial execution (cascaded, or the delete refused). A parent table in which the request both deletes and inserts is
// not replayed (a key that changes is a delete and an insert: the ON DELETE action would run), and neither is an update of a key that a foreign key refers to (the ON UPDATE action).
static int replay_req (sqlite3 *h, rb_state *R, const rb_req *rq) {
    const mw_rd_result *res = rq->res; const mw_cat *cat = R->cat;
    if (rq->fk) {
        unsigned char *fl = calloc((size_t)cat->n + 1, 1); if (!fl) return MW_CONFLICT;
        for (int i = 0; i < res->n; i++) {
            const mw_chg *c = &res->chg[i]; const mw_tab *lt = mw_cat_by_name(cat, c->tab->name); if (!lt) { free(fl); return MW_CONFLICT; }
            if (lt->is_parent) { fl[lt - cat->tabs] |= c->kind == 3 ? 1 : c->kind == 1 ? 2 : 0; if (fl[lt - cat->tabs] == 3) { free(fl); return MW_CONFLICT; } }
        }
        free(fl);
    }
    static const int plain[3] = { 3, 2, 1 }, withfk[3] = { 2, 3, 1 };
    for (int ph = 0; ph < 3; ph++) {
        int phase = rq->fk ? withfk[ph] : plain[ph];
        int steps = (rq->fk && phase != 2) ? cat->maxrank : 0;
        for (int step = 0; step <= steps; step++) {
            int rank = phase == 3 ? cat->maxrank - step : step;
            const mw_tab *lt_prev = NULL, *lt = NULL; const mw_tab *rt_prev = NULL;
            for (int i = 0; i < res->n; i++) {
                const mw_chg *c = &res->chg[i]; if (c->kind != phase) continue;
                if (c->tab != rt_prev) { lt = mw_cat_by_name(cat, c->tab->name); rt_prev = c->tab; lt_prev = lt; } else lt = lt_prev;      // (the leader's own catalog: the same schema, the same cookie)
                if (!lt || !lt->ok) return MW_CONFLICT;
                if (steps && lt->rank != rank) continue;
                int r = apply_chg(h, lt, &R->ts[lt - cat->tabs], c);
                if (r != SQLITE_OK) return r;
            }
        }
    }
    return SQLITE_OK;
}

// All the requests of a batch in one transaction at the snapshot that the helper has now. SQLITE_OK: the transaction committed (or no request could be replayed: every state is final); SQLITE_BUSY: lost the race, again.
static int group_once (mw_lane *L, rb_state *R, rb_req **b, int n) {
    sqlite3 *h = L->rb_db;
    for (int i = 0; i < n; i++) b[i]->state = 0;
    sqlite3_db_config(h, SQLITE_DBCONFIG_ENABLE_FKEY, b[0]->fk ? 1 : 0, NULL);       // (the connections of a batch are replayed as the first one has its foreign keys; the others, if they differ, run again)
    if (sqlite3_exec(h, "BEGIN", NULL, NULL, NULL) != SQLITE_OK) return SQLITE_BUSY;
    int ok = 0, rc = SQLITE_OK; uint32_t cookie = 0;
    sqlite3_stmt *sv = NULL;
    if (sqlite3_prepare_v2(h, "PRAGMA schema_version", -1, &sv, NULL) == SQLITE_OK && sqlite3_step(sv) == SQLITE_ROW) cookie = (uint32_t)sqlite3_column_int64(sv, 0); else { sqlite3_finalize(sv); sqlite3_exec(h, "ROLLBACK", NULL, NULL, NULL); for (int i = 0; i < n; i++) b[i]->state = 2; return SQLITE_OK; }
    sqlite3_finalize(sv);
    for (int i = 0; i < n; i++) {
        rb_req *rq = b[i];
        if (rq->cookie != cookie || rq->fk != b[0]->fk) { rq->state = 2; continue; }                // (the schema changed meanwhile)
        sqlite3_exec(h, "SAVEPOINT g", NULL, NULL, NULL);
        int r = replay_req(h, R, rq);
        if (r == SQLITE_OK) { sqlite3_exec(h, "RELEASE g", NULL, NULL, NULL); rq->state = 1; ok++; }
        else if (r == SQLITE_BUSY) { rc = SQLITE_BUSY; break; }
        else { sqlite3_exec(h, "ROLLBACK TO g", NULL, NULL, NULL); sqlite3_exec(h, "RELEASE g", NULL, NULL, NULL); rq->state = 2; }      // (a true conflict: this request alone is taken back)
    }
    if (rc == SQLITE_BUSY || ok == 0) { sqlite3_exec(h, "ROLLBACK", NULL, NULL, NULL); if (rc == SQLITE_BUSY) return SQLITE_BUSY; return SQLITE_OK; }
    int crc = sqlite3_exec(h, "COMMIT", NULL, NULL, NULL);
    if (crc != SQLITE_OK) {
        sqlite3_exec(h, "ROLLBACK", NULL, NULL, NULL);
        if ((crc & 0xff) == SQLITE_BUSY) return SQLITE_BUSY;
        for (int i = 0; i < n; i++) if (b[i]->state == 1) b[i]->state = 2;
        return SQLITE_OK;
    }
    mw_tx_info ti; memset(&ti, 0, sizeof ti); sqlite3_file_control(h, "main", MW_FCNTL_TXINFO, &ti);
    uint32_t order = 0;
    for (int i = 0; i < n; i++) if (b[i]->state == 1) { if (ti.commit_epoch) { b[i]->epoch = ti.commit_epoch; b[i]->order = order++; } else b[i]->state = 2; }
    return SQLITE_OK;
}

static void run_batch (mw_lane *L, rb_req **b, int n) {
    mw_db *db = L->db; rb_state *R = L->rb_state;
    uint64_t t0 = now_ns();
    if (helper_open(L) != SQLITE_OK) { for (int i = 0; i < n; i++) b[i]->state = 2; return; }
    if (R->sync != L->sync_level) { char q[40]; snprintf(q, sizeof q, "PRAGMA synchronous=%d", L->sync_level); sqlite3_exec(L->rb_db, q, NULL, NULL, NULL); R->sync = L->sync_level; }
    mw_lane *hl = NULL; { void *lp = NULL; if (sqlite3_file_control(L->rb_db, "main", MW_FCNTL_LANE_PTR, &lp) == SQLITE_OK) hl = lp; }
    if (db->mp) mw_mp_rebase_lock(db);                                          // (one batch at a time in all the processes)
    int attempt = 0; bool closed = false;
    for (; attempt < MW_REBASE_MAX_ATTEMPTS; attempt++) {
        if (attempt == MW_REBASE_GATE_AFTER && hl) { mw_gate_close(db, hl); closed = true; }      // (the next attempt runs against a frozen state)
        if (group_once(L, R, b, n) != SQLITE_BUSY) break;
        atomic_fetch_add(&db->n_rebase_retries, 1);
        if (!closed) sched_yield();
    }
    if (attempt == MW_REBASE_MAX_ATTEMPTS) for (int i = 0; i < n; i++) b[i]->state = 2;
    if (closed) mw_gate_open(db);
    if (db->mp) mw_mp_rebase_unlock(db);
    int done = 0; for (int i = 0; i < n; i++) if (b[i]->state == 1) done++;
    atomic_fetch_add(&db->n_rebases, (uint64_t)done);
    if (done > 1) atomic_fetch_add(&db->n_rebase_grouped, (uint64_t)done);
    uint64_t att = (uint64_t)attempt + 1, m = atomic_load(&db->n_rebase_max_attempts);
    while (att > m && !atomic_compare_exchange_weak(&db->n_rebase_max_attempts, &m, att)) {}
    atomic_fetch_add(&db->n_rebase_ns, now_ns() - t0);
}

// The commit of `lane` conflicted on pages that it wrote: replay its row changes. SQLITE_OK: committed at *out_epoch (the lane's own pages are not published); MW_CONFLICT: refused.
int mw_lane_rebase (mw_lane *lane, const uint8_t *const *imgs, uint32_t cookie, uint64_t *out_epoch) {
    mw_db *db = lane->db;
    rb_state *R = lane->rb_state;
    if (!R) { R = calloc(1, sizeof *R); if (!R) return MW_CONFLICT; lane->rb_state = R; }
    if (!R->cat || R->cat->cookie != cookie) { state_reset_stmts(R); mw_cat_free(R->cat); R->cat = mw_cat_build(lane); if (R->cat) { R->ts = calloc((size_t)(R->cat->n ? R->cat->n : 1), sizeof *R->ts); R->nts = R->cat->n; } }
    if (!R->cat || !R->ts || !R->cat->rebasable || R->cat->cookie != cookie) { atomic_fetch_add(&db->n_unrebasable, 1); return MW_CONFLICT; }
    mw_rd_result res;
    mw_rowdiff_compute(lane, imgs, R->cat, &res);                               // (in parallel: every connection decodes its own pages)
    if (res.unsupported || res.n == 0) { mw_rd_result_free(&res); atomic_fetch_add(&db->n_unrebasable, 1); return MW_CONFLICT; }
    if (mw_lane_reads_unchanged(lane)) { mw_rd_result_free(&res); atomic_fetch_add(&db->n_unrebasable, 1); return MW_CONFLICT; }       // (it read rows that it did not change: write skew, the application retries)
    bool fk = false;
    if (R->cat->has_fk) { int on = 1; if (lane->rd_db) sqlite3_db_config(lane->rd_db, SQLITE_DBCONFIG_ENABLE_FKEY, -1, &on); fk = on != 0; }       // (an application that does not enforce them: the replay does not either)
    rb_req rq = { .lane = lane, .res = &res, .cookie = cookie, .fk = fk };
    pthread_mutex_lock(&db->rb_qmu);
    if (db->rb_qtail) ((rb_req *)db->rb_qtail)->next = &rq; else db->rb_qhead = &rq;
    db->rb_qtail = &rq;
    while (!rq.done) {
        if (!db->rb_leader) {                                                   // the leader of the next batch: what is queued now, in the order it came
            db->rb_leader = true;
            rb_req *batch[MW_REBASE_BATCH]; int n = 0;
            while (db->rb_qhead && n < MW_REBASE_BATCH) { rb_req *x = db->rb_qhead; db->rb_qhead = x->next; if (!db->rb_qhead) db->rb_qtail = NULL; x->next = NULL; batch[n++] = x; }
            pthread_mutex_unlock(&db->rb_qmu);
            run_batch(lane, batch, n);
            pthread_mutex_lock(&db->rb_qmu);
            for (int i = 0; i < n; i++) batch[i]->done = true;                  // (the states were provisional until now: a waiter that woke up for another reason must not read them)
            db->rb_leader = false;
            pthread_cond_broadcast(&db->rb_qcv);
        } else pthread_cond_wait(&db->rb_qcv, &db->rb_qmu);
    }
    pthread_mutex_unlock(&db->rb_qmu);
    mw_rd_result_free(&res);
    if (rq.state == 1) { *out_epoch = rq.epoch; lane->tx.commit_epoch = rq.epoch; lane->tx.commit_order = rq.order; return SQLITE_OK; }
    return MW_CONFLICT;
}
