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
//  What is not: the rows that the transaction read and did not change. A page that it only read is validated as before (a change to it refuses the commit), but a row that it read from a page that
//  it also wrote is not looked at: that is snapshot isolation (write skew is possible there), what a page-level validation did not allow.
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
#define MW_REBASE_GATE_AFTER   32     // lost attempts before the publication gate is closed for the next one (measured 20-60% faster than closing it after 3: the gate stops every committer; a replay that is still losing after 32 is starving)

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
    int rc = sqlite3_open_v2(uri, &h, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI | SQLITE_OPEN_NOMUTEX, MW_VFS_NAME);
    sqlite3_free(uri);
    if (rc != SQLITE_OK) { sqlite3_close(h); return rc; }
    sqlite3_busy_timeout(h, 0);
    sqlite3_db_config(h, SQLITE_DBCONFIG_ENABLE_TRIGGER, 0, NULL);                  // (the effects of a trigger are rows of the write set: they are replayed as rows)
    sqlite3_db_config(h, SQLITE_DBCONFIG_ENABLE_FKEY, 0, NULL);
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

// One replay at the snapshot that the helper has now. SQLITE_OK: committed. MW_CONFLICT: a true conflict (the row changed, a constraint, the schema): refuse. SQLITE_BUSY: lost the race: again.
static int replay_once (mw_lane *lane, rb_state *R, const mw_rd_result *res, uint32_t cookie, uint64_t *epoch) {
    sqlite3 *h = lane->rb_db;
    if (sqlite3_exec(h, "BEGIN", NULL, NULL, NULL) != SQLITE_OK) return SQLITE_BUSY;
    int rc = MW_CONFLICT;
    sqlite3_stmt *sv = NULL;
    if (sqlite3_prepare_v2(h, "PRAGMA schema_version", -1, &sv, NULL) != SQLITE_OK) goto out;
    if (sqlite3_step(sv) != SQLITE_ROW || (uint32_t)sqlite3_column_int64(sv, 0) != cookie) { sqlite3_finalize(sv); sv = NULL; goto out; }     // (the schema changed meanwhile)
    sqlite3_finalize(sv); sv = NULL;
    for (int phase = 3; phase >= 1; phase--) {                                  // deletes, then updates, then inserts: a key that one frees and another takes
        for (int i = 0; i < res->n; i++) {
            const mw_chg *c = &res->chg[i]; if (c->kind != phase) continue;
            int ti = (int)(c->tab - R->cat->tabs); tstmt *ts = &R->ts[ti];
            rec_t oldr, newr;
            if (c->old_rec && !rec_parse(&oldr, c->old_rec, c->old_len)) goto out;
            if (c->new_rec && !rec_parse(&newr, c->new_rec, c->new_len)) goto out;
            if ((c->old_rec && oldr.n < c->tab->nrec) || (c->new_rec && newr.n < c->tab->nrec)) goto out;      // (a row written before an ALTER TABLE ADD COLUMN: its missing columns are defaults)
            int src;
            if (c->old_rec) {                                                  // it is the row that the transaction saw
                if (bind_row(ts->sel, c->tab, c->rowid, &oldr, false, 0) != SQLITE_OK) goto out;
                src = sqlite3_step(ts->sel); sqlite3_reset(ts->sel);
                if (src == SQLITE_BUSY || (src & 0xff) == SQLITE_BUSY) { rc = SQLITE_BUSY; goto out; }
                if (src != SQLITE_ROW) goto out;
            }
            if (c->kind == 3) { if (bind_row(ts->del, c->tab, c->rowid, NULL, false, 0) != SQLITE_OK) goto out; src = sqlite3_step(ts->del); sqlite3_reset(ts->del); }
            else if (c->kind == 2) {
                uint64_t mask; if (!changed_mask(c->tab, &oldr, &newr, &mask)) goto out;
                if (mask == 0) continue;                                                                // (only a derived column changed)
                sqlite3_stmt *us = update_stmt(h, c->tab, ts, mask); if (!us) goto out;
                if (bind_row(us, c->tab, c->rowid, &newr, true, mask) != SQLITE_OK) goto out;
                src = sqlite3_step(us); sqlite3_reset(us);
            }
            else { if (bind_row(ts->ins, c->tab, c->rowid, &newr, false, 0) != SQLITE_OK) goto out; src = sqlite3_step(ts->ins); sqlite3_reset(ts->ins); }
            if (src != SQLITE_DONE) { if ((src & 0xff) == SQLITE_BUSY) rc = SQLITE_BUSY; goto out; }
        }
    }
    {
        int crc = sqlite3_exec(h, "COMMIT", NULL, NULL, NULL);
        if (crc == SQLITE_OK) {
            mw_tx_info ti; memset(&ti, 0, sizeof ti); sqlite3_file_control(h, "main", MW_FCNTL_TXINFO, &ti);
            if (getenv("MW_REBASE_DEBUG")) { fprintf(stderr, "REBASE ok: tx snapshot %llu, helper snapshot %llu, commit %llu, changes:", (unsigned long long)lane->tx.snapshot_epoch, (unsigned long long)ti.snapshot_epoch, (unsigned long long)ti.commit_epoch); for (int i = 0; i < res->n; i++) fprintf(stderr, " k%d r%lld", res->chg[i].kind, (long long)res->chg[i].rowid); fprintf(stderr, "\n"); }
            *epoch = ti.commit_epoch; return ti.commit_epoch ? SQLITE_OK : MW_CONFLICT;
        }
        rc = (crc & 0xff) == SQLITE_BUSY ? SQLITE_BUSY : MW_CONFLICT;
    }
out:
    sqlite3_exec(h, "ROLLBACK", NULL, NULL, NULL);
    return rc;
}

// The commit of `lane` conflicted on pages that it wrote: replay its row changes. SQLITE_OK: committed at *out_epoch (the lane's own pages are not published); MW_CONFLICT: refused.
int mw_lane_rebase (mw_lane *lane, const uint8_t *const *imgs, uint32_t cookie, uint64_t *out_epoch) {
    mw_db *db = lane->db;
    uint64_t t0 = now_ns();
    rb_state *R = lane->rb_state;
    if (!R) { R = calloc(1, sizeof *R); if (!R) return MW_CONFLICT; lane->rb_state = R; }
    if (!R->cat || R->cat->cookie != cookie) { state_reset_stmts(R); mw_cat_free(R->cat); R->cat = mw_cat_build(lane); if (R->cat) { R->ts = calloc((size_t)(R->cat->n ? R->cat->n : 1), sizeof *R->ts); R->nts = R->cat->n; } }
    if (!R->cat || !R->ts || !R->cat->rebasable || R->cat->cookie != cookie) { atomic_fetch_add(&db->n_unrebasable, 1); return MW_CONFLICT; }
    mw_rd_result res;
    mw_rowdiff_compute(lane, imgs, R->cat, &res);
    if (res.unsupported || res.n == 0) { mw_rd_result_free(&res); atomic_fetch_add(&db->n_unrebasable, 1); return MW_CONFLICT; }
    if (helper_open(lane) != SQLITE_OK) { mw_rd_result_free(&res); return MW_CONFLICT; }
    for (int i = 0; i < res.n; i++) {                                           // the statements of the tables that this transaction touched
        int ti = (int)(res.chg[i].tab - R->cat->tabs); tstmt *ts = &R->ts[ti];
        if (!ts->ready && tstmt_prepare(lane->rb_db, res.chg[i].tab, ts) != SQLITE_OK) { mw_rd_result_free(&res); atomic_fetch_add(&db->n_unrebasable, 1); return MW_CONFLICT; }
    }
    if (R->sync != lane->sync_level) { char q[40]; snprintf(q, sizeof q, "PRAGMA synchronous=%d", lane->sync_level); sqlite3_exec(lane->rb_db, q, NULL, NULL, NULL); R->sync = lane->sync_level; }
    mw_lane *hl = NULL; { void *lp = NULL; if (sqlite3_file_control(lane->rb_db, "main", MW_FCNTL_LANE_PTR, &lp) == SQLITE_OK) hl = lp; }
    pthread_mutex_lock(&db->rebase_mu);                                         // (one replay at a time: replays that run together lose races against each other and against the ordinary commits, measured 20-40% slower; the decoding above is parallel)
    int rc = MW_CONFLICT, attempt = 0; bool closed = false; uint64_t epoch = 0;
    for (; attempt < MW_REBASE_MAX_ATTEMPTS; attempt++) {
        if (attempt == MW_REBASE_GATE_AFTER && hl) { mw_gate_close(db, hl); closed = true; }      // (the next attempt runs against a frozen state)
        rc = replay_once(lane, R, &res, cookie, &epoch);
        if (rc != SQLITE_BUSY) break;
        atomic_fetch_add(&db->n_rebase_retries, 1);
        if (!closed) sched_yield();
    }
    if (closed) mw_gate_open(db);
    pthread_mutex_unlock(&db->rebase_mu);
    mw_rd_result_free(&res);
    uint64_t att = (uint64_t)attempt + 1, m = atomic_load(&db->n_rebase_max_attempts);
    while (att > m && !atomic_compare_exchange_weak(&db->n_rebase_max_attempts, &m, att)) {}
    atomic_fetch_add(&db->n_rebase_ns, now_ns() - t0);
    if (rc == SQLITE_OK) { atomic_fetch_add(&db->n_rebases, 1); *out_epoch = epoch; lane->tx.commit_epoch = epoch; return SQLITE_OK; }
    return MW_CONFLICT;
}
