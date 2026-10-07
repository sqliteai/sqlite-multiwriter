// Randomised serializability check of the commit protocol.
// Concurrent transactions (inserts, updates, deletes, changes of a UNIQUE column, growth of a payload that splits and frees pages) run on a small set of keys; each one records what it
// read and what it did, and the epoch it read from and the epoch it committed at (MW_FCNTL_TXINFO). Afterwards the committed transactions are replayed on a model, one at a time in the order
// of their commit epochs, and the test checks that
//   1. every read-write transaction saw exactly the state that the model has just before it: the database behaved as the serial execution in commit order (every key that a transaction reads it
//      also writes, so under snapshot isolation with first-committer-wins this must hold exactly: a lost update, a write that goes through on a row that was deleted meanwhile, a UNIQUE value
//      held by two rows, would all show here);
//   2. a read-only transaction saw exactly the state after the commits up to the epoch it read from (a consistent snapshot);
//   3. the final content of the table is the model's, the UNIQUE column has no duplicate (checked with a table scan, not through its index), and integrity_check is ok.
// The same check with real processes (`mw_mp=1`, one connection each) that are killed with SIGKILL at random moments and replaced by new ones. A transaction that was killed between its
// intent (written to the process's record file before COMMIT) and its outcome is in doubt: each process owns a row of `txlog` (a page of its own: no conflicts between processes) that every
// transaction of it updates with its sequence number, so after the kill the database itself says whether it committed. Its commit epoch is not known: the model replays it at an epoch that
// no recorded transaction has (every commit takes the next epoch) and at which its reads fit the model, so a commit that is lost, or that nobody recorded, shows as a gap with no transaction that fits.
// Write skew (a transaction that reads a key it does not write) is not generated: snapshot isolation does not prevent it, and the test must pass on the engine as it is. It is the check that a
// finer-grained conflict detection (a logical replay of the commits that conflict only on a page) would have to keep passing.
#include <pthread.h>
#include <stdatomic.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/wait.h>
#include "mw_test.h"
#include "multiwriter.h"

enum { MAXOPS = 4, ROKEYS = 8 };
typedef struct { int kind, key, a, b; int sex, sv, su, spl, sw; } op_t;                 // kinds: 0 touch (w += 1: a write that the page really sees), 1 v += a, 2 delete, 3 u = b (-1: NULL), 4 insert (v = a, u = b, payload ins_pl), 5 payload = a bytes
typedef struct { uint64_t snap, commit; uint64_t seq; int n; op_t op[MAXOPS]; int ins_pl; int slot; uint32_t order; } rw_t;      // order: among the commits of one epoch (a group replay of the rebase), their place
typedef struct { uint64_t snap; int n; int key[ROKEYS], ex[ROKEYS], v[ROKEYS], u[ROKEYS], pl[ROKEYS], w[ROKEYS]; } ro_t;
typedef struct { rw_t *rw; size_t nrw, caprw; ro_t *ro; size_t nro, capro; long busy, constraint, other; } rec_t;

static const char *g_path; static int g_keys; static double g_secs; static int g_procs_mode, g_threadmode, g_rebase;      // g_threadmode: the "processes" of the power-loss run are threads of one process (the engine's single-process mode)
static int open_db (const char *path, sqlite3 **db) {
    char uri[300]; snprintf(uri, sizeof uri, "file:%s?mw=2&mw_gc=%d%s%s", path, getenv("MW_SERIAL_GC") ? atoi(getenv("MW_SERIAL_GC")) : 16, ((g_procs_mode && !g_threadmode) || getenv("MW_TEST_MP")) ? "&mw_mp=1" : "", g_rebase ? "&mw_rebase=1" : "");
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) { sqlite3_extended_result_codes(*db, 1); sqlite3_busy_timeout(*db, 0); }
    return rc;
}
static uint64_t rnd (unsigned *s) { *s = *s * 1103515245u + 12345u; uint64_t x = *s >> 8; *s = *s * 1103515245u + 12345u; return (x << 16) ^ (*s >> 8); }
static int ucol (unsigned *s) { uint64_t r = rnd(s) % ((uint64_t)g_keys * 2 + 3); return r > (uint64_t)g_keys * 2 ? -1 : (int)r; }      // (a domain about twice the keys: collisions are common)

static int read_row (sqlite3_stmt *sel, int key, int *ex, int *v, int *u, int *pl, int *w) {
    sqlite3_reset(sel); sqlite3_bind_int(sel, 1, key);
    int rc = sqlite3_step(sel);
    if (rc == SQLITE_ROW) { *ex = 1; *v = sqlite3_column_int(sel, 0); *u = sqlite3_column_type(sel, 1) == SQLITE_NULL ? -1 : sqlite3_column_int(sel, 1); *pl = sqlite3_column_int(sel, 2); *w = sqlite3_column_int(sel, 3); rc = SQLITE_OK; }
    else if (rc == SQLITE_DONE) { *ex = 0; *v = *u = *pl = *w = 0; rc = SQLITE_OK; }
    sqlite3_reset(sel); return rc;
}
static uint64_t snap_epoch (sqlite3 *db) { mw_tx_info ti; memset(&ti, 0, sizeof ti); sqlite3_file_control(db, "main", MW_FCNTL_TXINFO, &ti); return ti.snapshot_epoch; }
static uint64_t commit_epoch (sqlite3 *db, uint32_t *order) { mw_tx_info ti; memset(&ti, 0, sizeof ti); sqlite3_file_control(db, "main", MW_FCNTL_TXINFO, &ti); *order = ti.commit_order; return ti.commit_epoch; }

// ---- the record file of a process (one write() per record: a kill never leaves half of one in the middle) ----
enum { REC_INTENT = 1, REC_COMMIT, REC_RO, REC_RESOLVE, REC_ABORT, REC_FULL };
typedef struct { uint32_t type, slot; uint64_t seq, epoch; uint32_t order, pad; } rhdr;
static void put_rec (int fd, uint32_t type, int slot, uint64_t seq, uint64_t epoch, uint32_t order, const void *pl, size_t n) {
    if (fd < 0) return;
    char buf[sizeof(rhdr) + sizeof(rw_t) + sizeof(ro_t)]; rhdr h = { type, (uint32_t)slot, seq, epoch, order, 0 };
    memcpy(buf, &h, sizeof h); if (n) memcpy(buf + sizeof h, pl, n);
    if (write(fd, buf, sizeof h + n) != (ssize_t)(sizeof h + n)) _exit(3);
}

// one transaction of a worker. proc: the slot of a process (>= 0: the transaction also updates its txlog row and writes its records to fd), -1 for a thread.
typedef struct { sqlite3 *db; sqlite3_stmt *sel; unsigned rng; int slot, fd; uint64_t seq; rec_t *R; } ctx_t;
static void run_ro (ctx_t *c, ro_t *out, int *got) {
    ro_t ro; memset(&ro, 0, sizeof ro); ro.n = ROKEYS; *got = 0;
    if (mw_exec(c->db, "BEGIN") != SQLITE_OK) return;
    int bad = 0;
    for (int i = 0; i < ROKEYS && !bad; i++) { ro.key[i] = 1 + (int)(rnd(&c->rng) % (uint64_t)g_keys); bad = read_row(c->sel, ro.key[i], &ro.ex[i], &ro.v[i], &ro.u[i], &ro.pl[i], &ro.w[i]); }
    ro.snap = snap_epoch(c->db); mw_exec(c->db, "COMMIT");
    if (bad) { c->R->other++; return; }
    put_rec(c->fd, REC_RO, c->slot, 0, 0, 0, &ro, sizeof ro);
    *out = ro; *got = 1;
}
static void run_rw (ctx_t *c, rw_t *out, int *got) {
    rw_t tx; memset(&tx, 0, sizeof tx); tx.n = 1 + (int)(rnd(&c->rng) % MAXOPS); tx.slot = c->slot; tx.seq = c->seq + 1; *got = 0;
    sqlite3 *db = c->db; rec_t *R = c->R;
    if (mw_exec(db, "BEGIN") != SQLITE_OK) return;
    int ok = 1; char sql[200];
    for (int i = 0; i < tx.n && ok; i++) {
        op_t *o = &tx.op[i]; int again;
        do { again = 0; o->key = 1 + (int)(rnd(&c->rng) % (uint64_t)g_keys); for (int j = 0; j < i; j++) if (tx.op[j].key == o->key) again = 1; } while (again);       // (distinct keys: the reads and writes of a transaction do not overlap)
        if (read_row(c->sel, o->key, &o->sex, &o->sv, &o->su, &o->spl, &o->sw) != SQLITE_OK) { ok = 0; break; }
        uint64_t r = rnd(&c->rng) % 10;
        if (!o->sex) { o->kind = 4; o->a = (int)(rnd(&c->rng) % 1000); o->b = ucol(&c->rng); tx.ins_pl = (int)(rnd(&c->rng) % 400);
            if (o->b < 0) snprintf(sql, sizeof sql, "INSERT INTO t(id,v,u,p) VALUES(%d,%d,NULL,zeroblob(%d))", o->key, o->a, tx.ins_pl);
            else snprintf(sql, sizeof sql, "INSERT INTO t(id,v,u,p) VALUES(%d,%d,%d,zeroblob(%d))", o->key, o->a, o->b, tx.ins_pl);
            o->spl = tx.ins_pl; }
        else if (r < 3) { o->kind = 1; o->a = 1 + (int)(rnd(&c->rng) % 9); snprintf(sql, sizeof sql, "UPDATE t SET v = v + %d WHERE id = %d", o->a, o->key); }
        else if (r < 5) { o->kind = 2; snprintf(sql, sizeof sql, "DELETE FROM t WHERE id = %d", o->key); }
        else if (r < 7) { o->kind = 3; do o->b = ucol(&c->rng); while (o->b == o->su);       // (a change that changes nothing writes no page: the row would be read and not written)
            if (o->b < 0) snprintf(sql, sizeof sql, "UPDATE t SET u = NULL WHERE id = %d", o->key); else snprintf(sql, sizeof sql, "UPDATE t SET u = %d WHERE id = %d", o->b, o->key); }
        else if (r < 9) { o->kind = 5; do o->a = (int)(rnd(&c->rng) % 600); while (o->a == o->spl); snprintf(sql, sizeof sql, "UPDATE t SET p = zeroblob(%d) WHERE id = %d", o->a, o->key); }
        else { o->kind = 0; snprintf(sql, sizeof sql, "UPDATE t SET w = w + 1 WHERE id = %d", o->key); }
        int rc = mw_exec(db, sql);
        if (rc != SQLITE_OK) { ok = 0; if ((rc & 0xff) == SQLITE_FULL) put_rec(c->fd, REC_FULL, c->slot, 0, 0, 0, NULL, 0); if ((rc & 0xff) == SQLITE_CONSTRAINT) R->constraint++; else if ((rc & 0xff) == SQLITE_BUSY) R->busy++; else { R->other++; if (getenv("MW_VERBOSE")) printf("  other rc %d on: %s\n", rc, sql); } }
    }
    if (ok && c->slot >= 0) { snprintf(sql, sizeof sql, "UPDATE txlog SET seq = %llu WHERE slot = %d", (unsigned long long)tx.seq, c->slot); int rc = mw_exec(db, sql); if (rc != SQLITE_OK) { ok = 0; if ((rc & 0xff) == SQLITE_BUSY) R->busy++; else R->other++; } }
    if (ok) {
        tx.snap = snap_epoch(db);
        put_rec(c->fd, REC_INTENT, c->slot, tx.seq, 0, 0, &tx, sizeof tx);                              // (before COMMIT: a kill from here on leaves the transaction in doubt)
        int rc = mw_exec(db, "COMMIT");
        if (rc != SQLITE_OK) { ok = 0; if ((rc & 0xff) == SQLITE_FULL) put_rec(c->fd, REC_FULL, c->slot, 0, 0, 0, NULL, 0); if ((rc & 0xff) == SQLITE_BUSY) R->busy++; else { R->other++; if (getenv("MW_VERBOSE")) printf("  commit rc %d\n", rc); } }
        else tx.commit = commit_epoch(db, &tx.order);
    }
    if (!ok) { mw_exec(db, "ROLLBACK"); put_rec(c->fd, REC_ABORT, c->slot, tx.seq, 0, 0, NULL, 0); return; }
    if (tx.commit == 0) { R->other++; return; }                                                       // (it wrote no page: nothing to order)
    put_rec(c->fd, REC_COMMIT, c->slot, tx.seq, tx.commit, tx.order, NULL, 0);
    c->seq = tx.seq; *out = tx; *got = 1;
}

static void *worker (void *arg) {
    rec_t *R = arg; sqlite3 *db; if (open_db(g_path, &db) != SQLITE_OK) { mw_failures++; return NULL; }
    static _Atomic int ids; ctx_t c = { .db = db, .rng = 9001u + 7919u * (unsigned)atomic_fetch_add(&ids, 1), .slot = -1, .fd = -1, .R = R };
    sqlite3_prepare_v2(db, "SELECT v, u, length(p), w FROM t WHERE id = ?", -1, &c.sel, NULL);
    struct timespec t0; clock_gettime(CLOCK_MONOTONIC, &t0);
    for (;;) {
        struct timespec t1; clock_gettime(CLOCK_MONOTONIC, &t1);
        if ((double)(t1.tv_sec - t0.tv_sec) + (double)(t1.tv_nsec - t0.tv_nsec) / 1e9 > g_secs) break;
        int got;
        if (rnd(&c.rng) % 6 == 0) { ro_t ro; run_ro(&c, &ro, &got); if (got) { if (R->nro == R->capro) { R->capro = R->capro ? R->capro * 2 : 1024; R->ro = realloc(R->ro, R->capro * sizeof(ro_t)); } R->ro[R->nro++] = ro; } }
        else { rw_t tx; run_rw(&c, &tx, &got); if (got) { if (R->nrw == R->caprw) { R->caprw = R->caprw ? R->caprw * 2 : 1024; R->rw = realloc(R->rw, R->caprw * sizeof(rw_t)); } R->rw[R->nrw++] = tx; } }
    }
    sqlite3_finalize(c.sel); sqlite3_close(db); return NULL;
}

static void rec_name (char *fn, size_t n, const char *path, int slot);
// a process: runs until it is killed. It first says which of its sequence numbers the database has (RESOLVE: the transaction that the previous incarnation left in doubt has committed if its number is not above it).
static sqlite3 *g_child_db;
static void child_dump (int sig) {      // (SIGUSR1, debug: what this process counted)
    (void)sig; mw_db_stats st; memset(&st, 0, sizeof st); if (g_child_db) sqlite3_file_control(g_child_db, "main", MW_FCNTL_DBSTATS, &st);
    fprintf(stderr, "CHILD %d: commits %llu aborts %llu page_conflicts %llu read_conflicts %llu schema_conflicts %llu rebases %llu rebase_retries %llu unrebasable %llu relocations %llu\n", (int)getpid(), (unsigned long long)st.commits, (unsigned long long)st.aborts, (unsigned long long)st.page_conflicts, (unsigned long long)st.read_conflicts, (unsigned long long)st.schema_conflicts, (unsigned long long)st.rebases, (unsigned long long)st.rebase_retries, (unsigned long long)st.unrebasable, (unsigned long long)st.relocations);
}
static void slot_loop (int slot, int generation, int arm_crash) {
    sqlite3 *db; if (open_db(g_path, &db) != SQLITE_OK) _exit(2);
    g_child_db = db; signal(SIGUSR1, child_dump);
    char fn[300]; rec_name(fn, sizeof fn, g_path, slot);
    int fd = open(fn, O_WRONLY | O_APPEND | O_CREAT, 0644); if (fd < 0) _exit(2);
    rec_t R; memset(&R, 0, sizeof R);
    ctx_t c = { .db = db, .rng = 31337u + 7919u * (unsigned)slot + 104729u * (unsigned)generation, .slot = slot, .fd = fd, .R = &R };
    sqlite3_prepare_v2(db, "SELECT v, u, length(p), w FROM t WHERE id = ?", -1, &c.sel, NULL);
    sqlite3_stmt *q; char sql[100]; snprintf(sql, sizeof sql, "SELECT seq FROM txlog WHERE slot = %d", slot); sqlite3_prepare_v2(db, sql, -1, &q, NULL);
    if (sqlite3_step(q) != SQLITE_ROW) _exit(2);
    c.seq = (uint64_t)sqlite3_column_int64(q, 0); sqlite3_finalize(q);
    put_rec(fd, REC_RESOLVE, slot, c.seq, 0, 0, NULL, 0);
    if (arm_crash && !getenv("MW_SERIAL_NO_CRASH") && rnd(&c.rng) % 3 != 0) {                                                         // most incarnations also die on their own at a point of the publication (inside the lock, mid-record, just after it): the repair of a dead publisher
        static const mw_fault_t pts[] = { MW_CRASH_MID_LOG, MW_CRASH_BEFORE_LOG, MW_CRASH_SHARED_APPENDED, MW_CRASH_SHARED_INSTALLED, MW_CRASH_AFTER_LOG, MW_CRASH_AFTER_VISIBLE, MW_CRASH_SHARED_GC, MW_CRASH_SHARED_GC };       // (twice: the collection runs every 16 commits, and a dead holder in it is a case of its own)
        { size_t pi = rnd(&c.rng) % (sizeof pts / sizeof *pts); if (getenv("MW_SERIAL_PT")) pi = (size_t)atoi(getenv("MW_SERIAL_PT")); mw_fault_arm(pts[pi], 20 + (int)(rnd(&c.rng) % 800)); }
    }
    for (;;) {
        int got;
        if (rnd(&c.rng) % 6 == 0) { ro_t ro; run_ro(&c, &ro, &got); } else { rw_t tx; run_rw(&c, &tx, &got); }
    }
}
static void child_main (int slot, int generation) { slot_loop(slot, generation, 1); }
static void *slot_thread (void *arg) { slot_loop((int)(intptr_t)arg, 0, 0); return NULL; }

typedef struct { int ex, v, u, pl, w; } mrow;
static int same (int ex, int v, int u, int pl, int w, const mrow *m) { return ex == m->ex && (!ex || (v == m->v && u == m->u && pl == m->pl && w == m->w)); }
static int cmp_u64 (const void *a, const void *b) { uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b; return x < y ? -1 : x > y; }

typedef struct { rw_t **known; size_t nknown; rw_t **doubt; size_t ndoubt; ro_t **ro; size_t nro; } txset;      // known: commit epoch known (sorted by it); doubt: committed, epoch unknown (to be placed in a gap)
typedef struct { mrow *m; int *owner; int keys; } model;
static model new_model (int keys);
static int g_quiet;      // (while the placement of the transactions in doubt is searched, what is reported is not printed)
#define REPORT(counter, ...) do { if ((counter)++ < 4 && !g_quiet) { printf("    "); printf(__VA_ARGS__); printf("\n"); } } while (0)
typedef struct { long bad_read, bad_unique, bad_ro, bad_dup_epoch, bad_gap, bad_tail; } viol;

static int reads_fit (const rw_t *t, const model *M) { for (int k = 0; k < t->n; k++) { const op_t *o = &t->op[k]; if (!same(o->sex, o->sv, o->su, o->spl, o->sw, &M->m[o->key])) return 0; } return 1; }
static void apply_rw (const rw_t *t, uint64_t epoch, model *M, viol *V) {
    for (int k = 0; k < t->n; k++) { const op_t *o = &t->op[k]; const mrow *x = &M->m[o->key];
        if (!same(o->sex, o->sv, o->su, o->spl, o->sw, x)) REPORT(V->bad_read, "tx committed at %llu (snapshot %llu), key %d, op %d: saw (%d,%d,%d,%d,%d), model (%d,%d,%d,%d,%d)", (unsigned long long)epoch, (unsigned long long)t->snap, o->key, o->kind, o->sex, o->sv, o->su, o->spl, o->sw, x->ex, x->v, x->u, x->pl, x->w); }
    for (int k = 0; k < t->n; k++) { const op_t *o = &t->op[k]; mrow *x = &M->m[o->key];
        if (o->kind == 4 || o->kind == 3) { int nu = o->b; if (nu >= 0 && M->owner[nu] != -1 && M->owner[nu] != o->key) REPORT(V->bad_unique, "tx committed at %llu: u = %d already held by key %d", (unsigned long long)epoch, nu, M->owner[nu]); }
        switch (o->kind) {
            case 1: x->v += o->a; break;
            case 2: if (x->ex && x->u >= 0 && M->owner[x->u] == o->key) M->owner[x->u] = -1; x->ex = 0; x->v = x->u = x->pl = x->w = 0; break;
            case 3: if (x->u >= 0 && M->owner[x->u] == o->key) M->owner[x->u] = -1; x->u = o->b; if (o->b >= 0) M->owner[o->b] = o->key; break;
            case 4: x->ex = 1; x->v = o->a; x->u = o->b; x->pl = o->spl; x->w = 0; if (o->b >= 0) M->owner[o->b] = o->key; break;
            case 5: x->pl = o->a; break;
            default: x->w++; break;
        }
    }
}
static void check_ro (const ro_t *r, const model *M, viol *V) {
    for (int k = 0; k < r->n; k++) if (!same(r->ex[k], r->v[k], r->u[k], r->pl[k], r->w[k], &M->m[r->key[k]])) REPORT(V->bad_ro, "read-only at snapshot %llu: key %d saw (%d,%d,%d,%d,%d), model (%d,%d,%d,%d,%d)", (unsigned long long)r->snap, r->key[k], r->ex[k], r->v[k], r->u[k], r->pl[k], r->w[k], M->m[r->key[k]].ex, M->m[r->key[k]].v, M->m[r->key[k]].u, M->m[r->key[k]].pl, M->m[r->key[k]].w);
}
static int cmp_known (const void *a, const void *b) { const rw_t *p = *(rw_t *const *)a, *q = *(rw_t *const *)b; if (p->commit != q->commit) return p->commit < q->commit ? -1 : 1; return p->order < q->order ? -1 : p->order > q->order; }
static int cmp_ro (const void *a, const void *b) { uint64_t x = (*(ro_t *const *)a)->snap, y = (*(ro_t *const *)b)->snap; return x < y ? -1 : x > y; }
static int cmp_doubt (const void *a, const void *b) { const rw_t *p = *(rw_t *const *)a, *q = *(rw_t *const *)b; if (p->snap != q->snap) return p->snap < q->snap ? -1 : 1; if (p->slot != q->slot) return p->slot < q->slot ? -1 : 1; return p->seq < q->seq ? -1 : p->seq > q->seq; }      // (a total order: replay() sorts again for every try, and qsort is not stable: two in doubt with the same snapshot must always come in the same order)

// Replays the committed transactions in the order of the epochs. The ones in doubt take the epochs that no recorded transaction has (and the ones after the last), the first of them whose reads fit the model.
static int g_stop, g_aborted;      // while the placements are searched, a try stops at its first violation: the wrong choice is one of the latest ones
static int viol_total (const viol *V);
static int g_choice[256], g_nchoice, g_cand[256], g_nenc;      // the placements of the transactions in doubt that have more than one candidate: which one this try takes, and how many there were
static void replay_once (txset *S, model *M, viol *V) {
    g_nenc = 0; g_aborted = 0;
    if (getenv("MW_SERIAL_DUMP") && !g_quiet) { printf("  [known: %zu, first 14 by epoch:", S->nknown); for (size_t i = 0; i < S->nknown && i < 14; i++) printf(" (e%llu s%d q%llu snap%llu n%d)", (unsigned long long)S->known[i]->commit, S->known[i]->slot, (unsigned long long)S->known[i]->seq, (unsigned long long)S->known[i]->snap, S->known[i]->n); printf("]\n  [doubt (committed): %zu:", S->ndoubt); for (size_t i = 0; i < S->ndoubt && i < 10; i++) printf(" (s%d q%llu snap%llu n%d)", S->doubt[i]->slot, (unsigned long long)S->doubt[i]->seq, (unsigned long long)S->doubt[i]->snap, S->doubt[i]->n); printf("]\n"); }
    qsort(S->known, S->nknown, sizeof *S->known, cmp_known); qsort(S->ro, S->nro, sizeof *S->ro, cmp_ro); qsort(S->doubt, S->ndoubt, sizeof *S->doubt, cmp_doubt);
    char *used = calloc(S->ndoubt + 1, 1); size_t ri = 0, ki = 0, ndone = 0;
    uint64_t e = 2;                                                                       // (the first commit of a database is at epoch 2: what is before the first recorded commit can be one in doubt too)
    if (S->nknown && S->known[0]->commit < e) e = S->known[0]->commit;
    for (;;) {
        int have_known = ki < S->nknown;
        if (g_stop && viol_total(V)) { g_aborted = 1; free(used); return; }
        if (!have_known && ndone == S->ndoubt) break;
        while (ri < S->nro && S->ro[ri]->snap < e) check_ro(S->ro[ri++], M, V);                  // (read-only transactions whose snapshot lies before this epoch saw the state so far)
        if (have_known && S->known[ki]->commit == e) {
            if (ki && S->known[ki - 1]->commit == e && S->known[ki - 1]->order >= S->known[ki]->order) REPORT(V->bad_dup_epoch, "two commits at epoch %llu with the same place in it", (unsigned long long)e);
            apply_rw(S->known[ki], e, M, V); ki++;
            if (!(ki < S->nknown && S->known[ki]->commit == e)) e++;                      // (the commits of one epoch, a group replay: one after the other in their order)
            continue;
        }
        if (have_known && S->known[ki]->commit < e) { REPORT(V->bad_dup_epoch, "two commits at epoch %llu", (unsigned long long)S->known[ki]->commit); apply_rw(S->known[ki], S->known[ki]->commit, M, V); ki++; continue; }
        // an epoch that no recorded transaction has (inside the recorded range, or after it)
        size_t pick = (size_t)-1;
        { size_t cl[64]; int nc = 0;
          for (size_t d = 0; d < S->ndoubt && nc < 64; d++) if (!used[d] && S->doubt[d]->snap < e && reads_fit(S->doubt[d], M)) cl[nc++] = d;
          if (nc) { int k = g_nenc++; int c = k < g_nchoice ? g_choice[k] : 0; if (c >= nc) c = nc - 1; if (k < 256) g_cand[k] = nc; pick = cl[c]; } }
        if (pick == (size_t)-1) {
            if (getenv("MW_SERIAL_DEBUG")) { printf("  [gap epoch %llu] doubts left:", (unsigned long long)e); for (size_t d = 0; d < S->ndoubt; d++) if (!used[d]) printf(" (slot %d seq %llu snap %llu n %d fit %d)", S->doubt[d]->slot, (unsigned long long)S->doubt[d]->seq, (unsigned long long)S->doubt[d]->snap, S->doubt[d]->n, reads_fit(S->doubt[d], M)); printf("\n"); }
            if (have_known) { REPORT(V->bad_gap, "epoch %llu has no recorded commit and no transaction in doubt fits it (a commit that nobody accounts for, or a lost one)", (unsigned long long)e); e++; continue; }
            // after the last recorded commit: what is left is in doubt and its place is unknown; each must still fit somewhere: take them in order of their snapshots
            for (size_t d = 0; d < S->ndoubt; d++) if (!used[d]) { REPORT(V->bad_tail, "transaction in doubt (snapshot %llu, slot %d seq %llu) committed but fits no epoch", (unsigned long long)S->doubt[d]->snap, S->doubt[d]->slot, (unsigned long long)S->doubt[d]->seq); used[d] = 1; ndone++; }
            break;
        }
        used[pick] = 1; ndone++; if (getenv("MW_SERIAL_DEBUG")) printf("  [doubt placed] slot %d seq %llu snap %llu -> epoch %llu (nops %d)\n", S->doubt[pick]->slot, (unsigned long long)S->doubt[pick]->seq, (unsigned long long)S->doubt[pick]->snap, (unsigned long long)e, S->doubt[pick]->n); apply_rw(S->doubt[pick], e, M, V);
        if (g_threadmode && g_rebase) for (;;) {                                                   // (the threads of one process can have committed together, at one epoch: the ones that fit and began before it)
            size_t more = (size_t)-1; for (size_t d = 0; d < S->ndoubt; d++) if (!used[d] && S->doubt[d]->snap < e && reads_fit(S->doubt[d], M)) { more = d; break; }
            if (more == (size_t)-1) break;
            used[more] = 1; ndone++; apply_rw(S->doubt[more], e, M, V);
        }
        e++;
    }
    while (ri < S->nro) check_ro(S->ro[ri++], M, V);
    free(used);
}

// Two transactions that were in doubt can both fit the same epoch (a process killed while another was killed a moment ago): taking the first one is a guess. The placements are searched (the
// first try is that guess) for one with no violation; if there is none the violations of the first try are what is reported.
static int viol_total (const viol *V) { return (int)(V->bad_read + V->bad_unique + V->bad_ro + V->bad_dup_epoch + V->bad_gap + V->bad_tail); }
static void replay (txset *S, model *M, viol *V) {
    int keys = M->keys, best_choice[256], best_n = 0, tries = 0; int cand[256], nenc = 0;
    g_nchoice = 0; g_quiet = 1; g_stop = 1;
    model B = new_model(keys); viol VB; memset(&VB, 0, sizeof VB); replay_once(S, &B, &VB); tries++;
    int first_total = viol_total(&VB); nenc = g_nenc < 256 ? g_nenc : 256; memcpy(cand, g_cand, sizeof cand);
    int found = first_total == 0 && !g_aborted; memcpy(best_choice, g_choice, sizeof best_choice);
    int choice[256] = {0};
    while (!found && tries < 5000) {                                                                     // (the next vector of choices, like an odometer over the ambiguous placements)
        int k = nenc - 1; while (k >= 0 && choice[k] + 1 >= cand[k]) k--;
        if (k < 0) break;
        choice[k]++; for (int j = k + 1; j < 256; j++) choice[j] = 0;
        memcpy(g_choice, choice, sizeof choice); g_nchoice = 256;
        model T = new_model(keys); viol VT; memset(&VT, 0, sizeof VT); replay_once(S, &T, &VT); tries++;
        nenc = g_nenc < 256 ? g_nenc : 256; memcpy(cand, g_cand, sizeof cand);
        if (viol_total(&VT) == 0 && !g_aborted) { found = 1; memcpy(best_choice, choice, sizeof choice); best_n = 256; free(T.m); free(T.owner); break; }
        free(T.m); free(T.owner);
    }
    free(B.m); free(B.owner);
    g_quiet = 0; g_stop = 0;
    if (found && best_n) { memcpy(g_choice, best_choice, sizeof best_choice); g_nchoice = best_n; } else g_nchoice = 0;     // (the placement that is reported and whose model is kept)
    if (getenv("MW_SERIAL_DEBUG") || tries > 1 || !found) printf("  [placement of the transactions in doubt: %d tries, %s]\n", tries, found ? (best_n ? "another placement than the first fits" : "the first fits") : "none fits");
    replay_once(S, M, V);
}

static int final_check (const char *path, model *M, long *rows_out, long *model_rows_out, long *bad_final, long *dup, int *integ) {
    sqlite3 *chk; if (open_db(path, &chk) != SQLITE_OK) { mw_failures++; return -1; }
    long rows = 0; sqlite3_stmt *st; sqlite3_prepare_v2(chk, "SELECT id, v, u, length(p), w FROM t NOT INDEXED ORDER BY id", -1, &st, NULL);
    char *seen = calloc((size_t)M->keys + 2, 1); *bad_final = 0;
    while (sqlite3_step(st) == SQLITE_ROW) {
        int id = sqlite3_column_int(st, 0), v = sqlite3_column_int(st, 1), u = sqlite3_column_type(st, 2) == SQLITE_NULL ? -1 : sqlite3_column_int(st, 2), pl = sqlite3_column_int(st, 3), w = sqlite3_column_int(st, 4); rows++;
        if (id < 1 || id > M->keys || !same(1, v, u, pl, w, &M->m[id])) REPORT(*bad_final, "final row %d = (%d,%d,%d,%d) differs from the model", id, v, u, pl, w); else seen[id] = 1;
    }
    sqlite3_finalize(st);
    long model_rows = 0; for (int k = 1; k <= M->keys; k++) if (M->m[k].ex) { model_rows++; if (!seen[k]) REPORT(*bad_final, "key %d is in the model and not in the table", k); }
    free(seen);
    *dup = 0; sqlite3_prepare_v2(chk, "SELECT count(*) FROM (SELECT u FROM t NOT INDEXED WHERE u IS NOT NULL GROUP BY u HAVING count(*) > 1)", -1, &st, NULL); if (sqlite3_step(st) == SQLITE_ROW) *dup = sqlite3_column_int(st, 0); sqlite3_finalize(st);
    *integ = 0; sqlite3_prepare_v2(chk, "PRAGMA integrity_check", -1, &st, NULL); if (sqlite3_step(st) == SQLITE_ROW) *integ = strcmp((const char *)sqlite3_column_text(st, 0), "ok") == 0; sqlite3_finalize(st);
    *rows_out = rows; *model_rows_out = model_rows; sqlite3_close(chk); return 0;
}

static void make_db (const char *path, int nslots) {
    { char fn[400]; for (int slot = 0; slot < 32; slot++) { snprintf(fn, sizeof fn, "%s.rec%d", path, slot); unlink(fn); } }      // (the record files of a run that was killed: a process number that is used again gives the same path, and its records would be replayed into this run)
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER NOT NULL, u INTEGER UNIQUE, p BLOB NOT NULL, w INTEGER NOT NULL DEFAULT 0); CREATE TABLE txlog(slot INTEGER PRIMARY KEY, seq INTEGER NOT NULL, pad BLOB NOT NULL)"), SQLITE_OK);
    for (int i = 0; i < nslots; i++) { char q[120]; snprintf(q, sizeof q, "INSERT INTO txlog VALUES(%d, 0, zeroblob(3500))", i); CHECK_RC(mw_exec(s, q), SQLITE_OK); }          // (a row a page: the processes never conflict on it)
    sqlite3_close(s);
}
static model new_model (int keys) { model M = { calloc((size_t)keys + 2, sizeof(mrow)), malloc((size_t)(2 * keys + 4) * sizeof(int)), keys }; for (int i = 0; i < 2 * keys + 4; i++) M.owner[i] = -1; return M; }

static int skip (const char *name) { const char *o = getenv("MW_SERIAL_ONLY"); return o && !strstr(name, o); }
static void run_threads (const char *name, int keys, int threads, double secs) {
    if (skip(name)) return;
    char path[256]; mw_tmpdb(path, sizeof path, "serial"); g_procs_mode = 0; make_db(path, 0);
    g_path = path; g_keys = keys; g_secs = secs;
    sqlite3 *obs; CHECK_RC(open_db(path, &obs), SQLITE_OK);                                               // (kept open: the statistics belong to the database object, which lives while a connection does)
    rec_t *R = calloc((size_t)threads, sizeof *R); pthread_t th[64];
    for (int i = 0; i < threads; i++) pthread_create(&th[i], NULL, worker, &R[i]);
    for (int i = 0; i < threads; i++) pthread_join(th[i], NULL);
    mw_db_stats ds; memset(&ds, 0, sizeof ds); sqlite3_file_control(obs, "main", MW_FCNTL_DBSTATS, &ds); sqlite3_close(obs);
    txset S = {0}; size_t nrw = 0, nro = 0; long busy = 0, cons = 0, oth = 0;
    for (int i = 0; i < threads; i++) { nrw += R[i].nrw; nro += R[i].nro; busy += R[i].busy; cons += R[i].constraint; oth += R[i].other; }
    S.known = malloc((nrw + 1) * sizeof *S.known); S.ro = malloc((nro + 1) * sizeof *S.ro);
    for (int i = 0; i < threads; i++) { for (size_t k = 0; k < R[i].nrw; k++) S.known[S.nknown++] = &R[i].rw[k]; for (size_t k = 0; k < R[i].nro; k++) S.ro[S.nro++] = &R[i].ro[k]; }
    model M = new_model(keys); viol V; memset(&V, 0, sizeof V); replay(&S, &M, &V);
    long rows, model_rows, bad_final, dup; int integ; final_check(path, &M, &rows, &model_rows, &bad_final, &dup, &integ);
    printf("%s: %d keys, %d threads: %zu read-write and %zu read-only transactions committed, %ld refused (busy), %ld constraint errors, %ld other; page conflicts %llu, relocations %llu, merges %llu, rebases %llu (%llu lost races, %llu refused to replay); table %ld rows (model %ld)\n",
           name, keys, threads, nrw, nro, busy, cons, oth, (unsigned long long)ds.page_conflicts, (unsigned long long)ds.relocations, (unsigned long long)ds.merges, (unsigned long long)ds.rebases, (unsigned long long)ds.rebase_retries, (unsigned long long)ds.unrebasable, rows, model_rows);
    printf("   reads that differ from the serial order %ld, UNIQUE broken %ld, read-only inconsistent %ld, duplicate epochs %ld, final differs %ld, duplicate u in the table %ld, integrity %s\n", V.bad_read, V.bad_unique, V.bad_ro, V.bad_dup_epoch, bad_final, dup, integ ? "ok" : "BAD");
    CHECK(V.bad_read == 0); CHECK(V.bad_unique == 0); CHECK(V.bad_ro == 0); CHECK(V.bad_dup_epoch == 0); CHECK(bad_final == 0); CHECK(dup == 0); CHECK(integ); CHECK(rows == model_rows);
    CHECK(nrw >= 100);                                                                                // (not vacuous: on an idle machine there are ten thousand; with 24 copies of the test at once the hot rows with the rebase give a few hundred)
    if (g_rebase) CHECK(ds.rebases > 0);                                               // (and the rebase did take part)
    for (int i = 0; i < threads; i++) { free(R[i].rw); free(R[i].ro); } free(R); free(S.known); free(S.ro); free(M.m); free(M.owner);
    mw_rmdb(path);
}

// ---- real processes killed with SIGKILL ----
static const char *g_recdir;                                                                              // where the record files are (default: next to the database; for the power-loss test: a directory that the loss does not touch)
static void rec_name (char *fn, size_t n, const char *path, int slot) { if (g_recdir) snprintf(fn, n, "%s/rec%d", g_recdir, slot); else snprintf(fn, n, "%s.rec%d", path, slot); }

// Starts the processes, kills some of them at random moments while others die at crash points, for `secs`. cut_cmd: the end is a loss of power instead of a normal one: all processes are stopped (nothing is acknowledged
// after that), the command runs (it cuts the power of the disk), then they are killed.
static void procs_run (const char *path, int keys, int nprocs, double secs, int kill_ms, const char *idx_entries, const char *cut_cmd, int *kills_out, int *crashes_out) {
    if (getenv("MW_SERIAL_KILL_MS")) kill_ms = atoi(getenv("MW_SERIAL_KILL_MS"));
    if (idx_entries) setenv("MW_IDX_ENTRIES", idx_entries, 1); else unsetenv("MW_IDX_ENTRIES");
    g_procs_mode = 1; g_path = path; g_keys = keys; g_secs = secs;
    pid_t pid[32]; int gen[32]; struct timespec t0; clock_gettime(CLOCK_MONOTONIC, &t0);
    for (int i = 0; i < nprocs; i++) { gen[i] = 0; pid[i] = fork(); if (pid[i] == 0) child_main(i, 0); }
    unsigned rng = 555u; int kills = 0, crashes = 0; double next_kill = (double)kill_ms / 1000.0; int sampled = 0; long long last_tot = -2; double last_move = 0, last_poll = 0;
    for (;;) {
        struct timespec t1; clock_gettime(CLOCK_MONOTONIC, &t1);
        double el = (double)(t1.tv_sec - t0.tv_sec) + (double)(t1.tv_nsec - t0.tv_nsec) / 1e9;
        if (el > secs) break;
        usleep(5000);
        if (getenv("MW_SERIAL_STALL_SAMPLE") && !sampled && el > 0.5 && el - last_poll > 0.5) {      // (a stall of the writers: what the processes committed does not grow for 2 s: the header of the shared state and the stacks of the processes are taken, once)
            last_poll = el; sqlite3 *pdb; long long tot = -1;
            if (open_db(path, &pdb) == SQLITE_OK) {
                sqlite3_stmt *q; if (sqlite3_prepare_v2(pdb, "SELECT sum(seq) FROM txlog", -1, &q, NULL) == SQLITE_OK && sqlite3_step(q) == SQLITE_ROW) tot = sqlite3_column_int64(q, 0); sqlite3_finalize(q);
                if (getenv("MW_SERIAL_STALL_TRACE")) { fprintf(stderr, "[t=%.2f seq-sum=%lld] ", el, tot); sqlite3_file_control(pdb, "main", 0x4d570010, NULL); }
                if (tot == last_tot && el - last_move > 2.0) { sampled = 1; sqlite3_file_control(pdb, "main", 0x4d570010, NULL); for (int i = 0; i < nprocs; i++) kill(pid[i], SIGUSR1); usleep(300000); fflush(stderr); }
                sqlite3_close(pdb);
            }
            if (tot != last_tot) { last_tot = tot; last_move = el; }
            else if (sampled) { for (int i = 0; i < nprocs; i++) { char cmd[300]; snprintf(cmd, sizeof cmd, "sample %d 1 -file /tmp/sstack_%d_%d.txt >/dev/null 2>&1", (int)pid[i], (int)getpid(), i); (void)!system(cmd); } printf("  [stall: stacks in /tmp/sstack_%d_*.txt]\n", (int)getpid()); }
        }
        for (int i = 0; i < nprocs; i++) {                                                           // a process that died by itself (a crash point): replaced
            int st; if (waitpid(pid[i], &st, WNOHANG) == pid[i]) { crashes++; gen[i]++; pid[i] = fork(); if (pid[i] == 0) child_main(i, gen[i]); }
        }
        if (el > next_kill && !getenv("MW_SERIAL_NO_KILL")) {                                                                        // and now and then one is killed from outside, at whatever moment it is in
            int v = (int)(rnd(&rng) % (uint64_t)nprocs); kill(pid[v], SIGKILL); int st; waitpid(pid[v], &st, 0); kills++;
            gen[v]++; pid[v] = fork(); if (pid[v] == 0) child_main(v, gen[v]);
            next_kill = el + (double)(kill_ms / 2 + (int)(rnd(&rng) % (uint64_t)kill_ms)) / 1000.0;
        }
    }
    if (cut_cmd) {
        for (int i = 0; i < nprocs; i++) kill(pid[i], SIGSTOP);
        for (int i = 0; i < nprocs; i++) { int st; waitpid(pid[i], &st, WUNTRACED); }                  // (stopped: whatever they were doing, nothing more is acknowledged)
        printf("power cut: %s\n", cut_cmd); fflush(stdout);
        int rc = system(cut_cmd); CHECK(rc == 0);
    }
    for (int i = 0; i < nprocs; i++) { kill(pid[i], SIGKILL); int st; waitpid(pid[i], &st, 0); }
    *kills_out = kills; *crashes_out = crashes;
}

static void procs_verify (const char *name, const char *path, int keys, int nprocs, int kills, int crashes, int min_events) {
    g_procs_mode = 1; g_path = path; g_keys = keys;
    // what the database has of each process's transactions
    sqlite3 *chk; int orc = open_db(path, &chk);
    if (orc != SQLITE_OK) {                                                                              // (forensics: what the files were when the open failed)
        printf("  [open of %s failed: rc %d: %s]\n", path, orc, chk ? sqlite3_errmsg(chk) : "?"); fflush(stdout);
        char cmd[900]; snprintf(cmd, sizeof cmd, "ls -la %s* 2>&1 | head -20; head -c 100 %s | xxd | head -4", path, path); (void)!system(cmd);
    }
    CHECK_RC(orc, SQLITE_OK);                                              // (after a loss of power this open is the recovery)
    uint64_t final_seq[32] = {0}; for (int i = 0; i < nprocs; i++) { char q[100]; snprintf(q, sizeof q, "SELECT seq FROM txlog WHERE slot = %d", i); sqlite3_stmt *st; sqlite3_prepare_v2(chk, q, -1, &st, NULL); if (sqlite3_step(st) == SQLITE_ROW) final_seq[i] = (uint64_t)sqlite3_column_int64(st, 0); sqlite3_finalize(st); }
    sqlite3_close(chk);
    // the record files
    txset S = {0}; size_t cap_k = 1 << 16, cap_d = 1 << 12, cap_r = 1 << 16; S.known = malloc(cap_k * sizeof *S.known); S.doubt = malloc(cap_d * sizeof *S.doubt); S.ro = malloc(cap_r * sizeof *S.ro);
    long committed = 0, in_doubt = 0, in_doubt_committed = 0, full_errors = 0;
    for (int slot = 0; slot < nprocs; slot++) {
        char fn[300]; rec_name(fn, sizeof fn, path, slot); FILE *f = fopen(fn, "rb"); if (!f) continue;
        rw_t *pending = NULL; rhdr h;
        while (fread(&h, sizeof h, 1, f) == 1) {
            if (h.type == REC_INTENT) { rw_t *t = malloc(sizeof *t); if (fread(t, sizeof *t, 1, f) != 1) { free(t); break; } if (pending) free(pending); pending = t; }
            else if (h.type == REC_RO) { ro_t *r = malloc(sizeof *r); if (fread(r, sizeof *r, 1, f) != 1) { free(r); break; } if (S.nro == cap_r) { cap_r *= 2; S.ro = realloc(S.ro, cap_r * sizeof *S.ro); } S.ro[S.nro++] = r; }
            else if (h.type == REC_COMMIT) { if (pending && pending->seq == h.seq) { pending->commit = h.epoch; pending->order = h.order; if (S.nknown == cap_k) { cap_k *= 2; S.known = realloc(S.known, cap_k * sizeof *S.known); } S.known[S.nknown++] = pending; pending = NULL; committed++; } }
            else if (h.type == REC_ABORT) { free(pending); pending = NULL; }
            else if (h.type == REC_FULL) full_errors++;
            else if (h.type == REC_RESOLVE) {
                if (pending) { in_doubt++; if (pending->seq <= h.seq) { if (S.ndoubt == cap_d) { cap_d *= 2; S.doubt = realloc(S.doubt, cap_d * sizeof *S.doubt); } S.doubt[S.ndoubt++] = pending; in_doubt_committed++; } else free(pending); pending = NULL; }
            }
        }
        if (pending) { in_doubt++; if (pending->seq <= final_seq[slot]) { if (S.ndoubt == cap_d) { cap_d *= 2; S.doubt = realloc(S.doubt, cap_d * sizeof *S.doubt); } S.doubt[S.ndoubt++] = pending; in_doubt_committed++; } else free(pending); }
        fclose(f); unlink(fn);
    }
    model M = new_model(keys); viol V; memset(&V, 0, sizeof V); replay(&S, &M, &V);
    long rows, model_rows, bad_final, dup; int integ; final_check(path, &M, &rows, &model_rows, &bad_final, &dup, &integ);
    // the sequence numbers: what the database says each process committed last is what the records say (an acknowledged transaction that the database does not have shows here)
    long bad_seq = 0; for (int slot = 0; slot < nprocs; slot++) { uint64_t mx = 0; for (size_t i = 0; i < S.nknown; i++) if (S.known[i]->slot == slot && S.known[i]->seq > mx) mx = S.known[i]->seq; for (size_t i = 0; i < S.ndoubt; i++) if (S.doubt[i]->slot == slot && S.doubt[i]->seq > mx) mx = S.doubt[i]->seq; if (mx != final_seq[slot]) REPORT(bad_seq, "process %d: the database's last sequence number is %llu, the records' %llu", slot, (unsigned long long)final_seq[slot], (unsigned long long)mx); }
    printf("%s: %d keys, %d processes, %d kills and %d crashes at a publication point: %ld transactions acknowledged, %ld read-only; %ld in doubt after a kill, %ld of them committed; table %ld rows (model %ld); commits refused with SQLITE_FULL (the index of versions had no room): %ld\n", name, keys, nprocs, kills, crashes, committed, (long)S.nro, in_doubt, in_doubt_committed, rows, model_rows, full_errors);
    printf("   reads that differ from the serial order %ld, UNIQUE broken %ld, read-only inconsistent %ld, duplicate epochs %ld, epochs with nobody to account for them %ld, in doubt that fit no epoch %ld, final differs %ld, duplicate u %ld, sequence numbers %ld, integrity %s\n",
           V.bad_read, V.bad_unique, V.bad_ro, V.bad_dup_epoch, V.bad_gap, V.bad_tail, bad_final, dup, bad_seq, integ ? "ok" : "BAD");
    CHECK(V.bad_read == 0); CHECK(V.bad_unique == 0); CHECK(V.bad_ro == 0); CHECK(V.bad_dup_epoch == 0); CHECK(V.bad_gap == 0); CHECK(V.bad_tail == 0); CHECK(bad_final == 0); CHECK(dup == 0); CHECK(bad_seq == 0); CHECK(integ); CHECK(rows == model_rows);
    CHECK(committed >= 500); CHECK(kills + crashes >= min_events); CHECK(full_errors < 500);                       // (the index of versions is collected: a holder that died in a collection must not leave pages that nobody collects; a compactor that dies leaves the index full until another one takes its claim over: a few commits are refused, not hundreds: 2 s of them was the bug)
    free(S.known); free(S.doubt); free(S.ro); free(M.m); free(M.owner);
}

static void run_procs (const char *name, int keys, int nprocs, double secs, int kill_ms, const char *idx_entries) {
    if (skip(name)) return;
    char path[256]; mw_tmpdb(path, sizeof path, "serialp"); g_procs_mode = 1; make_db(path, nprocs);
    int kills, crashes; procs_run(path, keys, nprocs, secs, kill_ms, idx_entries, NULL, &kills, &crashes);
    unsetenv("MW_IDX_ENTRIES");                                    // (the small index is for this scenario only: the verification and the scenarios that follow, with threads in the shared mode, use the default one)
    procs_verify(name, path, keys, nprocs, kills, crashes, 10);
    mw_rmdb(path);
}

// Power-loss test (test/power/run.sh): the database is on a disk with a volatile write cache that is cut at the end of the run phase; the verify phase runs on what the disk kept.
//   MW_SERIAL_MODE=threads: one process with a thread for each slot instead of the processes mode.   MW_SERIAL_PHASE=run|verify  MW_SERIAL_DB=<path>  MW_SERIAL_REC=<dir that the loss does not touch>  [MW_SERIAL_KEYS=300 MW_SERIAL_PROCS=6 MW_SERIAL_SECS=6]  MW_SERIAL_CUT_CMD=<command>
static int power_phase (const char *phase) {
    g_rebase = getenv("MW_SERIAL_REBASE") != NULL && *getenv("MW_SERIAL_REBASE");
    g_threadmode = getenv("MW_SERIAL_MODE") && !strcmp(getenv("MW_SERIAL_MODE"), "threads");
    const char *path = getenv("MW_SERIAL_DB"); g_recdir = getenv("MW_SERIAL_REC");
    if (!path || !g_recdir) { printf("MW_SERIAL_DB and MW_SERIAL_REC are needed\n"); return 2; }
    int keys = getenv("MW_SERIAL_KEYS") ? atoi(getenv("MW_SERIAL_KEYS")) : 300, nprocs = getenv("MW_SERIAL_PROCS") ? atoi(getenv("MW_SERIAL_PROCS")) : 6; double secs = getenv("MW_SERIAL_SECS") ? atof(getenv("MW_SERIAL_SECS")) : 6.0;
    if (!strcmp(phase, "run")) {
        g_procs_mode = 1; make_db(path, nprocs); sync();                                                   // (the database exists on the disk before the run)
        int kills = 0, crashes = 0;
        if (g_threadmode) {                                                                                 // one process, a thread for each slot (the engine's single-process mode: the staged log of the process)
            g_path = path; g_keys = keys; g_secs = secs;
            pid_t pid = fork();
            if (pid == 0) { pthread_t th[32]; for (int i = 0; i < nprocs; i++) pthread_create(&th[i], NULL, slot_thread, (void *)(intptr_t)i); for (;;) pause(); }
            usleep((useconds_t)(secs * 1e6));
            kill(pid, SIGSTOP); int st; waitpid(pid, &st, WUNTRACED);
            const char *cut = getenv("MW_SERIAL_CUT_CMD"); if (cut) { printf("power cut: %s\n", cut); fflush(stdout); CHECK(system(cut) == 0); }
            kill(pid, SIGKILL); waitpid(pid, &st, 0);
        } else procs_run(path, keys, nprocs, secs, 250, NULL, getenv("MW_SERIAL_CUT_CMD"), &kills, &crashes);
        printf("run phase: %d kills, %d crashes at a publication point, then the power cut\n", kills, crashes);
        char fn[400]; snprintf(fn, sizeof fn, "%s/events", g_recdir); FILE *f = fopen(fn, "w"); if (f) { fprintf(f, "%d %d\n", kills, crashes); fclose(f); }
    } else {
        int kills = 0, crashes = 0; char fn[400]; snprintf(fn, sizeof fn, "%s/events", g_recdir); FILE *f = fopen(fn, "r"); if (f) { if (fscanf(f, "%d %d", &kills, &crashes) != 2) kills = crashes = 0; fclose(f); }
        procs_verify("after the loss of power", path, keys, nprocs, kills, crashes, 0);
    }
    MW_DONE();
}

int main (void) {
    setvbuf(stdout, NULL, _IOLBF, 0);
    if (getenv("MW_SERIAL_PHASE")) return power_phase(getenv("MW_SERIAL_PHASE"));
    g_rebase = getenv("MW_TEST_REBASE") != NULL;                                                          // (MW_TEST_REBASE=1: only the runs with the rebase)
    for (int pass = g_rebase ? 1 : 0; pass < 2; pass++) {
        g_rebase = pass;
        const char *suffix = pass ? " + rebase" : "";
        char nm[100];
        snprintf(nm, sizeof nm, "hot (few keys, many conflicts)%s", suffix); run_threads(nm, 12, 8, 3.0);
        snprintf(nm, sizeof nm, "medium%s", suffix); run_threads(nm, 200, 8, 3.0);
        snprintf(nm, sizeof nm, "wide (many pages, pages freed and reused)%s", suffix); run_threads(nm, 3000, 8, 4.0);
        if (!pass) {
            run_procs("processes, hot", 12, 6, 6.0, 250, NULL);
            run_procs("processes, medium", 300, 6, 6.0, 250, NULL);
            run_procs("processes, wide", 3000, 6, 6.0, 250, NULL);
            run_procs("processes, small index of versions", 300, 6, 10.0, 250, "3000");
        } else {
            run_procs("processes, hot + rebase", 12, 6, 6.0, 250, NULL);
            run_procs("processes, medium + rebase", 300, 6, 6.0, 250, NULL);
        }
    }
    MW_DONE();
}
