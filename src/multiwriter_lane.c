//
//  multiwriter_lane.c
//  sqlite-multiwriter
//
//  Private write lanes. In private-lane mode ("mw=2") each connection gets:
//    - its own in-memory "-wal" (no real file): SQLite believes it is the only writer, so
//      there is no WAL write-lock contention between connections;
//    - its own private wal-index (xShmMap memory) and no-op shm locks;
//    - a main-database file whose writes are swallowed (the real file is only ever written
//      by the compactor).
//  The physical write set of a transaction is the set of page numbers of the frames
//  SQLite appended to the private WAL up to the commit frame. When the snapshot ends the
//  private WAL is emptied and the wal-index header invalidated: SQLite's own recovery path
//  then rebuilds an empty WAL and reports "changed", which resets the page cache. Every
//  transaction therefore starts with a cold page cache; this is also what makes the set of
//  pages read through xRead a complete read-set.
//

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>
#include "multiwriter_os.h"
#include "multiwriter_io.h"
#include "multiwriter_internal.h"
#include "multiwriter_seglog.h"

#define MW_HOT_CREDIT 16     // serialised transactions after a conflict, decaying by one per successful commit

static _Thread_local mw_lane *tls_lane;      // lane whose main file this thread touched last (pairs a new WAL with its lane)
static sqlite3_io_methods lane_main_io;
static sqlite3_io_methods lane_wal_io;
static const sqlite3_io_methods *pass_io;

#define FILE_LANE(pf) (((mw_file *)(pf))->lane)
#define REAL(pf)      (((mw_file *)(pf))->real)

static uint32_t be32 (const uint8_t *p) { return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | p[3]; }

typedef struct { uint32_t pg; uint64_t ep; } pe_t;
static int cmp_pe (const void *a, const void *b) { const pe_t *x = a, *y = b; if (x->pg != y->pg) return x->pg < y->pg ? -1 : 1; return x->ep > y->ep ? -1 : x->ep < y->ep; }
static int cmp_u32 (const void *a, const void *b) { uint32_t x = *(const uint32_t *)a, y = *(const uint32_t *)b; return x < y ? -1 : x > y; }

// MARK: - write set -

// Adds `pgno` (image in frame `frame`) to the write set; a later frame of the same page supersedes.
static inline uint32_t ws_slot (uint32_t pgno, uint32_t cap) { return (pgno * 2654435761u) & (cap - 1); }
static int ws_hash_rebuild (mw_lane *lane, uint32_t cap) {
    int *h = sqlite3_malloc64((sqlite3_uint64)cap * sizeof(int));
    if (!h) return SQLITE_NOMEM;
    memset(h, 0, (size_t)cap * sizeof(int));
    for (int i = 0; i < lane->ws_n; i++) { uint32_t k = ws_slot(lane->ws_pgnos[i], cap); while (h[k]) k = (k + 1) & (cap - 1); h[k] = i + 1; }
    sqlite3_free(lane->ws_hash);
    lane->ws_hash = h;
    lane->ws_hash_cap = cap;
    return SQLITE_OK;
}
static int ws_add (mw_lane *lane, uint32_t pgno, int frame) {
    if ((uint32_t)(lane->ws_n + 1) * 2 > lane->ws_hash_cap) {
        uint32_t cap = lane->ws_hash_cap ? lane->ws_hash_cap : 64;
        while (cap < (uint32_t)(lane->ws_n + 1) * 4) cap *= 2;
        int rc = ws_hash_rebuild(lane, cap);
        if (rc != SQLITE_OK) return rc;
    }
    uint32_t hk = ws_slot(pgno, lane->ws_hash_cap);
    while (lane->ws_hash[hk]) {
        int i = lane->ws_hash[hk] - 1;
        if (lane->ws_pgnos[i] == pgno) { lane->ws_frame[i] = frame; return SQLITE_OK; }
        hk = (hk + 1) & (lane->ws_hash_cap - 1);
    }
    if (lane->ws_n == lane->ws_cap) {
        int cap = lane->ws_cap ? lane->ws_cap * 2 : 16;
        uint32_t *p = sqlite3_realloc64(lane->ws_pgnos, (sqlite3_uint64)cap * sizeof(uint32_t));
        if (!p) return SQLITE_NOMEM;
        lane->ws_pgnos = p;
        int *q = sqlite3_realloc64(lane->ws_frame, (sqlite3_uint64)cap * sizeof(int));
        if (!q) return SQLITE_NOMEM;
        lane->ws_frame = q;
        lane->ws_cap = cap;
    }
    lane->ws_pgnos[lane->ws_n] = pgno;
    lane->ws_frame[lane->ws_n++] = frame;
    lane->ws_hash[hk] = lane->ws_n;
    return SQLITE_OK;
}

// Schema cookie (page 1, bytes 40..43) as seen at the lane's snapshot.
static uint32_t lane_schema_cookie (mw_lane *lane) {
    uint8_t c[4] = {0};
    mw_store *st = lane->db->store;
    if (!mw_store_read(st, 1, lane->tx.snapshot_epoch, 40, 4, c)) {
        mw_file *f = lane->file;
        f->real->pMethods->xRead(f->real, c, 4, 40);
    }
    return ((uint32_t)c[0] << 24) | ((uint32_t)c[1] << 16) | ((uint32_t)c[2] << 8) | c[3];
}


// ---- what the transaction read ----
// The rebase replays the rows that a transaction changed and checks only those. A row that it read and did not change, on a page that somebody else wrote, would go unchecked (write skew), and the pages
// cannot tell the two apart (a blind UPDATE of one row also reads and writes its page). So the statements say it (the statement hook of a mw_rebase=1 connection): a transaction that ran a SELECT, or a
// statement that is not a point statement, is not rebased: the commit is refused as before and the application retries. A point statement is an INSERT ... VALUES or an UPDATE/DELETE of one row found by its rowid or by a unique index
// (the seek of a unique index finds one row; a range or a non-unique index loops), that reads nothing else: its bytecode (EXPLAIN, available in every build of SQLite) has no loop, no cursor opened for
// reading (another table, a subquery), no virtual table. What a foreign key adds is allowed: the lookup of the parent row (a read cursor, one seek), the scan of the child table for a parent that is deleted
// (a loop on a read cursor that only compares columns and counts), the sub-programs of its actions. They are repeated by SQLite in the replay, which runs with the foreign keys on. An UPDATE or DELETE that changed no row has read that the row is absent: also a read.
enum { RD_OTHER = 0, RD_DML };
typedef struct { char *sql; bool point; } rd_ent;
enum { RD_CACHE = 32 };
enum { BC_MAX = 400 };
typedef struct { char op[24]; int addr, p1, p2; } bc_op;
static bool bc_in (const char *op, const char *const *set, size_t n) { for (size_t i = 0; i < n; i++) if (strcmp(op, set[i]) == 0) return true; return false; }
static bool bytecode_is_point (sqlite3 *db, const char *sql) {
    char *ex = sqlite3_mprintf("EXPLAIN %s", sql); if (!ex) return false;
    sqlite3_stmt *e = NULL; int rc = sqlite3_prepare_v2(db, ex, -1, &e, NULL); sqlite3_free(ex);
    if (rc != SQLITE_OK || !e) { sqlite3_finalize(e); return false; }
    static const char *const deny[] = { "SorterSort", "SorterNext", "SeekScan", "OpenEphemeral", "OpenAutoindex", "OpenPseudo", "VOpen", "VFilter", "VNext", "VUpdate", "VColumn", "Gosub",
                                        "BeginSubrtn", "InitCoroutine", "Yield", "Sort" };
    // a constant expression that is computed once (zeroblob(100), abs(-1)) is a block guarded by Once that touches no table; a subquery that SQLite computes once is one too, but it has a cursor in it
    static const char *const constop[] = { "Integer", "Int64", "Real", "String8", "String", "Blob", "Null", "Function", "PureFunc", "Copy", "SCopy", "IntCopy", "Variable", "Add", "Subtract", "Multiply", "Divide",
                                           "Remainder", "Concat", "Cast", "Affinity", "Not", "BitAnd", "BitOr", "ShiftLeft", "ShiftRight", "Negative", "AddImm", "Noop", "MustBeInt", "RealAffinity" };
    // what a foreign key check may do inside a loop that scans the rows of the other table: nothing but compare columns and count
    static const char *const fkbody[] = { "Column", "Ne", "Eq", "Lt", "Le", "Gt", "Ge", "FkCounter", "Integer", "Copy", "SCopy", "Affinity", "IsNull", "NotNull", "Rowid", "IdxGT", "IdxGE", "IdxLT", "IdxLE",
                                          "DeferredSeek", "IdxRowid", "Goto", "MustBeInt", "Null", "String8", "Int64", "Real", "Variable" };
    bc_op *ops = malloc(BC_MAX * sizeof *ops); int n = 0; bool point = ops != NULL;
    while (point && sqlite3_step(e) == SQLITE_ROW) {
        const char *op = (const char *)sqlite3_column_text(e, 1);
        if (!op || n >= BC_MAX) { point = false; break; }
        if (n > 0 && !strcmp(op, "Init")) break;                       // (EXPLAIN lists the sub-programs after the main one, each starting with its own Init: the actions of foreign keys. They are repeated by the replay.)
        snprintf(ops[n].op, sizeof ops[n].op, "%s", op); ops[n].addr = sqlite3_column_int(e, 0); ops[n].p1 = sqlite3_column_int(e, 2); ops[n].p2 = sqlite3_column_int(e, 3); n++;
    }
    sqlite3_finalize(e); e = NULL;
    int rdcur[64]; int nrd = 0;                                        // the cursors opened for reading
    for (int i = 0; point && i < n; i++) if (!strcmp(ops[i].op, "OpenRead")) { if (nrd >= 64) point = false; else rdcur[nrd++] = ops[i].p1; }
    int verified_to = -1;                                              // ops up to here belong to a loop that was verified
    for (int i = 0; point && i < n; i++) {
        const char *op = ops[i].op;
        if (bc_in(op, deny, sizeof deny / sizeof *deny)) { point = false; break; }
        if (!strcmp(op, "Once")) {
            for (int k = i + 1; k < n && ops[k].addr < ops[i].p2; k++) if (!bc_in(ops[k].op, constop, sizeof constop / sizeof *constop)) { point = false; break; }
            continue;
        }
        if (!strcmp(op, "Program")) continue;                           // (the actions of a foreign key; a database with a trigger is never rebased)
        bool isread = false; for (int k = 0; k < nrd; k++) if (rdcur[k] == ops[i].p1) isread = true;
        bool start = !strcmp(op, "Rewind") || !strcmp(op, "Last") || (isread && (!strncmp(op, "Seek", 4)));
        if (start) {
            int j = -1; for (int k = i + 1; k < n; k++) if ((!strcmp(ops[k].op, "Next") || !strcmp(ops[k].op, "Prev")) && ops[k].p1 == ops[i].p1) { j = k; break; }
            if (j < 0) { if (!strncmp(op, "Seek", 4)) continue; point = false; break; }         // (a lookup of one row; a Rewind with no end is not one)
            if (!isread) { point = false; break; }                                              // (a loop over the table that is written: a scan)
            bool fkc = false;
            for (int k = i + 1; k < j; k++) {
                if (!bc_in(ops[k].op, fkbody, sizeof fkbody / sizeof *fkbody)) { point = false; break; }
                if (!strcmp(ops[k].op, "FkCounter")) fkc = true;
            }
            if (!fkc) point = false;
            verified_to = j; i = j; continue;
        }
        if ((!strcmp(op, "Next") || !strcmp(op, "Prev")) && i > verified_to) { point = false; break; }
    }
    free(ops);
    return point;
}
static bool stmt_is_point (mw_lane *lane, sqlite3_stmt *st, const char *sql) {
    rd_ent *c = lane->rd_cache;
    if (!c) { c = lane->rd_cache = sqlite3_malloc64(RD_CACHE * sizeof *c); if (!c) return false; memset(c, 0, RD_CACHE * sizeof *c); }
    for (int i = 0; i < RD_CACHE; i++) if (c[i].sql && strcmp(c[i].sql, sql) == 0) return c[i].point;
    bool point = bytecode_is_point(sqlite3_db_handle(st), sql);
    int slot = lane->rd_cache_next++ % RD_CACHE;
    sqlite3_free(c[slot].sql); c[slot].sql = sqlite3_mprintf("%s", sql); c[slot].point = c[slot].sql ? point : false;
    return point;
}
static int stmt_kind (const char *sql) {
    while (sql && (*sql == ' ' || *sql == '\t' || *sql == '\n' || *sql == '\r')) sql++;
    if (!sql) return RD_OTHER;
    if (sqlite3_strnicmp(sql, "INSERT", 6) == 0 || sqlite3_strnicmp(sql, "REPLACE", 7) == 0 || sqlite3_strnicmp(sql, "UPDATE", 6) == 0 || sqlite3_strnicmp(sql, "DELETE", 6) == 0) return RD_DML;
    return RD_OTHER;
}
static bool has_word (const char *sql, const char *w) {
    size_t n = strlen(w);
    for (const char *p = sql; *p; p++) if (sqlite3_strnicmp(p, w, (int)n) == 0 && (p == sql || !(isalnum((unsigned char)p[-1]) || p[-1] == '_')) && !(isalnum((unsigned char)p[n]) || p[n] == '_')) return true;
    return false;
}
static bool stmt_reads (const char *sql) {
    while (sql && (*sql == ' ' || *sql == '\t' || *sql == '\n' || *sql == '\r' || *sql == '(')) sql++;
    return sql && (sqlite3_strnicmp(sql, "SELECT", 6) == 0 || sqlite3_strnicmp(sql, "WITH", 4) == 0 || sqlite3_strnicmp(sql, "VALUES", 6) == 0);
}
void mw_lane_stmt_note (mw_lane *lane, mw_stmt_note *n) {
    sqlite3_stmt *st = n->stmt;
    if (!n->ending) {
        if (n->autocommit) { lane->rd_dep = false; lane->rd_chg_base = sqlite3_total_changes(sqlite3_db_handle(st)); }       // (this statement starts a transaction)
        const char *sql = sqlite3_sql(st);
        if (sql && sqlite3_strnicmp(sql, "EXPLAIN", 7) == 0) return;               // (our own: the bytecode of a statement is looked at from inside the hook)
        lane->rd_db = sqlite3_db_handle(st);
        if (stmt_reads(sql) || (sql && has_word(sql, "REPLACE"))) lane->rd_dep = true;      // (REPLACE deletes rows that no change counter counts)
        lane->rd_cur = st; lane->rd_cur_kind = stmt_kind(sql);
        if (lane->rd_cur_kind == RD_DML && !stmt_is_point(lane, st, sql)) lane->rd_dep = true;
        return;
    }
    if (lane->rd_cur != st) return;                                              // (also the EXPLAIN of the hook itself)
    lane->rd_cur = NULL;
    if (lane->rd_cur_kind == RD_DML && !sqlite3_stmt_readonly(st) && sqlite3_changes(sqlite3_db_handle(st)) == 0) lane->rd_dep = true;       // (an UPDATE or DELETE that found nothing has read that the row is absent)
}
// (A statement that changed nothing writes no page, so the one that is committing now has no such read.)
// The hook is the connection's one trace callback. SQLite has no way to read the current one, so it cannot be saved and put back, or chained: an application that installs its own sqlite3_trace_v2
// replaces it, and then no statement is seen. A commit happens inside a statement (COMMIT, or the write itself in autocommit mode), and the hook has seen that one if it is the statement that is running
// now on the connection. If it is not, the hook was replaced (at any time: even in the middle of the transaction the statement that commits is not seen): the transaction is treated as one that
// read, no rebase, never a wrong one.
bool mw_lane_reads_unchanged (mw_lane *lane, int nnet) {
    if (lane->rd_dep) return true;
    if (!lane->rd_cur || !lane->rd_db) return true;
    bool alive = false;
    for (sqlite3_stmt *st = sqlite3_next_stmt(lane->rd_db, NULL); st; st = sqlite3_next_stmt(lane->rd_db, st)) if (st == lane->rd_cur && sqlite3_stmt_busy(st)) alive = true;
    if (!alive) return true;
    // A statement that changes a row to what it is (UPDATE t SET a = 5 where a is 5) is counted as a change and writes no page, so the replay, which has only the row changes of the pages, does not see it: the
    // row it read (and wrote, as it thought) goes unchecked. The changes that the statements counted (sqlite3_total_changes: the rows of the statements and of the foreign key actions, not the rows that
    // REPLACE deletes: those transactions are excluded above) must be exactly the net row changes found in the pages: every counted change is then a row that changed, and no row changed twice or back.
    // (The committing statement itself is not counted yet: an autocommit point statement changes one row, or it would have written no page.)
    int counted = (int)((unsigned)sqlite3_total_changes(lane->rd_db) - (unsigned)lane->rd_chg_base);
    if (lane->rd_cur_kind == RD_DML) counted++;
    return counted != nnet;
}

// A conflicting transaction can be rebased (multiwriter_rebase.c: its row changes are replayed at the latest snapshot) only if the connection asked for it (URI mw_rebase=1), it is not itself a
// rebase helper, it is the first commit of its snapshot (a later one would be missing the pages of the earlier one) and it changed no schema (its page 1 carries the snapshot's cookie).
static bool lane_can_rebase (mw_lane *lane, const uint8_t *pg1, uint32_t snapshot_cookie) {
    if (lane->norebase || !lane->rebase_on) return false;
    if (lane->commit_base > 0) return false;
    if (lane->rb_skip > 0) { lane->rb_skip--; return false; }
    if (pg1) {
        uint32_t c = ((uint32_t)pg1[40] << 24) | ((uint32_t)pg1[41] << 16) | ((uint32_t)pg1[42] << 8) | pg1[43];
        if (c != snapshot_cookie) return false;                  // DDL in this transaction
    }
    return true;
}

// Publishes the captured write set as one atomic commit at a new epoch.
// Consecutive refusals after which a long transaction takes the turn (MW_LONG_STARVE).
static int long_starve (void) { static _Atomic int c = MW_KNOB_UNSET; return mw_knob_int(&c, "MW_LONG_STARVE", 2); }

static int lane_publish_inner (mw_lane *lane);
// Every commit is counted while it is inside (the recovery of a failed log waits for the others to leave), and the first commit that finds the database failed tries to recover it.
static int lane_publish (mw_lane *lane) {
    mw_db *db = lane->db;
    for (;;) {
        atomic_fetch_add(&db->inflight, 1);
        if (!atomic_load(&db->recovering)) break;
        atomic_fetch_sub(&db->inflight, 1);                                  // (a recovery is running: it stops the threads of the metadata store, so those must not wait for it)
        for (int i = 0; i < 5000 && atomic_load(&db->recovering); i++) usleep(1000);
    }
    if (atomic_load(&db->failed) && !db->mp) (void)mw_db_recover(db);
    int rc = lane_publish_inner(lane);
    atomic_fetch_sub(&db->inflight, 1);
    return rc;
}
static int lane_publish_inner (mw_lane *lane) {
    mw_db *db = lane->db;
    if (!lane->snapshot_held) {
        // The engine learns that a transaction began from the locks that SQLite takes in the shared memory of the WAL. A VFS stacked above this one that keeps its own shared memory (SQLite's test VFS
        // `tvfs` does) never passes them on: there is no snapshot, and the commit would be validated against nothing.
        static _Atomic int said;
        if (!atomic_exchange(&said, 1)) sqlite3_log(SQLITE_MISUSE, "multiwriter: a commit without a snapshot: a VFS stacked above does not forward xShmMap/xShmLock to it");
        return SQLITE_IOERR;
    }
    mw_memwal *w = &lane->wal;
    size_t fs = (size_t)w->pgsz + 24;
    const uint8_t **imgs = malloc((size_t)lane->ws_n * sizeof(uint8_t *));
    if (!imgs) return SQLITE_NOMEM;
    const uint8_t *pg1 = NULL;                                   // page 1 image if the transaction wrote it
    for (int i = 0; i < lane->ws_n; i++) {
        imgs[i] = w->buf + 32 + (size_t)lane->ws_frame[i] * fs + 24;
        if (lane->ws_pgnos[i] == 1) pg1 = imgs[i];
    }
    if (!lane->dsz_valid || lane->dsz_epoch != lane->tx.snapshot_epoch) { lane->dsz_val = mw_store_dbsize(db->store, lane->tx.snapshot_epoch); lane->dsz_epoch = lane->tx.snapshot_epoch; lane->dsz_valid = true; }
    if (lane->poisoned && lane->own_n > 0) { free(imgs); return MW_CONFLICT_READ; }          // (a commit of this snapshot was relocated: the next one would use the page numbers it had before; the snapshot ends and the transaction runs again)
    uint32_t snap_size = lane->dsz_val;
    uint32_t cookie = lane_schema_cookie(lane);
    // pages read but not written: mark the write set in the (sorted) bitmap, walk the read list
    uint32_t *ro = NULL;
    int nro = 0;
    if (lane->readcheck && lane->rs_n > 0) {
        ro = malloc((size_t)lane->rs_n * sizeof(uint32_t));
        if (!ro) { free(imgs); return SQLITE_NOMEM; }
        for (int i = 0; i < lane->ws_n; i++) if (lane->ws_pgnos[i] < lane->rs_bits_cap) lane->rs_bits[lane->ws_pgnos[i]] |= 2;
        for (int i = 0; i < lane->rs_n; i++) if (!(lane->rs_bits[lane->rs_list[i]] & 2)) ro[nro++] = lane->rs_list[i];
        for (int i = 0; i < lane->ws_n; i++) if (lane->ws_pgnos[i] < lane->rs_bits_cap) lane->rs_bits[lane->ws_pgnos[i]] &= (uint8_t)~2;
    }
    { static _Atomic int tr = MW_KNOB_UNSET; if (mw_knob_flag(&tr, "MW_PAGE_TRACE")) { char line[2048]; int o = snprintf(line, sizeof line, "PAGES n=%d:", lane->ws_n); for (int i = 0; i < lane->ws_n && o < (int)sizeof line - 12; i++) o += snprintf(line + o, sizeof line - (size_t)o, " %u", (unsigned)lane->ws_pgnos[i]); fprintf(stderr, "%s\n", line); } }   // (diagnostic: the page numbers of every commit, to map them to B-trees offline)
    mw_validate v = { .snapshot_epoch = lane->tx.snapshot_epoch, .check_cookie = true, .cookie = cookie, .read_pgnos = ro, .n_read = nro,
                      .own_pgnos = lane->own_pg, .own_n = lane->own_n, .own_epoch = lane->own_epoch, .own_epochs = lane->own_ep };
    uint64_t epoch = 0;
    uint64_t tp0 = MW_T0();
    // How long did this transaction stay open? A per-database turn (hot-spot escalation) is held from the snapshot to the commit: it pays for transactions that last
    // microseconds and serialises everything for those that last milliseconds (an application working inside the transaction: 5 ms think time made the median 32 ms
    // and the p99 130 ms with 8 agents on 4 hot rows). Long transactions only get the turn when they are starving (see `starving` below).
    if (lane->tx_t0) {
        struct timespec t1; clock_gettime(CLOCK_MONOTONIC, &t1);
        uint64_t d = (uint64_t)t1.tv_sec * 1000000000ull + (uint64_t)t1.tv_nsec - lane->tx_t0;
        lane->tx_long_run = d > 2000000 ? (lane->tx_long_run < 8 ? lane->tx_long_run + 1 : 8) : 0;   // (a run of them, not an average: one descheduled thread must not change the class)
    }
    const bool long_tx = lane->tx_long_run >= 3;                 // > 2 ms from snapshot to commit three times in a row (a 100-row insert is ~0.5 ms; this is an application working inside the transaction)
    // Multi-process: ONE round of the publication lock per commit. The plain attempt, and the relocation if it is refused because other commits extended the file, both run
    // inside the same hold (the relocation is prepared against the state it publishes on), instead of queueing again for the relocation: with N processes every wait for the
    // lock is long, somebody commits meanwhile and the prepared relocation would be refused again (3.7 lock rounds per commit at 128 processes, 335 us of lock per commit).
    // The rebase replays the row changes of the transaction and runs without the lock.
    bool mp_hold = db->mp && !(lane->rs_overflow && lane->readcheck);
    if (mp_hold) { mw_lane_reloc_prepare(lane, lane->ws_pgnos, imgs, lane->ws_n, lane->ws_dbsize, snap_size); if (lane->prep_delay_us > 0 && lane->rprep) usleep((useconds_t)lane->prep_delay_us); }       // (the private copies of the relocation that the commit most likely needs: made before the lock)
    if (mp_hold) {                                                // (what does not depend on the state of the others is made before the lock: the hashes of the pages of the record)
        lane->pre_ch = malloc((size_t)lane->ws_n * sizeof(uint64_t)); lane->pre_use = NULL;
        if (lane->pre_ch) { for (int i = 0; i < lane->ws_n; i++) lane->pre_ch[i] = mw_seglog_content_hash(imgs[i], (size_t)w->pgsz); lane->pre_use = lane->pre_ch; }
    }
    if (mp_hold) { uint64_t tw0 = MW_T0(); mw_gate_enter(db, lane); mw_mp_lock(db); MW_T1(MW_ST_MP_WAIT, tw0); lane->mp_held = true; lane->mp_t0 = MW_T0(); }
    int rc = lane->rs_overflow && lane->readcheck ? SQLITE_NOMEM                 // (the read set could not be kept: out of memory, which is what the application must see, not a conflict it would retry)
           : mw_db_publish(db, lane, &v, lane->ws_pgnos, imgs, lane->ws_n, lane->ws_dbsize, snap_size, lane->sync_level >= 2, &epoch);
#define MP_RELEASE_NOFINISH() do { if (mp_hold) { mp_hold = false; lane->mp_held = false; MW_T1(MW_ST_MP_HELD, lane->mp_t0); mw_mp_unlock(db); mw_gate_exit(db); mw_log_prefill_bg(db); } } while (0)
#define MP_RELEASE() do { if (mp_hold) { mp_hold = false; lane->mp_held = false; MW_T1(MW_ST_MP_HELD, lane->mp_t0); mw_mp_unlock(db); mw_gate_exit(db); mw_log_prefill_bg(db); \
                          if (rc == SQLITE_OK) rc = mw_db_publish_finish(db, lane, rc, epoch, lane->sync_level >= 2); } } while (0)
    if (rc != MW_CONFLICT) MP_RELEASE();
    if (lane->bp_wait_us && !mp_hold) { struct timespec bp = { 0, (long)lane->bp_wait_us * 1000L }; lane->bp_wait_us = 0; nanosleep(&bp, NULL); }       // back-pressure that the publish itself did not serve (a commit through the relocation path never reaches its finish): nothing is held here

    if (rc == SQLITE_OK) {
        lane->tx.commit_epoch = epoch;
        lane->tx.state = MW_TX_COMMITTED;
        lane->consec_aborts = 0;
        if (lane->retry_credit > 0) lane->retry_credit--;
        if (pg1 && ((uint32_t)pg1[40] << 24 | (uint32_t)pg1[41] << 16 | (uint32_t)pg1[42] << 8 | pg1[43]) != cookie) {   // schema change
            atomic_store(&db->last_schema_epoch, epoch);
            if (db->mp) atomic_store(&db->shm->schema_epoch, epoch);
            atomic_fetch_add(&db->schema_generation, 1);
        }
    } else if (rc == MW_CONFLICT) {
        // 1. Was the conflict only that other commits extended the file? Then the transaction's new pages are renumbered above the new end and it commits.
        uint64_t tr0 = MW_T0();
        int rr = lane->noreloc ? MW_RELOC_NA : mw_lane_relocate(lane, &v, lane->ws_pgnos, imgs, lane->ws_n, lane->ws_dbsize, snap_size, lane->sync_level >= 2, &epoch);
        if (!lane->noreloc && rr != MW_RELOC_NA) MW_T1(MW_ST_RELOC, tr0);
        if (rr == SQLITE_OK) rc = SQLITE_OK; else MP_RELEASE_NOFINISH();
        if (rr == SQLITE_OK) {
            MP_RELEASE();                                        // (multi-process: unlock, then wait for the group fsync; rc may turn into an I/O error)
            if (rc == SQLITE_OK) { lane->tx.commit_epoch = epoch; lane->tx.state = MW_TX_COMMITTED; lane->consec_aborts = 0; if (lane->retry_credit > 0) lane->retry_credit--; lane->poisoned = true; }
        }
        else if (rr != MW_RELOC_NA) rc = rr;
        else if (lane_can_rebase(lane, pg1, cookie)) {
            // 2. Physical conflict only on pages that it wrote (mw_rebase=1): discard its pages and replay its row changes at the latest snapshot; stock SQLite regenerates the pages.
            rc = mw_lane_rebase(lane, imgs, cookie, &epoch);
            if (rc == SQLITE_OK) lane->rb_streak = 0; else if (!lane->rb_nobackoff) { if (lane->rb_streak < 5) lane->rb_streak++; lane->rb_skip = (1 << lane->rb_streak) - 1; }      // (refused: the next conflicts of this connection are probably true ones too: they are not replayed, 1, 3, 7... in a row)
            if (rc == SQLITE_OK) { lane->tx.state = MW_TX_COMMITTED; lane->consec_aborts = 0; }   // commit_epoch was set by the rebase (the credit granted above is spent by the next commits)
            else if (rc != MW_CONFLICT) { lane->tx.state = MW_TX_ABORTED; atomic_fetch_add(&db->n_aborts, 1); }   // (any other failure also rolls the transaction back)
        }
    }
    free(imgs);
    free(ro);                                                    // (the read set is used again by the relocation above: freed only now)
    free(lane->pre_ch); lane->pre_ch = NULL; lane->pre_use = NULL; mw_lane_reloc_discard(lane);
    MW_T1(MW_ST_PUBLISH, tp0);
    if (rc == MW_CONFLICT || rc == MW_CONFLICT_SCHEMA || rc == MW_CONFLICT_READ) {
        // Not rebasable (or the rebase gave up): the transaction is rolled back, the caller retries.
        if (rc == MW_CONFLICT && lane->rebase_on) atomic_fetch_add(&db->n_unrebasable, 1);
        lane->tx.state = MW_TX_ABORTED;
        atomic_fetch_add(&db->n_aborts, 1);
        // A write-write conflict on the same pages repeats, so the next transactions run serialised. A schema change is not a hot spot, and neither is a
        // *read* conflict: an interior b-tree page read on the way down that somebody else split says nothing about the next transaction's pages.
        // Serialising pays off where a conflict is expensive: a tracked (rebasable) transaction that loses costs a whole rebase. An untracked one just runs
        // its statement again, and waiting for a turn costs more than the occasional retry (measured: 8-32 concurrent bulk / append writers, 1.3-1.5x faster without).
        lane->consec_aborts++;
        const bool starving = lane->consec_aborts >= (long_tx ? long_starve() : 16);          // (a long transaction that lost twice in a row takes the turn and keeps it for hot_credit*16 transactions: FIFO queue, bounded tail)
        if (!lane->norebase && rc != MW_CONFLICT_SCHEMA && rc != MW_CONFLICT_READ && (starving)) {
            lane->retry_credit = long_tx ? lane->hot_credit * 16 : lane->hot_credit;
            if (mw_timing_on) { extern _Atomic uint64_t mw_grants[2]; atomic_fetch_add(&mw_grants[lane->rebase_on ? 0 : 1], 1); }
        }
        rc = SQLITE_BUSY_SNAPSHOT;
    }
    return rc;
}

// Called when the commit frame (frame index k, db size after commit = dbsize) is complete.
static int lane_commit_frame (mw_lane *lane, int k, uint32_t dbsize) {
    mw_memwal *w = &lane->wal;
    size_t fs = (size_t)w->pgsz + 24;
    if (lane->ws_hash_cap > 4096 && (uint32_t)lane->ws_n * 16 < lane->ws_hash_cap) { sqlite3_free(lane->ws_hash); lane->ws_hash = NULL; lane->ws_hash_cap = 0; }   // (a bulk load must not leave a huge table)
    else if (lane->ws_hash_cap) memset(lane->ws_hash, 0, (size_t)lane->ws_hash_cap * sizeof(int));
    lane->ws_n = 0;
    lane->ws_dbsize = dbsize;
    if (k < lane->commit_base) lane->commit_base = 0;            // (the log was restarted: earlier frames are gone)
    for (int i = lane->commit_base; i <= k; i++) {
        size_t off = 32 + (size_t)i * fs;
        if (off + fs > w->size) return SQLITE_IOERR;
        int rc = ws_add(lane, be32(w->buf + off), i);
        if (rc != SQLITE_OK) return rc;
    }
    w->commit_seen = true;
    lane->tx.ws_pages = (uint32_t)lane->ws_n;
    lane->tx.state = MW_TX_PREPARED;
    int rc = lane_publish(lane);
    if (rc == SQLITE_OK) {
        // The read snapshot may outlive this commit (a statement still stepping): the next commit starts after these frames and must not
        // count this commit's pages as somebody else's change.
        lane->commit_base = k + 1;
        lane->dsz_val = dbsize;                                  // (the size this snapshot has after its own commit: the next commit of it that does not change the size must not record the size at the snapshot, which another commit may have grown meanwhile)
        if (lane->own_n + lane->ws_n > lane->own_cap) {
            int cap = lane->own_n + lane->ws_n + 16;
            uint32_t *p = sqlite3_realloc64(lane->own_pg, (sqlite3_uint64)cap * sizeof(uint32_t));
            if (!p) { lane->commit_base = 0; return rc; }
            lane->own_pg = p;
            uint64_t *pe = sqlite3_realloc64(lane->own_ep, (sqlite3_uint64)cap * sizeof(uint64_t));
            if (!pe) { lane->commit_base = 0; return rc; }
            lane->own_ep = pe;
            lane->own_cap = cap;
        }
        const uint64_t ce = lane->tx.commit_epoch;
        for (int i = 0; i < lane->ws_n; i++) { lane->own_ep[lane->own_n] = ce; lane->own_pg[lane->own_n++] = lane->ws_pgnos[i]; }
        // sorted by page, and for equal pages the latest commit first; one entry for each page, with the epoch of the commit that wrote it last
        { int n = lane->own_n; pe_t *t = malloc((size_t)n * sizeof *t);
          if (!t) { lane->commit_base = 0; return rc; }
          for (int i = 0; i < n; i++) { t[i].pg = lane->own_pg[i]; t[i].ep = lane->own_ep[i]; }
          qsort(t, (size_t)n, sizeof *t, cmp_pe);
          int u = 0; for (int i = 0; i < n; i++) if (i == 0 || t[i].pg != lane->own_pg[u - 1]) { lane->own_pg[u] = t[i].pg; lane->own_ep[u] = t[i].ep; u++; }
          lane->own_n = u; free(t); }
        lane->own_epoch = lane->tx.commit_epoch;
    }
    return rc;
}

// MARK: - private WAL (memory file) -

static int wal_close (sqlite3_file *pf) {
    mw_file *f = (mw_file *)pf;
    mw_ev(MW_EV_CLOSE, f, 0, 0, 0);
    if (f->lane) { f->lane->wal_bound = false; f->lane->wal.size = 0; }
    f->base.pMethods = NULL;
    return SQLITE_OK;
}
static int wal_read (sqlite3_file *pf, void *buf, int n, sqlite3_int64 off) {
    mw_memwal *w = &FILE_LANE(pf)->wal;
    mw_ev(MW_EV_READ, (mw_file *)pf, off, n, 1);
    if ((size_t)off >= w->size) { memset(buf, 0, (size_t)n); return SQLITE_IOERR_SHORT_READ; }
    size_t avail = w->size - (size_t)off;
    size_t take = avail < (size_t)n ? avail : (size_t)n;
    memcpy(buf, w->buf + off, take);
    if (take < (size_t)n) { memset((uint8_t *)buf + take, 0, (size_t)n - take); return SQLITE_IOERR_SHORT_READ; }
    return SQLITE_OK;
}
static int wal_write (sqlite3_file *pf, const void *buf, int n, sqlite3_int64 off) {
    mw_lane *lane = FILE_LANE(pf);
    mw_memwal *w = &lane->wal;
    mw_ev(MW_EV_WRITE, (mw_file *)pf, off, n, 1);
    size_t end = (size_t)off + (size_t)n;
    if (end > w->cap) {
        size_t cap = w->cap ? w->cap : 65536;
        while (cap < end) cap *= 2;
        uint8_t *p = sqlite3_realloc64(w->buf, cap);
        if (!p) return SQLITE_NOMEM;
        w->buf = p;
        w->cap = cap;
    }
    memcpy(w->buf + off, buf, (size_t)n);
    if (end > w->size) w->size = end;

    // WAL layout: 32-byte header, then frames of (24-byte header + page). SQLite writes the
    // header and the page of a frame with two calls (walWriteOneFrame); the page write ends it.
    if (off == 0 && n == 32) { w->pgsz = (int)be32(w->buf + 8); return SQLITE_OK; }
    if (n == 24) return SQLITE_OK;                                       // frame header, or checksum rewrite of one
    if (w->commit_seen && w->pgsz && n < w->pgsz) return SQLITE_OK;      // (padding after the commit frame: a device that is not power-safe makes SQLite pad the WAL to a sector boundary with a frame header and part of a page; a VFS stacked over this one can say so)
    if (w->pgsz == 0 || n != w->pgsz || off < 32 || ((size_t)off - 32) % ((size_t)w->pgsz + 24) != 24) return SQLITE_IOERR_WRITE;   // unexpected layout: fail loudly
    int k = (int)(((size_t)off - 32) / ((size_t)w->pgsz + 24));
    uint32_t ntrunc = be32(w->buf + 32 + (size_t)k * ((size_t)w->pgsz + 24) + 4);
    if (ntrunc != 0) return lane_commit_frame(lane, k, ntrunc);          // commit frame complete
    return SQLITE_OK;
}
static int wal_truncate (sqlite3_file *pf, sqlite3_int64 size) {
    mw_memwal *w = &FILE_LANE(pf)->wal;
    if ((size_t)size < w->size) w->size = (size_t)size;
    return SQLITE_OK;
}
static int wal_sync (sqlite3_file *pf, int flags) { mw_ev(MW_EV_SYNC, (mw_file *)pf, 0, 0, flags | 0x100); return SQLITE_OK; }
static int wal_filesize (sqlite3_file *pf, sqlite3_int64 *size) { *size = (sqlite3_int64)FILE_LANE(pf)->wal.size; return SQLITE_OK; }
static int wal_nolock (sqlite3_file *pf, int l) { return SQLITE_OK; }
static int wal_check_reserved (sqlite3_file *pf, int *out) { *out = 0; return SQLITE_OK; }
static int wal_file_control (sqlite3_file *pf, int op, void *arg) { return SQLITE_NOTFOUND; }
static int wal_sector_size (sqlite3_file *pf) { return 512; }
static int wal_device_char (sqlite3_file *pf) { return 0; }

int mw_lane_open_wal (mw_file *f, const char *name) {
    mw_lane *lane = tls_lane;
    if (!lane || !lane->private_mode || lane->wal_bound) return SQLITE_CANTOPEN;
    size_t pn = strlen(lane->db->path);
    if (strncmp(name, lane->db->path, pn) != 0 || strcmp(name + pn, "-wal") != 0) return SQLITE_CANTOPEN;   // not the lane we think
    f->lane = lane;
    f->is_wal = 1;
    lane->wal_bound = true;
    lane->wal.size = 0;
    f->base.pMethods = &lane_wal_io;
    return SQLITE_OK;
}

// MARK: - lane reset -

// Header copies live at bytes 0..95 of the first wal-index region. An all-zero header has
// isInit == 0, which SQLite treats as "malformed": it takes the (private) write lock, runs
// walIndexRecover over the (now empty) WAL and reports changed=1 => page cache reset.
void mw_lane_reset (mw_lane *lane, bool keep_header) {
    lane->commit_base = 0;
    lane->own_n = 0; lane->poisoned = false;
    lane->wal.size = 0;
    lane->wal.commit_seen = false;
    lane->wal.pgsz = 0;
    if (!keep_header && lane->nshm > 0 && lane->shm[0]) memset(lane->shm[0], 0, 96);
}

void mw_lane_free (mw_lane *lane) {
    if (!lane) return;
    mw_lane_reloc_discard(lane);
    if (lane->db && (lane->ddl_active || lane->db->ddl_owner == lane)) mw_lane_ddl_end(lane);        // (a connection that closes inside a DDL, without a snapshot of the main file, gives the barrier back: its pointer must not stay behind)
    if (lane->mp_slot >= 0 && lane->db && lane->db->mp) mw_mp_slot_free(lane->db, lane->mp_slot);
    for (int i = 0; i < lane->nshm; i++) sqlite3_free(lane->shm[i]);
    sqlite3_free(lane->wal.buf);
    sqlite3_free(lane->ws_pgnos);
    sqlite3_free(lane->ws_frame);
    sqlite3_free(lane->own_pg); sqlite3_free(lane->own_ep);
    sqlite3_free(lane->ws_hash);
    if (lane->rd_cache) { for (int i = 0; i < 32; i++) sqlite3_free(((rd_ent *)lane->rd_cache)[i].sql); sqlite3_free(lane->rd_cache); }
    sqlite3_free(lane->rs_bits);
    sqlite3_free(lane->rs_list);
    mw_lane_rebase_free(lane);
    sqlite3_free(lane);
}

// MARK: - main database file (private lane) -

// Read set: every page fetched through xRead during the snapshot (page 1 excluded: its schema cookie is
// validated separately, and it changes with every file growth). Complete because each transaction
// starts with a cold page cache (see the header comment).
static void rs_mark (mw_lane *lane, uint32_t pgno) {
    if (pgno >= lane->rs_bits_cap) {
        size_t cap = lane->rs_bits_cap ? lane->rs_bits_cap : 4096;
        while (cap <= pgno) cap *= 2;
        uint8_t *p = sqlite3_realloc64(lane->rs_bits, cap);
        if (!p) { lane->rs_overflow = true; return; }                            // cannot track: the commit fails with SQLITE_NOMEM
        memset(p + lane->rs_bits_cap, 0, cap - lane->rs_bits_cap);
        lane->rs_bits = p;
        lane->rs_bits_cap = cap;
    }
    if (lane->rs_bits[pgno]) return;
    if (lane->rs_n == lane->rs_cap) {
        int cap = lane->rs_cap ? lane->rs_cap * 2 : 64;
        uint32_t *p = sqlite3_realloc64(lane->rs_list, (sqlite3_uint64)cap * sizeof(uint32_t));
        if (!p) { lane->rs_overflow = true; return; }
        lane->rs_list = p;
        lane->rs_cap = cap;
    }
    lane->rs_bits[pgno] = 1;
    lane->rs_list[lane->rs_n++] = pgno;
}

static uint64_t lane_snapshot (mw_lane *lane) {
    return lane->snapshot_held ? lane->tx.snapshot_epoch : mw_db_visible_epoch(lane->db);
}

// Reads are served page by page: newest committed version <= the snapshot, else the real file.
static int lm_read (sqlite3_file *pf, void *buf, int n, sqlite3_int64 off) {
    mw_lane *lane = FILE_LANE(pf);
    tls_lane = lane;
    mw_ev(MW_EV_READ, (mw_file *)pf, off, n, 2);
    mw_store *st = lane->db->store;
    uint64_t snap = lane_snapshot(lane);
    uint8_t *dst = buf;
    int remaining = n;
    sqlite3_int64 o = off;
    while (remaining > 0) {
        uint32_t pgno = (uint32_t)(o / st->pgsz) + 1;
        uint32_t poff = (uint32_t)(o % st->pgsz);
        uint32_t take = (uint32_t)st->pgsz - poff;
        if ((int)take > remaining) take = (uint32_t)remaining;
        if (lane->readcheck && lane->snapshot_held && pgno != 1) rs_mark(lane, pgno);
        if (!mw_store_read(st, pgno, snap, poff, take, dst)) {
            int rc = mw_io_hit(MW_IO_READ, NULL) ? SQLITE_IOERR_READ : pass_io->xRead(pf, dst, (int)take, o);
            if (rc == SQLITE_OK && poff == 0 && take == (uint32_t)st->pgsz) mw_store_cache_base(st, pgno, dst);
            if (rc != SQLITE_OK) {
                if (rc == SQLITE_IOERR_SHORT_READ) memset(dst + take, 0, (size_t)remaining - take);
                return rc;
            }
        }
        dst += take; o += take; remaining -= (int)take;
    }
    return SQLITE_OK;
}
static int lm_write (sqlite3_file *pf, const void *buf, int n, sqlite3_int64 off) {
    mw_ev(MW_EV_WRITE, (mw_file *)pf, off, n, 2);      // swallowed: only the compactor writes the real file
    return SQLITE_OK;
}
static int lm_truncate (sqlite3_file *pf, sqlite3_int64 size) { return SQLITE_OK; }
static int lm_sync (sqlite3_file *pf, int flags) { return SQLITE_OK; }
static int lm_filesize (sqlite3_file *pf, sqlite3_int64 *size) {
    mw_lane *lane = FILE_LANE(pf);
    tls_lane = lane;
    uint32_t pages;
    {
        uint64_t snap = lane_snapshot(lane);
        if (!lane->dsz_valid || lane->dsz_epoch != snap) { lane->dsz_val = mw_store_dbsize(lane->db->store, snap); lane->dsz_epoch = snap; lane->dsz_valid = true; }
        pages = lane->dsz_val;
    }
    *size = (sqlite3_int64)pages * lane->db->store->pgsz;
    return SQLITE_OK;
}
// File locks are virtualised above SHARED. A lane never writes the real file, so it needs no
// RESERVED/PENDING/EXCLUSIVE lock; taking them for real would also make every closing connection's
// checkpoint attempt (PENDING) fail *other* connections' opens with SQLITE_BUSY. The real SHARED lock
// is kept: it stops an unaware stock connection from checkpointing/rewriting the file under us.
static int lm_lock (sqlite3_file *pf, int l) {
    tls_lane = FILE_LANE(pf);
    if (l <= SQLITE_LOCK_SHARED) return pass_io->xLock(pf, l);
    return SQLITE_OK;
}
static int lm_unlock (sqlite3_file *pf, int l) {
    if (l >= SQLITE_LOCK_SHARED) return SQLITE_OK;             // back to SHARED: the real lock never left it
    return pass_io->xUnlock(pf, l);
}
static int lm_check_reserved (sqlite3_file *pf, int *out) { *out = 0; return SQLITE_OK; }

static int pragma_is (const char *name, const char *want) { return name && sqlite3_stricmp(name, want) == 0; }

static int lm_file_control (sqlite3_file *pf, int op, void *arg) {
    mw_lane *lane = FILE_LANE(pf);
    tls_lane = lane;
    if (op == MW_FCNTL_TXINFO) { *(mw_tx_info *)arg = lane->tx; return SQLITE_OK; }
    if (op == MW_FCNTL_WRITESET) {
        mw_writeset *ws = (mw_writeset *)arg;
        ws->n = lane->ws_n;
        for (int i = 0; i < lane->ws_n && i < ws->cap; i++) ws->pgnos[i] = lane->ws_pgnos[i];
        return SQLITE_OK;
    }
    if (op == MW_FCNTL_DBSTATS) { mw_lane_fill_stats(lane, (mw_db_stats *)arg); return SQLITE_OK; }
    if (op == MW_FCNTL_STMT) { mw_lane_stmt_note(lane, (mw_stmt_note *)arg); return SQLITE_OK; }
    if (op == MW_FCNTL_DDL_BEGIN) { mw_lane_ddl_begin(lane); return SQLITE_OK; }
    if (op == MW_FCNTL_DDL_RELEASE_IDLE) { if (lane->ddl_active && !lane->snapshot_held) mw_lane_ddl_end(lane); return SQLITE_OK; }
    if (op == MW_FCNTL_COMPACT) { mw_compact_result *r = (mw_compact_result *)arg; return mw_db_compact(lane->db, r); }
    if (op == MW_FCNTL_LANE_PTR) { *(void **)arg = lane; return SQLITE_OK; }
    if (op == MW_FCNTL_GC) { *(uint64_t *)arg = mw_db_gc(lane->db); return SQLITE_OK; }
    if (op == SQLITE_FCNTL_MMAP_SIZE) { *(sqlite3_int64 *)arg = 0; return SQLITE_OK; }   // mmap disabled: reports 0 disabled: reads must go through xRead
    if (op == SQLITE_FCNTL_SIZE_HINT) return SQLITE_OK;         // we never grow the real file from a lane
    if (op == SQLITE_FCNTL_PRAGMA) {
        char **az = (char **)arg;
        const char *name = az[1], *val = az[2];
        if (pragma_is(name, "journal_mode") && val && sqlite3_stricmp(val, "wal") != 0) {
            az[0] = sqlite3_mprintf("multiwriter: journal_mode=%s is not supported (WAL only)", val);
            return SQLITE_ERROR;
        }
        if (pragma_is(name, "locking_mode") && val && sqlite3_stricmp(val, "exclusive") == 0) {
            az[0] = sqlite3_mprintf("multiwriter: locking_mode=EXCLUSIVE is not supported");
            return SQLITE_ERROR;
        }
        if (pragma_is(name, "auto_vacuum") && val && sqlite3_stricmp(val, "none") != 0 && strcmp(val, "0") != 0) {
            az[0] = sqlite3_mprintf("multiwriter: auto_vacuum=%s is not supported (NONE only)", val);
            return SQLITE_ERROR;
        }
        if (pragma_is(name, "synchronous") && val) {          // observed, not handled: SQLite still executes it
            int lv = (int)strtol(val, NULL, 10);
            if (!strcmp(val, "0") || !sqlite3_stricmp(val, "off")) lv = 0;
            else if (!sqlite3_stricmp(val, "normal")) lv = 1;
            else if (!sqlite3_stricmp(val, "full")) lv = 2;
            else if (!sqlite3_stricmp(val, "extra")) lv = 3;
            lane->sync_level = lv;
            return SQLITE_NOTFOUND;
        }
        if (pragma_is(name, "mmap_size") && val) { az[0] = sqlite3_mprintf("0"); return SQLITE_OK; }   // normalised to 0
        return SQLITE_NOTFOUND;
    }
    return pass_io->xFileControl(pf, op, arg);
}

static int lm_shm_map (sqlite3_file *pf, int page, int size, int extend, void volatile **pp) {
    mw_lane *lane = FILE_LANE(pf);
    tls_lane = lane;
    mw_ev(MW_EV_SHMMAP, (mw_file *)pf, page, size, extend);
    if (size != MW_SHM_REGION_BYTES || page >= MW_SHM_MAX_REGIONS) return SQLITE_IOERR_SHMSIZE;
    if (page >= lane->nshm) {
        if (!extend) { *pp = NULL; return SQLITE_OK; }
        while (lane->nshm <= page) {
            uint8_t *r = sqlite3_malloc64(MW_SHM_REGION_BYTES);
            if (!r) return SQLITE_NOMEM;
            memset(r, 0, MW_SHM_REGION_BYTES);
            lane->shm[lane->nshm++] = r;
        }
    }
    *pp = lane->shm[page];
    return SQLITE_OK;
}

// Locks are private (no other connection can observe this wal-index): always granted. They
// still delimit the lane's transaction exactly as measured on the stock WAL.
static int lm_shm_lock (sqlite3_file *pf, int ofst, int n, int flags) {
    mw_lane *lane = FILE_LANE(pf);
    mw_ev(MW_EV_SHMLOCK, (mw_file *)pf, ofst, n, flags);
    if (n == 1) return mw_lane_on_shm_lock(lane, ofst, flags);
    return SQLITE_OK;
}
static void lm_shm_barrier (sqlite3_file *pf) { mw_ev(MW_EV_SHMBARRIER, (mw_file *)pf, 0, 0, 0); atomic_signal_fence(memory_order_seq_cst); }     // (the wal-index is private to the lane: only the compiler must not reorder)
static int lm_shm_unmap (sqlite3_file *pf, int del) {
    mw_ev(MW_EV_SHMUNMAP, (mw_file *)pf, del, 0, 0);
    return SQLITE_OK;                                          // regions are freed with the lane
}
static int lm_fetch (sqlite3_file *pf, sqlite3_int64 off, int n, void **pp) { *pp = NULL; return SQLITE_OK; }   // no mmap
static int lm_unfetch (sqlite3_file *pf, sqlite3_int64 off, void *p) { return SQLITE_OK; }

void mw_lane_methods_init (const sqlite3_io_methods *pass) {
    pass_io = pass;
    lane_main_io = *pass;
    lane_main_io.iVersion = 3;
    lane_main_io.xRead = lm_read;
    lane_main_io.xWrite = lm_write;
    lane_main_io.xTruncate = lm_truncate;
    lane_main_io.xSync = lm_sync;
    lane_main_io.xFileSize = lm_filesize;
    lane_main_io.xLock = lm_lock;
    lane_main_io.xUnlock = lm_unlock;
    lane_main_io.xCheckReservedLock = lm_check_reserved;
    lane_main_io.xFileControl = lm_file_control;
    lane_main_io.xShmMap = lm_shm_map;
    lane_main_io.xShmLock = lm_shm_lock;
    lane_main_io.xShmBarrier = lm_shm_barrier;
    lane_main_io.xShmUnmap = lm_shm_unmap;
    lane_main_io.xFetch = lm_fetch;
    lane_main_io.xUnfetch = lm_unfetch;

    memset(&lane_wal_io, 0, sizeof(lane_wal_io));
    lane_wal_io.iVersion = 1;
    lane_wal_io.xClose = wal_close;
    lane_wal_io.xRead = wal_read;
    lane_wal_io.xWrite = wal_write;
    lane_wal_io.xTruncate = wal_truncate;
    lane_wal_io.xSync = wal_sync;
    lane_wal_io.xFileSize = wal_filesize;
    lane_wal_io.xLock = wal_nolock;
    lane_wal_io.xUnlock = wal_nolock;
    lane_wal_io.xCheckReservedLock = wal_check_reserved;
    lane_wal_io.xFileControl = wal_file_control;
    lane_wal_io.xSectorSize = wal_sector_size;
    lane_wal_io.xDeviceCharacteristics = wal_device_char;
}

// The first lane of a database creates its page store from the real file's header.
int mw_lane_open_main (mw_file *f, mw_lane *lane) {
    mw_db *db = lane->db;
    uint8_t h[100];
    sqlite3_int64 size = 0;
    int rc = f->real->pMethods->xRead(f->real, h, sizeof h, 0);
    if (rc == SQLITE_OK) rc = f->real->pMethods->xFileSize(f->real, &size);
    if (rc != SQLITE_OK) return rc;
    if (h[52] || h[53] || h[54] || h[55]) return SQLITE_CANTOPEN;          // auto_vacuum / incremental_vacuum: page relocation is not supported
    int pgsz = (h[16] << 8) | h[17];
    if (pgsz == 1) pgsz = 65536;
    if (pgsz < 512 || (pgsz & (pgsz - 1))) return SQLITE_NOTADB;

    sqlite3_mutex_enter(db->mu);
    if (!db->store) {
        db->store = db->mp_req ? mw_store_create_light(pgsz, (uint32_t)(size / pgsz)) : mw_store_create(pgsz, (uint32_t)(size / pgsz));
        if (db->store && db->base_cache_bytes) db->store->base_limit = db->base_cache_bytes;
        if (!db->store) rc = SQLITE_NOMEM;
        else {
            db->store->reserved = h[20];
            if (db->mp_req) rc = mw_mp_open(db);                // shared header + lock file (first process initialises it)
            if (rc == SQLITE_OK) rc = db->mp_req ? mw_shared_open(db) : mw_log_open(db, pgsz);    // create the commit log or recover committed state from it
            if (rc == SQLITE_OK && db->mp) rc = mw_mp_finish_open(db);
            if (rc == SQLITE_OK && db->mp_req) rc = mw_shared_open_finish(db);
            if (rc != SQLITE_OK) { mw_store_free(db->store); db->store = NULL; }
        }
    } else if (db->store->pgsz != pgsz) rc = SQLITE_MISUSE;
    sqlite3_mutex_leave(db->mu);
    if (rc != SQLITE_OK) return rc;
    if (db->mp) { lane->mp_slot = mw_mp_slot_alloc(db); if (lane->mp_slot < 0) return SQLITE_FULL; }
    f->base.pMethods = &lane_main_io;
    lane->file = f;
    tls_lane = lane;
    return SQLITE_OK;
}

// MARK: - bootstrap -

// Private lanes require the database file to be in WAL mode (the pager then never rewrites the
// header from a lane). New/rollback-mode files are converted with a private stock connection on
// the underlying VFS; if a real WAL is left over from stock use it is checkpointed by that close.
// True if the file is a database in WAL mode with no real -wal that holds committed frames: what the engine can open without writing to it.
bool mw_path_is_clean_wal_db (const char *path) {
    size_t n = strlen(path); char *wp = malloc(n + 5); if (!wp) return false;
    memcpy(wp, path, n); memcpy(wp + n, "-wal", 5);
    struct stat sb; bool stale = stat(wp, &sb) == 0 && sb.st_size > 0; free(wp);
    FILE *fp = fopen(path, "rb"); if (!fp) return false;
    uint8_t h[100]; size_t got = fread(h, 1, sizeof h, fp); fclose(fp);
    return got == sizeof h && h[18] == 2 && h[19] == 2 && !stale;
}

int mw_ensure_wal_db (const char *path) {
    // A real -wal left by stock use (a crashed process, the sqlite3 CLI) holds committed frames the lane store cannot see; and the compactor would
    // later overwrite the main file under it, so a stock connection replaying that WAL would corrupt newer pages. Checkpoint it first.
    bool stale_wal = false;
    {
        size_t n = strlen(path);
        char *wp = malloc(n + 5);
        if (!wp) return SQLITE_NOMEM;
        memcpy(wp, path, n); memcpy(wp + n, "-wal", 5);
        struct stat sb;
        stale_wal = stat(wp, &sb) == 0 && sb.st_size > 0;
        free(wp);
    }
    FILE *fp = fopen(path, "rb");
    if (fp) {
        uint8_t h[100];
        size_t n = fread(h, 1, sizeof h, fp);
        fclose(fp);
        if (n == sizeof h && h[18] == 2 && h[19] == 2 && !stale_wal) return SQLITE_OK;
    }
    sqlite3 *c = NULL;
    int rc = sqlite3_open_v2(path, &c, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_PRIVATECACHE, mw_root_vfs()->zName);
    if (rc == SQLITE_OK) {
        sqlite3_busy_timeout(c, 5000);                             // (two openers of a new database may convert it at the same time)
        rc = sqlite3_exec(c, "PRAGMA journal_mode=WAL", NULL, NULL, NULL);
        if (rc == SQLITE_OK && stale_wal) rc = sqlite3_exec(c, "PRAGMA wal_checkpoint(TRUNCATE)", NULL, NULL, NULL);
    }
    sqlite3_close(c);
    return rc;
}
