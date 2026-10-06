//
//  multiwriter_mp.c
//  sqlite-multiwriter
//
//  Multi-process mode (URI mw_mp=1): several processes, each with several connections, on one database.
//
//  The shared commit log is the sequencer. Shared state lives in "<db>-mwlock", mmapped by every process:
//  the committed epoch and log end, the db_version counter, the compaction target, and a registry of
//  *slots* (one per lane: pid, snapshot epoch, lowest unresolved db_version) and *processes*.
//  Each process keeps its own in-memory page store and brings it up to date by tailing the log.
//
//  Commit (multiwriter_pages.c, under mw_mp_lock = in-process mutex + fcntl lock):
//      catch up on the log  ->  validate against the (now complete) local store  ->  assign the next epoch
//      -> install + append the record (+ fsync if synchronous>=FULL)  ->  publish committed_epoch/log_end.
//  Publication is therefore serialised across ALL processes (one short critical section, fsync included at
//  synchronous=FULL: no cross-process group commit). Execution, page reads, validation of the next
//  transaction, rebases and reads are fully parallel.
//
//  Snapshot: a lane first catches up, then takes db->epoch. A snapshot must not fall below the compaction
//  target announced in the header (protocol below), so compaction never overwrites state a live snapshot
//  still needs.
//
//  Crashes: a process that dies loses only its uncommitted work (its record is visible only once the header
//  is updated, and that happens inside the publication lock, which the kernel releases on death). Its slots
//  are recognised by kill(pid, 0) and ignored/reclaimed, so it neither holds back compaction nor the
//  db_version export ceiling. If every process died, the next opener recovers from the log like a single
//  process would.
//

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <sched.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <stdio.h>
#include <time.h>
#include "multiwriter_io.h"
#include "multiwriter_internal.h"
#include "multiwriter_wait.h"

#define MP_MAGIC 0x4d574d5031303031ull       /* "MWMP1001" */
#define MP_LOCK_PUBLISH ((off_t)1 << 20)      /* fcntl lock bytes (beyond EOF is fine) */
#define MP_LOCK_COMPACT (((off_t)1 << 20) + 1)
#define MP_LOCK_REBASE  (((off_t)1 << 20) + 4)      /* one replay of the rebase at a time in all the processes (the kernel drops it with the process) */

static int fcntl_lock (int fd, off_t byte, int type, bool wait) {
    struct flock fl;
    memset(&fl, 0, sizeof fl);
    fl.l_type = (short)type; fl.l_whence = SEEK_SET; fl.l_start = byte; fl.l_len = 1;
    for (;;) {
        if (fcntl(fd, wait ? F_SETLKW : F_SETLK, &fl) == 0) return 0;
        if (errno == EINTR) continue;
        return -1;
    }
}

// Liveness is a kernel-released lock, not a pid probe: every process holds an fcntl write lock on its own byte of "<db>-mwlk" for as long as it
// is registered, and the kernel drops it when the process dies (also for a zombie that nobody has reaped yet, and whatever happens to its pid).
// A pid is alive if a registered process entry carries it and that entry's byte is locked by somebody else (F_GETLK). The header only holds
// pids of registered processes (slots, publication lock, DDL owner).
#define MP_LOCK_LIVE(i) (((off_t)1 << 20) + 16 + (off_t)(i))
static bool proc_locked (mw_db *db, int i) {
    struct flock fl;
    memset(&fl, 0, sizeof fl);
    fl.l_type = F_WRLCK; fl.l_whence = SEEK_SET; fl.l_start = MP_LOCK_LIVE(i); fl.l_len = 1;
    if (fcntl(db->mp_pubfd, F_GETLK, &fl) != 0) return true;              // cannot tell: assume alive (never steal on doubt)
    return fl.l_type != F_UNLCK;
}
static bool pid_alive (mw_db *db, int32_t pid) {
    if (pid <= 0) return false;
    if (pid == (int32_t)getpid()) return true;
    for (int i = 0; i < MW_MP_PROCS; i++)
        if (atomic_load(&db->shm->procs[i].pid) == pid && proc_locked(db, i)) return true;
    return false;
}

// MARK: - open / close -

// Everything in the header that carries our pid at the moment we register is of a process that died and whose pid we were given (a live process has its own pid): its snapshot slots, the
// publication lock, the DDL owner, its tickets. Left alone they would be taken for ours (alive) for as long as we live, and nobody would ever steal them.
static void mp_drop_own_stale (mw_db *db) {
    mw_shm *sh = db->shm; const int32_t me = (int32_t)getpid();
    for (int i = 0; i < MW_MP_SLOTS; i++) {
        int32_t p = atomic_load(&sh->slots[i].pid);
        if (p == me && atomic_compare_exchange_strong(&sh->slots[i].pid, &p, -1)) {
            atomic_store(&sh->slots[i].snap, MW_MP_NONE); atomic_store(&sh->slots[i].minres, 0); atomic_store(&sh->slots[i].writing, 0); atomic_store(&sh->slots[i].pid, 0);
        }
    }
    int32_t o = me; if (atomic_compare_exchange_strong(&sh->pub_owner, &o, 0)) db->mp_stale_owner = true;       // (it may have died inside a publication: the repair runs when the database is open)
    int32_t d = me; atomic_compare_exchange_strong(&sh->ddl_pid, &d, 0);
    for (int i = 0; i < 1024; i++) { int32_t t = me; atomic_compare_exchange_strong(&sh->pub_tk_pid[i], &t, 0); }
}

int mw_mp_open (mw_db *db) {
    db->mp_path = mw_sidecar_path(db->path, "mwlock");
    db->mp_pubpath = mw_sidecar_path(db->path, "mwlk");
    if (!db->mp_path || !db->mp_pubpath) return SQLITE_NOMEM;
    size_t len = (sizeof(mw_shm) + 4095) & ~(size_t)4095;
    for (int attempt = 0; attempt < 500; attempt++) {
        db->mp_lockfd = open(db->mp_path, O_RDWR | O_CREAT | O_NOFOLLOW, mw_file_mode(db->path));
        if (db->mp_lockfd < 0) return SQLITE_CANTOPEN;
        bool first = flock(db->mp_lockfd, LOCK_EX | LOCK_NB) == 0;         // nobody else alive: we (re)initialise
        if (!first && flock(db->mp_lockfd, LOCK_SH) != 0) { close(db->mp_lockfd); db->mp_lockfd = -1; return SQLITE_BUSY; }   // blocks while the first process initialises
        // The last process to close unlinks the lock files while holding the exclusive lock: if we were waiting on that lock we hold a
        // deleted inode that nobody else will ever find (two "first" processes on one database). Start over on a stale file.
        struct stat fs, ps;
        if (fstat(db->mp_lockfd, &fs) != 0) { close(db->mp_lockfd); db->mp_lockfd = -1; return SQLITE_IOERR; }
        if (stat(db->mp_path, &ps) != 0 || ps.st_ino != fs.st_ino || ps.st_dev != fs.st_dev) { close(db->mp_lockfd); db->mp_lockfd = -1; continue; }
        // publication/compaction byte locks live on a file of their own: on BSD/macOS flock() and fcntl() locks on
        // one file conflict with each other, and every process holds a shared flock on the header file. (Opened only now: while
        // we hold the header lock nobody can unlink it.)
        db->mp_pubfd = open(db->mp_pubpath, O_RDWR | O_CREAT | O_NOFOLLOW, mw_file_mode(db->path));
        if (db->mp_pubfd < 0) { close(db->mp_lockfd); db->mp_lockfd = -1; return SQLITE_CANTOPEN; }
        // (every failure from here on gives the files back: the flock on the header file is what the other processes wait for while the first one initialises it)
        #define MP_OPEN_FAIL(code) do { close(db->mp_pubfd); db->mp_pubfd = -1; close(db->mp_lockfd); db->mp_lockfd = -1; return (code); } while (0)
        if ((size_t)fs.st_size < len && mw_io_ftruncate(db->mp_lockfd, (off_t)len) != 0) MP_OPEN_FAIL(SQLITE_IOERR);
        void *m = mw_io_mmap(NULL, len, PROT_READ | PROT_WRITE, MAP_SHARED, db->mp_lockfd, 0);
        if (m == MAP_FAILED) MP_OPEN_FAIL(SQLITE_IOERR);
        if (!first && (((mw_shm *)m)->magic != MP_MAGIC || !atomic_load(&((mw_shm *)m)->ready))) {
            // the process that was initialising it failed: release and try to become the first ourselves
            munmap(m, len); close(db->mp_pubfd); db->mp_pubfd = -1; close(db->mp_lockfd); db->mp_lockfd = -1;
            struct timespec ts = { 0, 2000000 }; nanosleep(&ts, NULL);
            continue;
        }
        db->shm = m;
        db->mp_first = first;
        pthread_mutex_init(&db->mp_mu, NULL);
        if (first) {
            memset(db->shm, 0, len);
            for (int i = 0; i < MW_MP_SLOTS; i++) atomic_init(&db->shm->slots[i].snap, MW_MP_NONE);
            db->shm->magic = MP_MAGIC;
        }
        db->mp = true;
        // Register this process *before* the log is opened, and remember the log generation we start from: if the log is reset or rewritten
        // while we replay it, the catch-up sees a different generation and repositions instead of trusting the file we opened.
        db->mp_proc = -1;
        for (int i = 0; i < MW_MP_PROCS && db->mp_proc < 0; i++) {
            if (fcntl_lock(db->mp_pubfd, MP_LOCK_LIVE(i), F_WRLCK, false) != 0) continue;     // held by a live process: next entry
            atomic_store(&db->shm->procs[i].applied, 0);
            atomic_store(&db->shm->procs[i].pid, (int32_t)getpid());
            db->mp_proc = i;
        }
        if (db->mp_proc < 0) { db->mp = false; db->shm = NULL; munmap(m, len); MP_OPEN_FAIL(SQLITE_FULL); }
        if (!first) mp_drop_own_stale(db);
        if (!first) { db->mp_gen = MW_LOG_GEN(atomic_load(&db->shm->log_pos)); db->mp_base_seen = atomic_load(&db->shm->base_epoch); }
        return SQLITE_OK;
    }
    return SQLITE_BUSY;
}

int mw_mp_finish_open (mw_db *db) {
    mw_shm *sh = db->shm;
    if (db->mp_first) {
        sh->pgsz = (uint32_t)db->store->pgsz;
        atomic_store(&sh->committed_epoch, atomic_load(&db->epoch));
        atomic_store(&sh->log_pos, MW_LOG_POS(1, db->log_off));
        atomic_store(&sh->log_ready, db->logfile_size);                 // (what the file holds now: written bytes)
        atomic_store(&sh->base_epoch, db->base_epoch);
        atomic_store(&sh->compact_T, db->base_epoch);
        atomic_store(&sh->schema_epoch, 1);
        atomic_store(&sh->dbv_counter, 0);
        atomic_store(&sh->ready, 1);
        flock(db->mp_lockfd, LOCK_SH);                                  // downgrade: others may proceed
    }
    if (db->mp_first) { db->mp_gen = MW_LOG_GEN(atomic_load(&sh->log_pos)); db->mp_base_seen = atomic_load(&sh->base_epoch); }
    atomic_store(&sh->procs[db->mp_proc].applied, atomic_load(&db->epoch));
    db->mp_lazy = getenv("MW_MP_LAZY") != NULL;                         // opt-in: the catch-up installs versions that point into the shared log (measured slower, see docs §40)
    return SQLITE_OK;
}

void mw_mp_close (mw_db *db, bool *sole) {
    *sole = false;
    if (!db->mp) return;
    if (db->mp_proc >= 0) {
        atomic_store(&db->shm->procs[db->mp_proc].applied, 0); atomic_store(&db->shm->procs[db->mp_proc].pid, 0);
        fcntl_lock(db->mp_pubfd, MP_LOCK_LIVE(db->mp_proc), F_UNLCK, false);
    }
    flock(db->mp_lockfd, LOCK_UN);
    *sole = flock(db->mp_lockfd, LOCK_EX | LOCK_NB) == 0;                // nobody else holds it: last process
}

// MARK: - publication lock, catch-up -

#if defined(__aarch64__)
#define MP_RELAX() __asm__ __volatile__("yield")
#elif defined(__x86_64__)
#define MP_RELAX() __asm__ __volatile__("pause")
#else
#define MP_RELAX() ((void)0)
#endif

// Publication lock: an atomic flag in the shared header (a fcntl lock costs two syscalls per commit and showed up
// as the top cost with 8 processes). The holder's pid is stored, so a lock left by a crashed process is detected
// (kill(pid, 0)) and stolen; that is safe because the critical section only makes its record visible at its very
// end (log_end/committed_epoch), so a holder that died earlier left nothing but unpublished bytes.
// Waiting for the publication lock: after a short spin, sleep this many microseconds between polls (MW_MP_SLEEP_US, default 50; 0 = sched_yield). Yielding keeps every
// waiting process runnable and 16-32 processes on 18 cores then fight for the cores the holder needs (measured: 16 processes 5.7k -> 8.0k tx/s with 50 us, 8 processes 8.1k -> 10.4k).
int mw_mp_catchup_locked (mw_db *db);
static int mp_sleep_us (void) { static _Atomic int c = MW_KNOB_UNSET; return mw_knob_int(&c, "MW_MP_SLEEP_US", 50); }

// Sleeping between polls pays only when many processes compete (with 2-4 the hand-off latency of a sleep costs more than the yields: 4 processes 13-15k -> 11-12k).
static bool mp_crowded (mw_db *db) {
    static _Atomic int mp_c = MW_KNOB_UNSET; const int min_procs = mw_knob_int(&mp_c, "MW_MP_SLEEP_MIN_PROCS", 6);
    int n = 0;
    for (int i = 0; i < MW_MP_PROCS; i++) if (atomic_load_explicit(&db->shm->procs[i].pid, memory_order_relaxed) > 0) n++;
    return n >= min_procs;
}

static bool mp_ticket_lock (void) { static _Atomic int c = MW_KNOB_UNSET; return !mw_knob_flag(&c, "MW_MP_LOCK_SPIN"); }

// The publication lock as a FIFO ticket queue in the shared header. With the polling lock every waiting process woke up every 50 us (64 processes: over a million
// wake-ups a second competing with the holder for the cores); here only the first two in the queue poll fast, the others sleep for about as long as the processes ahead
// of them need, and the lock goes to the next ticket in order. Waiting processes apply the commits published meanwhile, so the holder has only the last record left.
void mw_mp_lock (mw_db *db) {
    pthread_mutex_lock(&db->mp_mu);
    mw_shm *sh = db->shm;
    int32_t me = (int32_t)getpid();
    if (mp_ticket_lock()) {
        uint64_t t = atomic_fetch_add(&sh->pub_ticket, 1);
        atomic_store(&sh->pub_tk_pid[t % 1024], me);
        uint64_t stall_s = UINT64_MAX, stall_ns = 0;                                                    // (the ticket being served and since when: one whose owner never wrote its pid)
        for (unsigned polls = 0; ; polls++) {
            uint32_t wk = atomic_load_explicit(&sh->pub_wake[t % 1024], memory_order_acquire);          // (sampled before the queue is looked at: a wake in between is not lost)
            uint64_t s = atomic_load_explicit(&sh->pub_serving, memory_order_acquire);
            if (s == t) {
                int32_t exp = 0;
                if (atomic_compare_exchange_strong_explicit(&sh->pub_owner, &exp, me, memory_order_acquire, memory_order_relaxed)) { return; }
                if (exp != me && (polls & 31) == 31 && !pid_alive(db, exp) && atomic_compare_exchange_strong(&sh->pub_owner, &exp, me)) { db->mp_recheck = true; if (db->shared) mw_shared_repair(db); return; }   // the holder died
            } else if (s > t) {                                                                             // our ticket was skipped (we looked dead for too long): a new one
                t = atomic_fetch_add(&sh->pub_ticket, 1);
                atomic_store(&sh->pub_tk_pid[t % 1024], me);
                stall_s = UINT64_MAX;
                continue;
            } else if (s < t && (polls & 31) == 31) {
                int32_t pid = atomic_load(&sh->pub_tk_pid[s % 1024]);
                if (pid == 0) {                                                                         // the owner of this ticket has not written its pid: it is a moment from it, or it died between taking the ticket and that, and nobody would ever skip it
                    uint64_t nowns = mw_stage_now();
                    if (stall_s != s) { stall_s = s; stall_ns = nowns; }
                    else if (nowns - stall_ns > 500000000ull && atomic_load(&sh->pub_owner) == 0) {
                        uint64_t ss = s; if (atomic_compare_exchange_strong(&sh->pub_serving, &ss, s + 1)) atomic_store_explicit(&sh->pub_tk_pid[s % 1024], 0, memory_order_relaxed);
                        stall_s = UINT64_MAX;
                    }
                } else stall_s = UINT64_MAX;
                if (pid > 0 && (pid != me && !pid_alive(db, pid))) { if (getenv("MW_DEBUG")) fprintf(stderr, "pid %d: skipping ticket %llu of pid %d (owner %d)\n", (int)me, (unsigned long long)s, (int)pid, (int)atomic_load(&sh->pub_owner)); if (atomic_compare_exchange_strong(&sh->pub_serving, &s, s + 1)) atomic_store_explicit(&sh->pub_tk_pid[s % 1024], 0, memory_order_relaxed); }   // (slot cleared once its ticket is past: a dead pid left by the ticket 1024 earlier must not get a fresh ticket skipped) a queued process that gave up (-1) or died: skip its ticket
            }
            uint64_t ahead = s < t ? t - s : 0;
            if (polls == 3000 && getenv("MW_DEBUG")) fprintf(stderr, "pid %d: lock stuck: ticket %llu serving %llu owner %d tk_pid[s] %d shared %d\n", (int)me, (unsigned long long)t, (unsigned long long)s, (int)atomic_load(&sh->pub_owner), (int)atomic_load(&sh->pub_tk_pid[s % 1024]), (int)db->shared);
            // (only near the head of the queue: every waiter applying every commit as it appears is N times the work of a commit, and past ~64 processes that work is what the cores are spent on)
            if (ahead <= 4 && atomic_load_explicit(&sh->committed_epoch, memory_order_acquire) != atomic_load_explicit(&db->epoch, memory_order_acquire) && atomic_load(&db->failed) == 0) mw_mp_catchup_locked(db);
            static _Atomic int fast_c = MW_KNOB_UNSET, per_c = MW_KNOB_UNSET, cap_c = MW_KNOB_UNSET;
            const int fast = mw_knob_int(&fast_c, "MW_MP_FAST", 2); (void)per_c; (void)cap_c;
            if (ahead <= (uint64_t)fast) { if (polls < 400) MP_RELAX(); else sched_yield(); }
            else { mw_wait_u32(&sh->pub_wake[t % 1024], wk, 5000); }                                    // (woken by the unlock that brings us within 2 of the head; the timeout is for the liveness checks above)
        }
    }
    for (unsigned spin = 0; ; spin++) {
        int32_t exp = 0;
        if (atomic_load_explicit(&sh->pub_owner, memory_order_relaxed) == 0 && atomic_compare_exchange_strong_explicit(&sh->pub_owner, &exp, me, memory_order_acquire, memory_order_relaxed)) return;
        if ((spin & 255) == 255) {
            int32_t owner = atomic_load(&sh->pub_owner);
            if (owner != 0 && owner != me && !pid_alive(db, owner) && atomic_compare_exchange_strong(&sh->pub_owner, &owner, me)) { db->mp_recheck = true; if (db->shared) mw_shared_repair(db); }     // steal from a dead process
            if (atomic_load(&sh->pub_owner) == me) return;
        }
        if (atomic_load_explicit(&sh->committed_epoch, memory_order_acquire) != atomic_load_explicit(&db->epoch, memory_order_acquire) && atomic_load(&db->failed) == 0) mw_mp_catchup_locked(db);
        if (spin < 400) MP_RELAX();
        else if (mp_sleep_us() > 0 && mp_crowded(db)) { struct timespec ts = { 0, (long)mp_sleep_us() * 1000 }; nanosleep(&ts, NULL); }
        else sched_yield();
    }
}
void mw_mp_unlock (mw_db *db) {
    mw_shm *sh = db->shm;
    atomic_store_explicit(&sh->pub_owner, 0, memory_order_release);
    if (mp_ticket_lock()) {
        atomic_store_explicit(&sh->pub_tk_pid[atomic_load_explicit(&sh->pub_serving, memory_order_relaxed) % 1024], 0, memory_order_relaxed);   // (the slot of the ticket that is over: a stale pid, dead by now, must not get the ticket 1024 later skipped)
        uint64_t s = atomic_fetch_add_explicit(&sh->pub_serving, 1, memory_order_release) + 1;
        uint64_t nt = atomic_load_explicit(&sh->pub_ticket, memory_order_relaxed);
        if (nt > s + 2) { _Atomic uint32_t *w = &sh->pub_wake[(s + 2) % 1024]; atomic_fetch_add_explicit(w, 1, memory_order_release); mw_wake_u32(w, true); }     // the waiter that is now 2 behind the head starts spinning
    }      // (only the holder advances it, or a waiter that skips a dead ticket)
    pthread_mutex_unlock(&db->mp_mu);
}

bool mw_mp_pid_alive (mw_db *db, int32_t pid) { return pid_alive(db, pid); }

int mw_mp_catchup_locked_impl (mw_db *db);
int mw_mp_catchup_locked (mw_db *db) { uint64_t t0 = MW_T0(); int rc = mw_mp_catchup_locked_impl(db); MW_T1(MW_ST_MP_CATCH, t0); return rc; }
int mw_mp_catchup_locked_impl (mw_db *db) {
    if (db->shared) return SQLITE_OK;
    mw_shm *sh = db->shm;
    if (db->mp_recheck && atomic_load(&sh->pub_owner) == (int32_t)getpid()) {
        // The previous holder died with the lock. If it was replacing the log (rename done, header not yet updated) the path now names a different
        // file than the header describes, and the offsets in the header are those of the old one: adopt the new file and publish a new generation.
        db->mp_recheck = false;
        struct stat a, b;
        if (fstat(db->logfd, &a) == 0 && stat(db->logpath, &b) == 0 && (a.st_ino != b.st_ino || a.st_dev != b.st_dev)) {
            uint64_t old = atomic_load(&sh->log_pos);
            if (mw_log_reopen(db) == SQLITE_OK) {
                mw_log_remap_ro(db, 0);
                uint64_t vend = mw_log_scan_after(db, ~(uint64_t)0, db->logfile_size);       // end of the valid records of the new file
                atomic_store_explicit(&sh->log_ready, db->logfile_size, memory_order_release);
                db->mp_gen = MW_LOG_GEN(old) + 1;
                atomic_store_explicit(&sh->log_pos, MW_LOG_POS(db->mp_gen, vend), memory_order_release);
                db->mp_gen = MW_LOG_GEN(old);                                             // (the normal path below repositions us on the new generation)
            }
        }
    }
    uint64_t pos = atomic_load_explicit(&sh->log_pos, memory_order_acquire);           // (generation, end) in one word: always a consistent pair
    uint64_t gen = MW_LOG_GEN(pos), end = MW_LOG_END(pos);
    uint64_t tgen0 = MW_T0();
    if (gen != db->mp_gen) {                                              // the log was reset or rewritten by another process
        for (;;) {
            db->mp_gen = gen;
            uint64_t tro0 = MW_T0();
            int rrc = mw_log_reopen(db);
            MW_T1(MW_ST_MP_REOPEN, tro0);
            if (rrc != SQLITE_OK) { if (getenv("MW_DEBUG")) fprintf(stderr, "pid %d: catch-up: log reopen failed rc=%d errno=%d (gen %llu)\n", (int)getpid(), rrc, errno, (unsigned long long)gen); atomic_store(&db->failed, 1); return rrc; }
            uint64_t B = mw_log_header_base(db);
            uint64_t e = atomic_load(&db->epoch);
            if (e < B) {                                                  // the records we still need are gone: the real file has them
                struct stat sb;
                uint32_t pages = stat(db->path, &sb) == 0 ? (uint32_t)((uint64_t)sb.st_size / (uint64_t)db->store->pgsz) : db->store->base_dbsize;
                mw_store_flush(db->store, pages, B);
                atomic_store(&db->epoch, B); atomic_store(&db->next_epoch, B);
                db->written_upto = db->synced_upto = B;
                e = B;
            }
            // The header may already say "everything up to B is in the real file" while the compactor has not restarted the log yet (the old records are
            // still there), or it may have restarted it: the first record newer than what we have is where to continue in both cases (not necessarily offset 64).
            uint64_t pos2 = atomic_load_explicit(&sh->log_pos, memory_order_acquire);
            if (MW_LOG_GEN(pos2) != gen) { gen = MW_LOG_GEN(pos2); end = MW_LOG_END(pos2); continue; }      // reset again meanwhile: start over on the new generation
            end = MW_LOG_END(pos2);                                       // (`end` read before the reopen may be stale: the log only grows within a generation)
            uint64_t tsc0 = MW_T0();
            db->log_off = mw_log_scan_after(db, e, end);
            MW_T1(MW_ST_MP_SCAN, tsc0);
            break;
        }
        MW_T1(MW_ST_MP_GEN, tgen0);
    }
    int rc = SQLITE_OK;
    if (db->log_off < end) {
        mw_log_remap_ro(db, end);
        while (db->log_off < end) {
            uint64_t size = 0, epoch = 0;
            rc = mw_log_apply_at(db, db->log_off, &size, &epoch);
            if (rc != SQLITE_OK) { if (getenv("MW_DEBUG")) fprintf(stderr, "pid %d: catch-up: apply_at failed rc=%d at off=%llu end=%llu epoch=%llu gen=%llu\n", (int)getpid(), rc, (unsigned long long)db->log_off, (unsigned long long)end, (unsigned long long)atomic_load(&db->epoch), (unsigned long long)gen); atomic_store(&db->failed, 1); return rc; }
            db->log_off += size;
            atomic_store(&db->next_epoch, epoch);
            db->written_upto = db->synced_upto = epoch;
            atomic_store_explicit(&db->epoch, epoch, memory_order_release);
        }
    }
    uint64_t base = atomic_load(&sh->base_epoch);
    if (base > db->mp_base_seen) {                                        // another process compacted: our copies <= base are redundant
        db->mp_base_seen = base;
        pthread_mutex_lock(&db->store->seq_mu); if (base > db->store->compacted_epoch) db->store->compacted_epoch = base; pthread_mutex_unlock(&db->store->seq_mu);
        mw_store_drop_dirty_upto(db->store, base);
    }
    uint64_t sc = atomic_load(&sh->schema_epoch);
    if (sc > atomic_load(&db->last_schema_epoch)) atomic_store(&db->last_schema_epoch, sc);
    atomic_store(&sh->procs[db->mp_proc].applied, atomic_load(&db->epoch));
    return SQLITE_OK;
}

int mw_mp_catchup (mw_db *db) {
    if (db->shared) return SQLITE_OK;                                  // (shared mode: nothing to apply, the index is shared)
    if (atomic_load_explicit(&db->shm->committed_epoch, memory_order_acquire) == atomic_load_explicit(&db->epoch, memory_order_acquire) &&
        MW_LOG_GEN(atomic_load(&db->shm->log_pos)) == db->mp_gen && atomic_load(&db->shm->base_epoch) == db->mp_base_seen) return SQLITE_OK;   // fast path
    pthread_mutex_lock(&db->mp_mu);
    int rc = mw_mp_catchup_locked(db);
    pthread_mutex_unlock(&db->mp_mu);
    return rc;
}

void mw_mp_publish_header (mw_db *db, uint64_t epoch) {
    atomic_store_explicit(&db->shm->log_pos, MW_LOG_POS(db->mp_gen, db->log_off), memory_order_release);
    atomic_store_explicit(&db->shm->committed_epoch, epoch, memory_order_release);
    atomic_store(&db->shm->procs[db->mp_proc].applied, epoch);
}

// MARK: - slots: snapshots and db_version reservations -

static void slot_reclaim_dead (mw_db *db);
void mw_mp_reap_dead_slots (mw_db *db) { slot_reclaim_dead(db); }
static void slot_reclaim_dead (mw_db *db) {
    mw_shm *sh = db->shm;
    for (int i = 0; i < MW_MP_SLOTS; i++) {
        int32_t pid = atomic_load(&sh->slots[i].pid);
        if (pid > 0 && !pid_alive(db, pid)) {          // (-1 is a reclaimer at work: never reclaim it twice)
            if (atomic_compare_exchange_strong(&sh->slots[i].pid, &pid, -1)) {           // -1 = being cleaned
                atomic_store(&sh->slots[i].snap, MW_MP_NONE);
                atomic_store(&sh->slots[i].minres, 0);
                atomic_store(&sh->slots[i].pid, 0);
            }
        }
    }
}

int mw_mp_slot_alloc (mw_db *db) {
    mw_shm *sh = db->shm;
    for (int pass = 0; pass < 2; pass++) {
        for (int i = 0; i < MW_MP_SLOTS; i++) {
            int32_t exp = 0;
            if (atomic_compare_exchange_strong(&sh->slots[i].pid, &exp, (int32_t)getpid())) {
                atomic_store(&sh->slots[i].snap, MW_MP_NONE);
                atomic_store(&sh->slots[i].minres, 0);
                atomic_store(&sh->slots[i].writing, 0);                         // (a slot that a dead process left in a write transaction would make a DDL of another wait the full 2 s)
                return i;
            }
        }
        slot_reclaim_dead(db);
    }
    return -1;
}

void mw_mp_slot_free (mw_db *db, int slot) {
    if (slot < 0) return;
    atomic_store(&db->shm->slots[slot].snap, MW_MP_NONE);
    atomic_store(&db->shm->slots[slot].minres, 0);
    atomic_store(&db->shm->slots[slot].pid, 0);
}

// Publishes the snapshot epoch in the lane's slot. The caller read it from db->epoch and must re-check
// compact_T afterwards: a compactor announces its target, fences, then re-scans the slots, so either it
// sees this snapshot (and lowers its target) or we see its target (and refresh to a newer snapshot).
void mw_mp_snapshot_register (mw_lane *lane) {
    atomic_store(&lane->db->shm->slots[lane->mp_slot].snap, lane->tx.snapshot_epoch);   // seq_cst
    atomic_thread_fence(memory_order_seq_cst);
}

uint64_t mw_mp_global_oldest (mw_db *db) {
    mw_shm *sh = db->shm;
    uint64_t oldest = atomic_load(&sh->committed_epoch);
    for (int i = 0; i < MW_MP_SLOTS; i++) {
        int32_t pid = atomic_load(&sh->slots[i].pid);
        if (pid <= 0) continue;
        uint64_t s = atomic_load(&sh->slots[i].snap);
        if (s != MW_MP_NONE && s < oldest && pid_alive(db, pid)) oldest = s;
    }
    return oldest;
}

// MARK: - compaction coordination and log reset -

void mw_mp_rebase_lock (mw_db *db) { fcntl_lock(db->mp_pubfd, MP_LOCK_REBASE, F_WRLCK, true); }
void mw_mp_rebase_unlock (mw_db *db) { fcntl_lock(db->mp_pubfd, MP_LOCK_REBASE, F_UNLCK, false); }
bool mw_mp_compaction_lock (mw_db *db) { return fcntl_lock(db->mp_pubfd, MP_LOCK_COMPACT, F_WRLCK, false) == 0; }
void mw_mp_compaction_unlock (mw_db *db) { fcntl_lock(db->mp_pubfd, MP_LOCK_COMPACT, F_UNLCK, false); }

// Compaction target: announced, fenced, re-scanned (see mw_mp_snapshot_register).
uint64_t mw_mp_compaction_target (mw_db *db) {
    mw_shm *sh = db->shm;
    uint64_t t0 = mw_mp_global_oldest(db);
    uint64_t cur = atomic_load(&sh->compact_T);
    while (t0 > cur && !atomic_compare_exchange_weak(&sh->compact_T, &cur, t0)) {}
    atomic_thread_fence(memory_order_seq_cst);
    uint64_t t1 = mw_mp_global_oldest(db);
    return t1 < t0 ? t1 : t0;
}

// When every live process is fully caught up at epoch T == committed, restart the log at its header without
// truncating the file (another process may still have it mapped: truncating would SIGBUS it). Stale bytes past
// the new records cannot validate: record epochs must be contiguous.
// (There used to be a reset of the log in place here: when every process had applied everything, the end was set back to 64 and the old bytes were overwritten by new
// records. With lazy versions that point into the mapping of the log that would corrupt what they point at, so the log is only ever *replaced* by a new file
// (mw_mp_rewrite_log, below); an old file stays valid for the processes that still map it.)

// MARK: - schema barrier across processes -
//
// Same contract as the in-process barrier (multiwriter_tx.c): an optimisation of *when* a schema change runs, never
// needed for correctness (the schema cookie is validated at commit). One DDL at a time across all processes; other
// processes' new write transactions are refused with SQLITE_BUSY (their busy handler waits) while it runs, and the
// DDL first waits (bounded) for their in-flight writers.

void mw_mp_ddl_begin (mw_db *db) {
    mw_shm *sh = db->shm;
    int32_t me = (int32_t)getpid();
    for (int i = 0; i < 4000; i++) {                                   // <= ~2 s
        int32_t cur = atomic_load(&sh->ddl_pid);
        if (cur == me) break;
        if ((cur == 0 || !pid_alive(db, cur)) && atomic_compare_exchange_strong(&sh->ddl_pid, &cur, me)) break;
        struct timespec ts = { 0, 500000 };
        nanosleep(&ts, NULL);
    }
    if (atomic_load(&sh->ddl_pid) != me) return;
    for (int i = 0; i < 4000; i++) {                                   // drain other processes' writers
        bool busy = false;
        for (int k = 0; k < MW_MP_SLOTS && !busy; k++) {
            int32_t pid = atomic_load(&sh->slots[k].pid);
            if (pid > 0 && pid != me && atomic_load(&sh->slots[k].writing) && pid_alive(db, pid)) busy = true;
        }
        if (!busy) return;
        struct timespec ts = { 0, 500000 };
        nanosleep(&ts, NULL);
    }
}

void mw_mp_ddl_end (mw_db *db) {
    int32_t me = (int32_t)getpid();
    atomic_compare_exchange_strong(&db->shm->ddl_pid, &me, 0);
}

bool mw_mp_ddl_blocked (mw_db *db) {
    int32_t cur = atomic_load(&db->shm->ddl_pid);
    return cur != 0 && cur != (int32_t)getpid() && pid_alive(db, cur);
}

void mw_mp_writing (mw_lane *lane, bool on) {
    if (lane->mp_slot >= 0) atomic_store(&lane->db->shm->slots[lane->mp_slot].writing, on ? 1 : 0);
}

// Keeps the shared log bounded under continuous load: replace it by a file holding only the records newer than T
// (the compactor already made everything <= T durable in the real file). Other processes notice log_gen, reopen the
// path and reposition (mw_mp_catchup_locked). Needs the publication lock: no append may run meanwhile.
void mw_mp_rewrite_log (mw_db *db, uint64_t T) {
    uint64_t trw0 = MW_T0();
    mw_mp_lock(db);
    MW_T1(MW_ST_MP_REWRITE_WAIT, trw0);
    trw0 = MW_T0();
    mw_shm *sh = db->shm;
    if (mw_mp_catchup_locked(db) == SQLITE_OK && atomic_load(&sh->base_epoch) >= T && db->log_off > 8 * 4096 &&
        db->log_off > (uint64_t)db->store->pgsz * 64) {
        mw_store_materialize_lazy(db->store);                        // (before seq_mu: lock order; the old mapping goes away)
        mw_log_fill_hold(db);                                        // (no prefiller writes into the old file with the new file's extent)
        pthread_mutex_lock(&db->store->seq_mu);
        int rc = mw_log_rewrite_tail(db, T, NULL);                       // new file (header base = T) + tail, atomically renamed; we switch to it
        pthread_mutex_unlock(&db->store->seq_mu);
        if (rc == SQLITE_OK) {
            db->mp_gen++;
            atomic_store_explicit(&sh->log_ready, db->logfile_size, memory_order_release);      // (a new file: its written extent)
            atomic_store_explicit(&sh->log_pos, MW_LOG_POS(db->mp_gen, db->log_off), memory_order_release);
        }
        mw_log_fill_release(db);
    }
    MW_T1(MW_ST_MP_REWRITE, trw0);
    mw_mp_unlock(db);
}

// MARK: - admission control (many processes) -
//
// With N processes every commit costs every process a catch-up, all of them contend for the one publication lock, and past ~32 processes the throughput *falls* with N
// (64: 5k, 128: 2k tx/s against 9.6k at 32). It engages with more than 1.5 processes per core.. Only a limited number of writer transactions may be in flight at once; the others sleep before taking their snapshot (a
// sleeping process has nothing to catch up yet). FIFO tickets order the waiters, the first few poll the slot table, the rest sleep in proportion to their distance. A wait is
// bounded (a transaction that cannot get a slot in 50 ms goes ahead without one) so that a holder that stays open for long, or a dead one, can never stop the others.
static int mp_admit_cap (int nprocs) {
    static _Atomic int c = MW_KNOB_UNSET;
    long n = sysconf(_SC_NPROCESSORS_ONLN);
    int big = (int)(n * 2 < 2 ? 2 : n * 2 > 64 ? 64 : n * 2), small = (int)(n / 3 < 2 ? 2 : n / 3 > 16 ? 16 : n / 3);
    // 2 per core while the processes are up to ~14 per core; beyond that (1000 processes on 18 cores) a small number of writers at a time does better: 6 against 36 there is 7.3k against 5.2k tx/s
    int v = mw_knob_int(&c, "MW_MP_ADMIT", 0); if (v <= 0) v = nprocs > 14 * (int)n ? small : big;       // (0: not set; the default depends on the processes now there)
    return v > 64 ? 64 : v;
}

static void adm_raise (_Atomic uint64_t *a, uint64_t v) { uint64_t cur = atomic_load(a); while (cur < v && !atomic_compare_exchange_weak(a, &cur, v)) {} }

// How long a transaction waits for an admission slot before it goes ahead without one (a holder that stays open for long must not stop everybody). It has to be longer than
// the queue: with N processes and 6 slots a turn comes after N/6 transactions (~1 ms each): 50 ms was enough for 128 processes and made every one of 800 give up, which sent
// them all at the publication lock together (46 tx/s at 800-1000 processes).
static long long admit_wait_ns (void) { static _Atomic int c = MW_KNOB_UNSET; return (long long)mw_knob_int(&c, "MW_MP_ADMIT_WAIT_MS", 3000) * 1000000ll; }

static inline void adm_wake_head (mw_shm *sh, uint64_t h) {              // the waiter whose ticket is h (if any) re-checks
    _Atomic uint32_t *w = &sh->adm_wake[h % 1024];
    atomic_fetch_add_explicit(w, 1, memory_order_release);
    mw_wake_u32(w, true);
}

int mw_mp_admit (mw_lane *lane) {
    mw_db *db = lane->db;
    mw_shm *sh = db->shm;
    int cap = mp_admit_cap(atomic_load_explicit(&db->mp_nprocs, memory_order_relaxed));
    if (cap <= 0) return 0;
    if (lane->adm_check++ % 64 == 0) {                                     // (how many processes are there? not worth a scan per transaction)
        int n = 0; for (int i = 0; i < MW_MP_PROCS; i++) if (atomic_load_explicit(&sh->procs[i].pid, memory_order_relaxed) > 0) n++;
        static _Atomic int from_c = MW_KNOB_UNSET;
        const int from = mw_knob_int(&from_c, "MW_MP_ADMIT_FROM", (int)(sysconf(_SC_NPROCESSORS_ONLN) * 3 / 2));     // (below ~1.5 processes per core admission costs 10% and gains nothing)
        atomic_store_explicit(&db->mp_nprocs, n, memory_order_relaxed);
        lane->adm_crowded = n > cap && n > from;
    }
    if (!lane->adm_crowded) return 0;
    // A FIFO queue of tickets. The waiter whose turn it is takes a free slot; a release wakes the head, and a grab wakes the next one (it may find another slot free): a waiter
    // sleeps on its own word until it is the head, so nobody polls and a hand-off costs one wake-up.
    const int32_t me = (int32_t)getpid();
    const uint64_t t = atomic_fetch_add(&sh->adm_ticket, 1);
    atomic_store(&sh->adm_tk_pid[t % 1024], me);
    uint64_t tadm0 = MW_T0();
    struct timespec t0; clock_gettime(CLOCK_MONOTONIC, &t0);
    for (;;) {
        uint32_t w = atomic_load_explicit(&sh->adm_wake[t % 1024], memory_order_acquire);          // (sampled before the conditions are checked: a wake in between is not lost)
        uint64_t adm = atomic_load_explicit(&sh->adm_admitted, memory_order_acquire);
        uint64_t ahead = t > adm ? t - adm : 0;
        if (ahead == 0) {
            for (int i = 0; i < cap; i++) {
                int32_t exp = 0;
                if (atomic_load_explicit(&sh->adm_slot_pid[i], memory_order_relaxed) == 0 && atomic_compare_exchange_strong(&sh->adm_slot_pid[i], &exp, me)) {
                    adm_raise(&sh->adm_admitted, t + 1);
                    lane->adm_slot = i;
                    if (atomic_load_explicit(&sh->adm_ticket, memory_order_relaxed) > t + 1) adm_wake_head(sh, t + 1);       // the next may find a free slot too
                    MW_T1(MW_ST_ADM_WAIT, tadm0); lane->adm_t0 = MW_T0();
                    return 1;
                }
            }
        }
        struct timespec now; clock_gettime(CLOCK_MONOTONIC, &now);
        if ((now.tv_sec - t0.tv_sec) * 1000000000ll + (now.tv_nsec - t0.tv_nsec) > admit_wait_ns()) {      // go ahead without a slot
            adm_raise(&sh->adm_admitted, t + 1);
            if (atomic_load_explicit(&sh->adm_ticket, memory_order_relaxed) > t + 1) adm_wake_head(sh, t + 1);
            return 0;
        }
        uint32_t timeout_us = ahead == 0 ? 2000 : ahead < 8 ? 5000 : 50000;
        mw_wait_u32(&sh->adm_wake[t % 1024], w, timeout_us);
        if (atomic_load_explicit(&sh->adm_wake[t % 1024], memory_order_acquire) != w) continue;       // woken: our turn, or a slot was released
        // timed out: is who we wait for gone?
        if (ahead == 0) {                                                      // the head: a holder that died keeps a slot for ever
            for (int i = 0; i < cap; i++) { int32_t pid = atomic_load(&sh->adm_slot_pid[i]); if (pid > 0 && pid != me && !pid_alive(db, pid)) atomic_compare_exchange_strong(&sh->adm_slot_pid[i], &pid, 0); }
        } else if (ahead < 8) {                                                // near the head: the head waiter may have died
            int32_t pid = atomic_load(&sh->adm_tk_pid[adm % 1024]);
            if (pid > 0 && pid != me && !pid_alive(db, pid)) { adm_raise(&sh->adm_admitted, adm + 1); adm_wake_head(sh, adm + 1); }
        }
    }
}

void mw_mp_admit_release (mw_lane *lane) {
    if (lane->adm_slot < 0) return;
    MW_T1(MW_ST_ADM_HOLD, lane->adm_t0);
    if (mw_timing_on) { int busy = 0; int cap = mp_admit_cap(atomic_load(&lane->db->mp_nprocs)); for (int i = 0; i < cap; i++) if (atomic_load(&lane->db->shm->adm_slot_pid[i]) != 0) busy++; mw_count_add(MW_C_ADM_BUSY, (uint64_t)busy); mw_count_add(MW_C_ADM_SAMPLES, 1); uint64_t tk = atomic_load(&lane->db->shm->adm_ticket), ad = atomic_load(&lane->db->shm->adm_admitted); mw_count_add(MW_C_ADM_WAITERS, tk > ad ? tk - ad : 0); }
    { struct timespec nw; clock_gettime(CLOCK_MONOTONIC, &nw); atomic_store(&lane->db->shm->adm_slot_rel_ns[lane->adm_slot], (uint64_t)nw.tv_sec * 1000000000ull + (uint64_t)nw.tv_nsec); }
    atomic_store_explicit(&lane->db->shm->adm_slot_pid[lane->adm_slot], 0, memory_order_release);
    lane->adm_slot = -1;
    { mw_shm *sh = lane->db->shm; uint64_t adm = atomic_load(&sh->adm_admitted); if (atomic_load(&sh->adm_ticket) > adm) adm_wake_head(sh, adm); }          // a slot is free: the head waiter takes it
}
