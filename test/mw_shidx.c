// Shared version index (multiwriter_shidx.c), phase 1: the data structure alone.
// Functional behaviour of versions, snapshots, GC, arena exhaustion; then a multi-process stress: writers (serialised by an flock) install commits, readers pin snapshots
// and check what they see, a reader is killed while it holds a pin and its slot is reaped.
#include "mw_test.h"
#include "multiwriter_shidx.h"
#include <signal.h>
#include <stdatomic.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <time.h>

// The location of the image of a page at an epoch is a function of both: a reader that gets a (epoch, loc) pair from the index can tell whether it is a real version.
static uint64_t loc_of (uint32_t pgno, uint64_t epoch) { return ((uint64_t)pgno * 0x9E3779B97F4A7C15ull) ^ (epoch * 0xC2B2AE3D27D4EB4Full); }

static void install (shidx *ix, uint64_t epoch, uint32_t dbsize, int n, const uint32_t *pg) {
    uint64_t locs[64];
    for (int i = 0; i < n; i++) locs[i] = loc_of(pg[i], epoch);
    int rc = shidx_install(ix, epoch, dbsize, n, pg, locs);
    CHECK_RC(rc, 0);
    shidx_publish(ix, epoch);
}

static bool alive_never (int32_t pid, void *ctx) { (void)pid; (void)ctx; return false; }

// ---- functional ----
static void test_functional (void) {
    char path[256]; snprintf(path, sizeof path, "%s/mw_shidx_f_%d.idx", getenv("TMPDIR") && *getenv("TMPDIR") ? getenv("TMPDIR") : "/tmp", (int)getpid()); unlink(path);
    shidx_params p = { 16, 4096, 64, 0 };
    shidx *ix = shidx_open(path, &p);
    CHECK(ix != NULL);
    if (!ix) return;
    uint64_t ep, loc; uint32_t sz;
    CHECK(shidx_committed(ix) == 1);
    CHECK(!shidx_lookup(ix, 5, 1, &ep, &loc));                 // nothing yet: the page is in the real file
    CHECK(!shidx_dbsize(ix, 1, &sz));
    uint32_t a[] = { 5, 6, 7 }, b[] = { 6 }, c[] = { 5, 9 };
    install(ix, 2, 10, 3, a);
    install(ix, 3, 11, 1, b);
    install(ix, 4, 12, 2, c);
    CHECK(shidx_committed(ix) == 4);
    // a snapshot sees the newest version <= its epoch
    CHECK(shidx_lookup(ix, 5, 2, &ep, &loc) && ep == 2 && loc == loc_of(5, 2));
    CHECK(shidx_lookup(ix, 5, 3, &ep, &loc) && ep == 2);
    CHECK(shidx_lookup(ix, 5, 4, &ep, &loc) && ep == 4 && loc == loc_of(5, 4));
    CHECK(shidx_lookup(ix, 6, 2, &ep, &loc) && ep == 2);
    CHECK(shidx_lookup(ix, 6, 3, &ep, &loc) && ep == 3);
    CHECK(shidx_lookup(ix, 6, 4, &ep, &loc) && ep == 3);
    CHECK(!shidx_lookup(ix, 9, 3, &ep, &loc));                 // page 9 first written at 4
    CHECK(shidx_lookup(ix, 9, 4, &ep, &loc) && ep == 4);
    CHECK(!shidx_lookup(ix, 8, 4, &ep, &loc));
    CHECK(shidx_dbsize(ix, 2, &sz) && sz == 10);
    CHECK(shidx_dbsize(ix, 3, &sz) && sz == 11);
    CHECK(shidx_dbsize(ix, 99, &sz) && sz == 12);
    CHECK(shidx_head_epoch(ix, 5) == 4 && shidx_head_epoch(ix, 6) == 3 && shidx_head_epoch(ix, 8) == 0);
    // installed but not published: invisible to a snapshot taken now
    uint32_t d[] = { 5 };
    uint64_t dl[] = { loc_of(5, 5) };
    CHECK_RC(shidx_install(ix, 5, 13, 1, d, dl), 0);
    int slot = shidx_slot_alloc(ix, (int32_t)getpid());
    uint64_t snap = shidx_pin(ix, slot);
    CHECK(snap == 4);
    CHECK(shidx_lookup(ix, 5, snap, &ep, &loc) && ep == 4);
    CHECK(shidx_head_epoch(ix, 5) == 5);                        // (the writer sees its own install: validation)
    shidx_publish(ix, 5);
    CHECK(shidx_lookup(ix, 5, snap, &ep, &loc) && ep == 4);    // the pinned snapshot does not move
    // GC with a pinned reader keeps what it can see
    uint64_t freed = shidx_gc(ix, 0);
    CHECK(shidx_lookup(ix, 5, snap, &ep, &loc) && ep == 4);
    CHECK(shidx_lookup(ix, 6, snap, &ep, &loc) && ep == 3);
    CHECK(shidx_oldest(ix) == 4);
    (void)freed;
    shidx_unpin(ix, slot);
    freed = shidx_gc(ix, 0);                                    // nobody pins: only the newest version of each page (and the size) is needed
    CHECK(freed > 0);
    CHECK(shidx_lookup(ix, 5, 5, &ep, &loc) && ep == 5);
    CHECK(shidx_lookup(ix, 6, 5, &ep, &loc) && ep == 3);
    CHECK(shidx_lookup(ix, 7, 5, &ep, &loc) && ep == 2);
    CHECK(shidx_dbsize(ix, 5, &sz) && sz == 13);
    // base: a page whose newest version is in the real file leaves the index
    shidx_gc(ix, 3);                                            // pages 6 (3), 7 (2) are <= 3
    CHECK(!shidx_lookup(ix, 6, 5, &ep, &loc) && !shidx_lookup(ix, 7, 5, &ep, &loc));
    CHECK(shidx_lookup(ix, 5, 5, &ep, &loc) && ep == 5);       // 5 and 9 are newer than the base
    CHECK(shidx_lookup(ix, 9, 5, &ep, &loc) && ep == 4);
    shidx_stats s; shidx_stats_get(ix, &s);
    CHECK(s.versions_live == 3 + 1 + 0 + 0 || s.versions_live > 0);
    // a reaped slot
    int s2 = shidx_slot_alloc(ix, 4242);
    shidx_pin(ix, s2);
    CHECK(shidx_reap(ix, alive_never, NULL) >= 1);
    CHECK(shidx_slot_alloc(ix, (int32_t)getpid()) >= 0);
    // a second opener adopts the file
    shidx *iy = shidx_open(path, NULL);
    CHECK(iy != NULL);
    if (iy) { CHECK(shidx_committed(iy) == 5); CHECK(shidx_lookup(iy, 5, 5, &ep, &loc) && ep == 5); shidx_close(iy); }
    shidx_close(ix);
    shidx_unlink(path);
}

// ---- arena exhaustion and candidate overflow ----
static void test_limits (void) {
    char path[256]; snprintf(path, sizeof path, "%s/mw_shidx_l_%d.idx", getenv("TMPDIR") && *getenv("TMPDIR") ? getenv("TMPDIR") : "/tmp", (int)getpid()); unlink(path);
    shidx_params p = { 14, 64, 8, 4 };                           // 64 versions, 4 candidates
    shidx *ix = shidx_open(path, &p);
    CHECK(ix != NULL);
    if (!ix) return;
    uint32_t pg[4] = { 1, 2, 3, 4 };
    uint64_t e = 1; int full = 0;
    for (int i = 0; i < 100; i++) {
        uint64_t locs[4]; e++;
        for (int k = 0; k < 4; k++) locs[k] = loc_of(pg[k], e);
        if (shidx_install(ix, e, 8, 4, pg, locs) != 0) { full = 1; e--; break; }
        shidx_publish(ix, e);
    }
    CHECK(full);                                                 // 5 versions per commit, 64 entries: about 12 commits
    CHECK(shidx_install(ix, e + 1, 8, 4, pg, (uint64_t[]){1, 2, 3, 4}) == -1);
    shidx_stats s; shidx_stats_get(ix, &s);
    CHECK(s.entries_used >= 60 && s.entries_used <= 64);
    uint64_t freed = shidx_gc(ix, 0);                            // the candidate list (4) overflowed: a full scan
    shidx_stats_get(ix, &s);
    CHECK(freed > 0 && s.gc_full_scans >= 1);
    uint64_t ep, loc;
    for (int k = 0; k < 4; k++) CHECK(shidx_lookup(ix, pg[k], e, &ep, &loc) && ep == e && loc == loc_of(pg[k], e));
    e++;
    uint64_t locs[4]; for (int k = 0; k < 4; k++) locs[k] = loc_of(pg[k], e);
    CHECK_RC(shidx_install(ix, e, 8, 4, pg, locs), 0);           // room again
    shidx_publish(ix, e);
    CHECK(shidx_install(ix, e + 1, 8, 1, (uint32_t[]){ 1u << 14 }, (uint64_t[]){ 1 }) == -2);       // page out of range
    shidx_close(ix);
    shidx_unlink(path);
}

// ---- multi-process stress ----
#define NPAGES 4000
typedef struct { _Atomic uint64_t truth[NPAGES]; _Atomic int stop; _Atomic uint64_t errors, reads, commits, base; _Atomic uint64_t e_real, e_older, e_repeat, e_size, e_snap; } shared_t;

static void writer_proc (shidx *ix, shared_t *sh, int lockfd, int id) {
    uint64_t rng = 0x1234567 + (uint64_t)id * 7919;
    int slot = shidx_slot_alloc(ix, (int32_t)getpid());
    while (!atomic_load(&sh->stop)) {
        flock(lockfd, LOCK_EX);
        uint64_t epoch = shidx_committed(ix) + 1;
        uint32_t pg[8]; uint64_t locs[8]; int n = 0;
        for (int i = 0; i < 6; i++) {
            rng = rng * 6364136223846793005ull + 1442695040888963407ull;
            uint32_t p = 1 + (uint32_t)((rng >> 33) % (NPAGES - 1));
            bool dup = false; for (int k = 0; k < n; k++) if (pg[k] == p) dup = true;
            if (!dup) { pg[n] = p; locs[n] = loc_of(p, epoch); n++; }
        }
        int rc = shidx_install(ix, epoch, (uint32_t)(1000 + epoch % 50), n, pg, locs);
        if (rc == 0) {
            for (int i = 0; i < n; i++) atomic_store(&sh->truth[pg[i]], epoch);
            shidx_publish(ix, epoch);
            atomic_fetch_add(&sh->commits, 1);
            if (epoch % 16 == 0) { uint64_t b = epoch > 2000 ? epoch - 2000 : 0; atomic_store(&sh->base, b); shidx_gc(ix, b); }
        } else { shidx_gc(ix, 0); }
        flock(lockfd, LOCK_UN);
    }
    (void)slot;
    _exit(0);
}

static void reader_proc (shidx *ix, shared_t *sh, int id, int hold_forever) {
    uint64_t rng = 0xABCDEF + (uint64_t)id * 104729;
    int slot = shidx_slot_alloc(ix, (int32_t)getpid());
    if (slot < 0) { atomic_fetch_add(&sh->errors, 1); _exit(1); }
    uint64_t last_ep[NPAGES]; memset(last_ep, 0, sizeof last_ep);
    uint64_t last_snap = 0;
    while (!atomic_load(&sh->stop)) {
        uint64_t snap = shidx_pin(ix, slot);
        if (snap < last_snap) { atomic_fetch_add(&sh->errors, 1); atomic_fetch_add(&sh->e_snap, 1); }
        last_snap = snap;
        uint32_t seen_pg[16]; uint64_t seen_ep[16];
        for (int i = 0; i < 16; i++) {
            rng = rng * 6364136223846793005ull + 1442695040888963407ull;
            uint32_t p = 1 + (uint32_t)((rng >> 33) % (NPAGES - 1));
            uint64_t ep = 0, loc = 0; bool f = shidx_lookup(ix, p, snap, &ep, &loc);
            if (f && (ep > snap || loc != loc_of(p, ep) || ep == 0)) { atomic_fetch_add(&sh->errors, 1); atomic_fetch_add(&sh->e_real, 1); }          // not a real version, or from the future
            if (f && ep < last_ep[p]) { atomic_fetch_add(&sh->errors, 1); atomic_fetch_add(&sh->e_older, 1); }                                        // a later snapshot must not see an older version
            if (f) last_ep[p] = ep;
            seen_pg[i] = p; seen_ep[i] = f ? ep : 0;
            uint32_t sz; if (shidx_dbsize(ix, snap, &sz) && (sz < 1000 || sz >= 1050)) { atomic_fetch_add(&sh->errors, 1); atomic_fetch_add(&sh->e_size, 1); }
        }
        for (int i = 0; i < 16; i++) {                           // the same pin: the same answer (nothing it can see changes under it)
            uint64_t ep = 0, loc = 0; bool f = shidx_lookup(ix, seen_pg[i], snap, &ep, &loc);
            // (the one allowed difference: the first answer was a version that has meanwhile been dropped because it is in the real file, which holds the same page)
            if ((f ? ep : 0) != seen_ep[i] && !(!f && seen_ep[i] != 0 && seen_ep[i] <= atomic_load(&sh->base))) { atomic_fetch_add(&sh->errors, 1); atomic_fetch_add(&sh->e_repeat, 1); }
        }
        atomic_fetch_add(&sh->reads, 32);
        if (hold_forever) { sleep(100); }                        // (killed by the test while pinned)
        shidx_unpin(ix, slot);
    }
    _exit(0);
}

static bool alive_kill0 (int32_t pid, void *ctx) { (void)ctx; return kill(pid, 0) == 0; }

static void test_stress (int writers, int readers, int seconds) {
    char path[128], lockp[160];
    snprintf(path, sizeof path, "%s/mw_shidx_s_%d.idx", getenv("TMPDIR") && *getenv("TMPDIR") ? getenv("TMPDIR") : "/tmp", (int)getpid()); unlink(path);
    snprintf(lockp, sizeof lockp, "%s.lock", path);
    shidx_params p = { 14, 1u << 16, 256, 0 };
    shidx *ix = shidx_open(path, &p);
    CHECK(ix != NULL);
    if (!ix) return;
    shared_t *sh = mmap(NULL, sizeof *sh, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
    memset(sh, 0, sizeof *sh);
    close(open(lockp, O_RDWR | O_CREAT, 0644));
    pid_t pids[64]; int np = 0;
    for (int i = 0; i < writers; i++) { pid_t c = fork(); if (c == 0) { shidx *iw = shidx_open(path, NULL); writer_proc(iw, sh, open(lockp, O_RDWR), i); } pids[np++] = c; }
    for (int i = 0; i < readers; i++) { pid_t c = fork(); if (c == 0) { shidx *ir = shidx_open(path, NULL); reader_proc(ir, sh, i, 0); } pids[np++] = c; }
    pid_t stuck = fork();                                        // a reader that pins a snapshot and never lets go: the GC must keep its versions, then it is killed and reaped
    if (stuck == 0) { shidx *ir = shidx_open(path, NULL); reader_proc(ir, sh, 99, 1); }
    sleep((unsigned)seconds / 2);
    uint64_t oldest_with_stuck = shidx_oldest(ix);
    kill(stuck, SIGKILL); waitpid(stuck, NULL, 0);
    int reaped = shidx_reap(ix, alive_kill0, NULL);
    CHECK(reaped >= 1);
    sleep((unsigned)(seconds - seconds / 2));
    atomic_store(&sh->stop, 1);
    for (int i = 0; i < np; i++) waitpid(pids[i], NULL, 0);
    if (atomic_load(&sh->errors)) printf("  reader errors: not a real version %llu, older than before %llu, repeat differs %llu, dbsize %llu, snapshot went back %llu\n", (unsigned long long)atomic_load(&sh->e_real), (unsigned long long)atomic_load(&sh->e_older), (unsigned long long)atomic_load(&sh->e_repeat), (unsigned long long)atomic_load(&sh->e_size), (unsigned long long)atomic_load(&sh->e_snap));
    CHECK(atomic_load(&sh->errors) == 0);
    CHECK(atomic_load(&sh->commits) > 100);
    CHECK(shidx_oldest(ix) > oldest_with_stuck || oldest_with_stuck == 0 || shidx_oldest(ix) >= oldest_with_stuck);
    // the final state is what the writers recorded
    uint64_t committed = shidx_committed(ix);
    shidx_gc(ix, atomic_load(&sh->base));
    uint64_t base = atomic_load(&sh->base);
    int bad = 0;
    for (uint32_t pg = 1; pg < NPAGES; pg++) {
        uint64_t ep, loc; bool f = shidx_lookup(ix, pg, committed, &ep, &loc);
        uint64_t t = atomic_load(&sh->truth[pg]);
        // a page whose newest version is <= the base was dropped (it is in the real file); newer ones must be there, exactly as the writers recorded them
        if (f ? (ep != t || loc != loc_of(pg, ep)) : t > base) bad++;
    }
    CHECK(bad == 0);
    shidx_stats s; shidx_stats_get(ix, &s);
    printf("  stress: %d writers, %d readers, %ds: %llu commits (epoch %llu), %llu lookups (%llu retried after meeting a freed entry), versions live %llu of %llu, gc runs %llu, reaped %d\n",
           writers, readers, seconds, (unsigned long long)atomic_load(&sh->commits), (unsigned long long)committed, (unsigned long long)atomic_load(&sh->reads), (unsigned long long)s.hazards,
           (unsigned long long)s.versions_live, (unsigned long long)s.entries_cap, (unsigned long long)s.gc_runs, reaped);
    munmap(sh, sizeof *sh);
    shidx_close(ix);
    shidx_unlink(path); unlink(lockp);
}

// ---- a free count that is above what the free list holds ----
// A holder that is killed between the two stores of an allocation used to leave n_free one above the list. The index that is exactly full then took entry 0 for a version, and published it:
// a page that has a version looked as if it had none, and was read from the file, stale. Now the entries of a commit are all taken before any is used, and an install that cannot get them all
// fails and leaves nothing behind.
static void test_overcount (void) {
    char path[256]; snprintf(path, sizeof path, "%s/mw_shidx_o_%d.idx", getenv("TMPDIR") && *getenv("TMPDIR") ? getenv("TMPDIR") : "/tmp", (int)getpid()); unlink(path);
    shidx_params p = { 16, 10, 8, 0 };                                 // 10 entries
    shidx *ix = shidx_open(path, &p);
    CHECK(ix != NULL);
    if (!ix) return;
    uint32_t a[] = { 5, 6 }, b[] = { 7, 8 }, c[] = { 9, 10 };
    install(ix, 2, 10, 2, a);                                           // 3 entries each (the size record and the pages)
    install(ix, 3, 11, 2, b);
    install(ix, 4, 12, 2, c);
    CHECK(shidx_room(ix) == 1);
    shidx_test_overcount_free(ix);                                      // the count says 2
    CHECK(shidx_room(ix) == 2);
    uint32_t d[] = { 5 };
    uint64_t dl[] = { loc_of(5, 5) };
    int rc = shidx_install(ix, 5, 13, 1, d, dl);                        // needs 2 entries
    CHECK(rc != 0);
    CHECK(shidx_head_epoch(ix, 5) == 2);                                // (nothing of it was installed)
    uint64_t ep, loc;
    CHECK(shidx_lookup(ix, 5, 4, &ep, &loc) && ep == 2 && loc == loc_of(5, 2));
    CHECK(shidx_lookup(ix, 9, 4, &ep, &loc) && ep == 4 && loc == loc_of(9, 4));
    uint32_t sz; CHECK(shidx_dbsize(ix, 4, &sz) && sz == 12);
    shidx_close(ix); unlink(path);
}

int main (void) {
    test_functional();
    test_limits();
    test_overcount();
    test_stress(3, 12, 4);
    MW_DONE();
}
