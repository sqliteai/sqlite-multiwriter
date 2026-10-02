// Shared version index: does it scale to 1000 processes? (phase 1 of the multi-process redesign, docs/multiwriter.md §43)
//
// N processes use one index: W of them commit (install + publish + GC, one at a time: an flock queue), the others are readers that behave like agents: pin a snapshot,
// look up `lookups` pages (what a transaction reads), unpin, think for `think_us`. Everything a reader sees is checked (a real version, not from the future, never older
// than what the same process saw before, the size record in range). Reported: commits/s, lookups/s, lookup latency (p50/p99/p99.9), the memory of a process and in total.
//
//   make mw-shidx-bench && dist/mw/shidx_bench --procs 1000 --writers 8 --seconds 10
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <signal.h>
#include <stdatomic.h>
#include <sys/mman.h>
#include <sys/file.h>
#include <sys/wait.h>
#include <sys/resource.h>
#include "multiwriter_shidx.h"

static uint64_t loc_of (uint32_t pgno, uint64_t epoch) { return ((uint64_t)pgno * 0x9E3779B97F4A7C15ull) ^ (epoch * 0xC2B2AE3D27D4EB4Full); }
static uint64_t now_ns (void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return (uint64_t)t.tv_sec * 1000000000ull + (uint64_t)t.tv_nsec; }

#define MAXLAT 4096
typedef struct {
    _Atomic int ready, go, stop;
    _Atomic uint64_t commits, lookups, errors, txs, base, rss_kb_sum, rss_kb_max, procs_done, pin_ns_sum, commit_ns_sum, pin_n;
    _Atomic uint64_t lat_hist[64];                  // log2 histogram of lookup latency in ns
} shared_t;

static int g_pages = 1 << 20, g_lookups = 20, g_think_us = 200, g_writers = 4, g_procs = 100, g_seconds = 10, g_writes_per_commit = 7;
static char g_path[200], g_lock[220];

static void rec_lat (shared_t *sh, uint64_t ns) { int b = 0; while (ns > 1 && b < 63) { ns >>= 1; b++; } atomic_fetch_add_explicit(&sh->lat_hist[b], 1, memory_order_relaxed); }

static void finish (shared_t *sh) {
    struct rusage ru; getrusage(RUSAGE_SELF, &ru);
    uint64_t kb = (uint64_t)ru.ru_maxrss / 1024;       // macOS: bytes
    atomic_fetch_add(&sh->rss_kb_sum, kb);
    uint64_t m = atomic_load(&sh->rss_kb_max); while (kb > m && !atomic_compare_exchange_weak(&sh->rss_kb_max, &m, kb)) {}
    atomic_fetch_add(&sh->procs_done, 1);
}

static void writer_main (shidx *ix, shared_t *sh, int id) {
    uint64_t rng = 0x9E3779B9u + (uint64_t)id * 7919;
    int lockfd = open(g_lock, O_RDWR);
    atomic_fetch_add(&sh->ready, 1);
    while (!atomic_load(&sh->go)) usleep(1000);
    while (!atomic_load(&sh->stop)) {
        flock(lockfd, LOCK_EX);
        uint64_t t0 = now_ns();
        uint64_t epoch = shidx_committed(ix) + 1;
        uint32_t pg[16]; uint64_t locs[16]; int n = 0;
        for (int i = 0; i < g_writes_per_commit; i++) {
            rng = rng * 6364136223846793005ull + 1442695040888963407ull;
            uint32_t p = 1 + (uint32_t)((rng >> 33) % (uint64_t)(g_pages - 1));
            bool dup = false; for (int k = 0; k < n; k++) if (pg[k] == p) dup = true;
            if (!dup) { pg[n] = p; locs[n] = loc_of(p, epoch); n++; }
        }
        if (shidx_install(ix, epoch, (uint32_t)(1000 + epoch % 100), n, pg, locs) == 0) {
            shidx_publish(ix, epoch);
            atomic_fetch_add(&sh->commits, 1);
            if (epoch % 32 == 0) { uint64_t b = epoch > 20000 ? epoch - 20000 : 0; atomic_store(&sh->base, b); shidx_gc(ix, b); }
        } else shidx_gc(ix, atomic_load(&sh->base));
        atomic_fetch_add(&sh->commit_ns_sum, now_ns() - t0);
        flock(lockfd, LOCK_UN);
        usleep(50);                                     // (the writers are not the point: leave the lock to the queue)
    }
    finish(sh);
    _exit(0);
}

static void reader_main (shidx *ix, shared_t *sh, int id) {
    uint64_t rng = 0xABCDEF + (uint64_t)id * 104729;
    int slot = shidx_slot_alloc(ix, (int32_t)getpid());
    if (slot < 0) { atomic_fetch_add(&sh->errors, 1); _exit(2); }
    atomic_fetch_add(&sh->ready, 1);
    while (!atomic_load(&sh->go)) usleep(1000);
    uint64_t last_snap = 0, sample = 0;
    while (!atomic_load(&sh->stop)) {
        uint64_t t0 = now_ns();
        uint64_t snap = shidx_pin(ix, slot);
        atomic_fetch_add_explicit(&sh->pin_ns_sum, now_ns() - t0, memory_order_relaxed); atomic_fetch_add_explicit(&sh->pin_n, 1, memory_order_relaxed);
        if (snap < last_snap) atomic_fetch_add(&sh->errors, 1);
        last_snap = snap;
        for (int i = 0; i < g_lookups; i++) {
            rng = rng * 6364136223846793005ull + 1442695040888963407ull;
            uint32_t p = 1 + (uint32_t)((rng >> 33) % (uint64_t)(g_pages - 1));
            uint64_t ep = 0, loc = 0;
            bool timed = (++sample & 15) == 0;
            uint64_t a = timed ? now_ns() : 0;
            bool f = shidx_lookup(ix, p, snap, &ep, &loc);
            if (timed) rec_lat(sh, now_ns() - a);
            if (f && (ep > snap || ep == 0 || loc != loc_of(p, ep))) atomic_fetch_add(&sh->errors, 1);
        }
        uint32_t sz; if (shidx_dbsize(ix, snap, &sz) && (sz < 1000 || sz >= 1100)) atomic_fetch_add(&sh->errors, 1);
        atomic_fetch_add_explicit(&sh->lookups, (uint64_t)g_lookups + 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&sh->txs, 1, memory_order_relaxed);
        shidx_unpin(ix, slot);
        if (g_think_us > 0) { struct timespec ts = { 0, (long)g_think_us * 1000 + (long)(rng % 50000) }; nanosleep(&ts, NULL); }
    }
    finish(sh);
    _exit(0);
}

static double pct (shared_t *sh, double p) {
    uint64_t tot = 0; for (int i = 0; i < 64; i++) tot += atomic_load(&sh->lat_hist[i]);
    uint64_t want = (uint64_t)((double)tot * p), acc = 0;
    for (int i = 0; i < 64; i++) { acc += atomic_load(&sh->lat_hist[i]); if (acc >= want) return (double)(1ull << i) * 1.5 / 1000.0; }   // (bucket middle, us)
    return 0;
}

int main (int argc, char **argv) {
    for (int i = 1; i + 1 < argc; i += 2) {
        if (!strcmp(argv[i], "--procs")) g_procs = atoi(argv[i + 1]);
        else if (!strcmp(argv[i], "--writers")) g_writers = atoi(argv[i + 1]);
        else if (!strcmp(argv[i], "--seconds")) g_seconds = atoi(argv[i + 1]);
        else if (!strcmp(argv[i], "--pages")) g_pages = atoi(argv[i + 1]);
        else if (!strcmp(argv[i], "--lookups")) g_lookups = atoi(argv[i + 1]);
        else if (!strcmp(argv[i], "--think-us")) g_think_us = atoi(argv[i + 1]);
        else { fprintf(stderr, "unknown argument %s\n", argv[i]); return 2; }
    }
    snprintf(g_path, sizeof g_path, "/tmp/shidx_bench_%d.idx", (int)getpid());
    snprintf(g_lock, sizeof g_lock, "%s.lock", g_path);
    unlink(g_path); close(open(g_lock, O_RDWR | O_CREAT, 0644));
    int lg = 12; while ((1 << lg) < g_pages) lg++;
    shidx_params p = { (uint32_t)lg + 1, 1u << 22, (uint32_t)g_procs + 64, 0 };
    shidx *ix = shidx_open(g_path, &p);
    if (!ix) { perror("shidx_open"); return 1; }
    shared_t *sh = mmap(NULL, sizeof *sh, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
    memset(sh, 0, sizeof *sh);
    // some history so that readers find versions from the start
    for (uint64_t e = 2; e < 2000; e++) {
        uint32_t pg[8]; uint64_t locs[8]; int n = 0;
        for (int i = 0; i < 7; i++) { uint32_t pgno = 1 + (uint32_t)(((e * 7 + (uint64_t)i) * 2654435761u) % (uint64_t)(g_pages - 1)); bool d = false; for (int k = 0; k < n; k++) if (pg[k] == pgno) d = true; if (!d) { pg[n] = pgno; locs[n] = loc_of(pgno, e); n++; } }
        shidx_install(ix, e, (uint32_t)(1000 + e % 100), n, pg, locs); shidx_publish(ix, e);
    }
    pid_t *pids = calloc((size_t)g_procs, sizeof(pid_t));
    uint64_t t_spawn = now_ns();
    for (int i = 0; i < g_procs; i++) {
        pid_t c = fork();
        if (c == 0) { shidx *x = shidx_open(g_path, NULL); if (!x) _exit(3); if (i < g_writers) writer_main(x, sh, i); else reader_main(x, sh, i); }
        if (c < 0) { perror("fork"); g_procs = i; break; }
        pids[i] = c;
    }
    int dead = 0;
    while (atomic_load(&sh->ready) < g_procs) {
        usleep(10000);
        int st; pid_t w;
        while ((w = waitpid(-1, &st, WNOHANG)) > 0) { fprintf(stderr, "a child (pid %d) exited during start-up: status %d\n", (int)w, WIFEXITED(st) ? WEXITSTATUS(st) : -WTERMSIG(st)); dead++; }
        if (dead) { atomic_store(&sh->stop, 1); atomic_store(&sh->go, 1); shidx_close(ix); shidx_unlink(g_path); unlink(g_lock); return 1; }
    }
    double spawn_s = (double)(now_ns() - t_spawn) / 1e9;
    uint64_t c0 = atomic_load(&sh->commits);
    atomic_store(&sh->go, 1);
    uint64_t t0 = now_ns();
    sleep((unsigned)g_seconds);
    uint64_t c1 = atomic_load(&sh->commits), l1 = atomic_load(&sh->lookups), tx1 = atomic_load(&sh->txs);
    double secs = (double)(now_ns() - t0) / 1e9;
    atomic_store(&sh->stop, 1);
    for (int i = 0; i < g_procs; i++) waitpid(pids[i], NULL, 0);
    shidx_stats s; shidx_stats_get(ix, &s);
    uint64_t rss_n = atomic_load(&sh->procs_done);
    printf("shidx_bench: %d processes (%d writers, %d readers), %d s, %d pages, readers %d lookups/tx + %d us think; started in %.1f s\n", g_procs, g_writers, g_procs - g_writers, g_seconds, g_pages, g_lookups, g_think_us, spawn_s);
    printf("  commits: %.0f/s   reader transactions: %.0f/s   lookups: %.2f M/s\n", (double)(c1 - c0) / secs, (double)tx1 / secs, (double)l1 / secs / 1e6);
    printf("  lookup latency (sampled): p50 %.2f us  p99 %.2f us  p99.9 %.2f us\n", pct(sh, 0.5), pct(sh, 0.99), pct(sh, 0.999));
    printf("  pin: %.2f us on average; a commit under the lock: %.1f us on average\n", atomic_load(&sh->pin_n) ? (double)atomic_load(&sh->pin_ns_sum) / (double)atomic_load(&sh->pin_n) / 1000.0 : 0.0, (c1 - c0) ? (double)atomic_load(&sh->commit_ns_sum) / (double)atomic_load(&sh->commits) / 1000.0 : 0.0);
    printf("  memory: one process %.1f MB on average (max %.1f MB), all %d: %.2f GB   (the private store of today: 110-160 MB per process)\n",
           rss_n ? (double)atomic_load(&sh->rss_kb_sum) / (double)rss_n / 1024.0 : 0.0, (double)atomic_load(&sh->rss_kb_max) / 1024.0, (int)rss_n, (double)atomic_load(&sh->rss_kb_sum) / 1048576.0);
    printf("  index: %llu versions live of %llu, %llu freed, %llu blocks, %llu GC runs, %llu lookups retried after meeting a freed entry\n",
           (unsigned long long)s.versions_live, (unsigned long long)s.entries_cap, (unsigned long long)s.versions_freed, (unsigned long long)s.blocks, (unsigned long long)s.gc_runs, (unsigned long long)s.hazards);
    printf("  errors: %llu  -> %s\n", (unsigned long long)atomic_load(&sh->errors), atomic_load(&sh->errors) ? "FAILED" : "OK");
    int rc = atomic_load(&sh->errors) ? 1 : 0;
    shidx_close(ix); shidx_unlink(g_path); unlink(g_lock);
    return rc;
}
