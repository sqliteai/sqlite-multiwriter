// The version store shared by several processes (docs §54): P writer processes commit to one store at the same time (and R reader processes look cells up), against the
// cost one process would have alone. Modes: "disjoint" (each process updates its own rows; the lookup of the old version runs outside the writer lock, as it would
// after the engine's page-level validation) and "shared" (all processes update the same rows; lookup and put are one step under the lock, so every update must be
// counted: afterwards the sum of the column versions is checked against the number of updates).
//   dist/mw/vsshared_bench --procs 4 --commits 40000 --k 20 --rows 1000000 --mode disjoint [--readers 2] [--mem 500000]
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include "multiwriter_vsshared.h"

static uint64_t now_ns (void) { struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec; }
static uint64_t scramble (uint64_t x) { x ^= x >> 33; x *= 0xff51afd7ed558ccdull; x ^= x >> 33; return x; }
static int cmp_d (const void *a, const void *b) { double x = *(const double *)a, y = *(const double *)b; return x < y ? -1 : x > y; }
static uint64_t rng_step (uint64_t *r) { uint64_t x = *r; x ^= x << 13; x ^= x >> 7; x ^= x << 17; return *r = x; }
typedef struct { int kind; double secs; uint64_t commits, ops; double look_mean_us, commit_p50_us, commit_p99_us, commit_max_us, lock_wait_us_per_commit, stall_ms; uint64_t stalls, flushes, merges, retries; double look_p50, look_p99, look_max; double flush_ms, merge_ms; long rss_mb; } result;

int main (int argc, char **argv) {
    int procs = 4, readers = 0, k = 20, cols = 5; uint64_t rows = 1000000, commits = 40000, mem = 500000, reader_ops = 3000000; bool shared = false; double rate = 0; const char *dir = "/tmp/vsshared_bench";
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--procs")) procs = atoi(argv[++i]); else if (!strcmp(argv[i], "--readers")) readers = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--commits")) commits = strtoull(argv[++i], 0, 10); else if (!strcmp(argv[i], "--k")) k = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--rows")) rows = strtoull(argv[++i], 0, 10); else if (!strcmp(argv[i], "--mem")) mem = strtoull(argv[++i], 0, 10);
        else if (!strcmp(argv[i], "--mode")) shared = !strcmp(argv[++i], "shared"); else if (!strcmp(argv[i], "--reader-ops")) reader_ops = strtoull(argv[++i], 0, 10);
        else if (!strcmp(argv[i], "--dir")) dir = argv[++i]; else if (!strcmp(argv[i], "--rate")) rate = atof(argv[++i]);     // commits per second per writer process (0 = as fast as possible)
    }
    char cmd[600]; snprintf(cmd, sizeof cmd, "rm -rf %s && mkdir -p %s", dir, dir); if (system(cmd)) return 1;
    vsh_params p = { dir, mem, true, false };
    vsh *h = vsh_open(&p);
    uint64_t t0 = now_ns();
    { vsh_key kb[500]; vsh_val vb[500]; int n = 0;
      for (uint64_t r = 0; r < rows; r++) for (int c = 0; c < cols; c++) { kb[n] = (vsh_key){ (1ull << 32) | (uint64_t)c, scramble(r) }; vb[n] = (vsh_val){ 1, (uint32_t)(r / 100 + 1) }; if (++n == 500) { vsh_commit(h, kb, vb, n); n = 0; } }
      if (n) vsh_commit(h, kb, vb, n); }
    vsh_flush_all(h);
    vsh_stats ls; vsh_stats_get(h, &ls);
    printf("== %llu rows x %d cells; %d writer process(es) x %llu commits of %d cells (%s), %d reader process(es); memory table %llu entries (2 tables)\n", (unsigned long long)rows, cols, procs, (unsigned long long)commits, k, shared ? "shared rows, lookup+put under the lock" : "disjoint rows, lookup outside the lock", readers, (unsigned long long)mem);
    printf("-- load: %.1f s, %llu runs, %.0f MB on disk\n", (double)(now_ns() - t0) / 1e9, (unsigned long long)ls.runs, (double)ls.disk_bytes / 1048576.0);
    vsh_close(h);

    int total = procs + readers, fds[64][2];
    for (int i = 0; i < total; i++) {
        if (pipe(fds[i])) return 1;
        pid_t pid = fork();
        if (pid == 0) {
            close(fds[i][0]);
            vsh_params q = { dir, mem, false, false }; vsh *w = vsh_open(&q); result res; memset(&res, 0, sizeof res);
            uint64_t rng = 0x9E3779B97F4A7C15ull * (uint64_t)(i + 1);
            #define RND() (rng_step(&rng))
            if (i < procs) {
                res.kind = 0; double *lat = malloc((size_t)commits * sizeof(double)); double look_ns = 0;
                uint64_t span = rows / (uint64_t)procs, base = (uint64_t)i * span;
                uint64_t T0 = now_ns();
                for (uint64_t c = 0; c < commits; c++) {
                    vsh_key kb[256]; vsh_val vb[256]; uint64_t cs = now_ns();
                    for (int j = 0; j < k; j++) {                                      // (distinct cells within one commit: a commit changes a cell once)
                        for (;;) { uint64_t row = shared ? RND() % rows : base + RND() % span; kb[j] = (vsh_key){ (1ull << 32) | (RND() % (uint64_t)cols), scramble(row) };
                                   bool dup = false; for (int q = 0; q < j; q++) if (kb[q].hi == kb[j].hi && kb[q].lo == kb[j].lo) dup = true; if (!dup) break; }
                    }
                    if (!shared) {
                        uint64_t a = now_ns();
                        for (int j = 0; j < k; j++) { vsh_val v; if (!vsh_get(w, kb[j], &v)) v = (vsh_val){ 0, 0 }; vb[j] = (vsh_val){ v.cv + 1, (uint32_t)(rows / 100 + 2 + c) }; }
                        look_ns += (double)(now_ns() - a);
                        vsh_commit(w, kb, vb, k);
                    } else {
                        for (;;) {
                            vsh_wait_room(w); vsh_lock(w); uint64_t a = now_ns();
                            for (int j = 0; j < k; j++) { vsh_val v; if (!vsh_get(w, kb[j], &v)) v = (vsh_val){ 0, 0 }; vb[j] = (vsh_val){ v.cv + 1, (uint32_t)(rows / 100 + 2 + c) }; }
                            look_ns += (double)(now_ns() - a);
                            if (vsh_commit_locked(w, kb, vb, k)) break;
                            vsh_unlock(w);
                        }
                        vsh_unlock_flush(w);
                    }
                    lat[c] = (double)(now_ns() - cs) / 1000.0;
                    if (rate > 0) { uint64_t due = T0 + (uint64_t)((double)(c + 1) * 1e9 / rate); while (now_ns() < due) {} }          // (paced: an engine commits at most a few tens of thousands of times a second in all)
                }
                res.secs = (double)(now_ns() - T0) / 1e9; res.commits = commits; res.ops = commits * (uint64_t)k; res.look_mean_us = look_ns / 1000.0 / (double)res.ops;
                qsort(lat, commits, sizeof(double), cmp_d); res.commit_p50_us = lat[commits / 2]; res.commit_p99_us = lat[(size_t)((double)commits * 0.99)]; res.commit_max_us = lat[commits - 1]; free(lat);
            } else {
                res.kind = 1; double *lat = malloc(reader_ops * sizeof(double) / 8); uint64_t nl = 0; uint64_t T0 = now_ns();
                for (uint64_t o = 0; o < reader_ops; o++) {
                    vsh_key key = { (1ull << 32) | (RND() % (uint64_t)cols), scramble(RND() % rows) }; vsh_val v; uint64_t a = now_ns(); vsh_get(w, key, &v); uint64_t b = now_ns();
                    if (o % 8 == 0) lat[nl++] = (double)(b - a) / 1000.0;
                }
                res.secs = (double)(now_ns() - T0) / 1e9; res.ops = reader_ops; qsort(lat, nl, sizeof(double), cmp_d); res.look_p50 = lat[nl / 2]; res.look_p99 = lat[(size_t)((double)nl * 0.99)]; res.look_max = lat[nl - 1]; res.look_mean_us = res.secs * 1e6 / (double)reader_ops; free(lat);
            }
            vsh_stats st; vsh_stats_get(w, &st); res.lock_wait_us_per_commit = res.commits ? (double)st.lock_wait_ns / 1000.0 / (double)res.commits : 0; res.stall_ms = (double)st.stall_ns / 1e6; res.stalls = st.stalls; res.flushes = st.flushes; res.merges = st.merges; res.retries = st.retries;
            res.flush_ms = (double)st.flush_ns / 1e6; res.merge_ms = (double)st.merge_ns / 1e6;
            struct rusage ru; getrusage(RUSAGE_SELF, &ru); res.rss_mb = ru.ru_maxrss / (1024 * 1024);
            if (write(fds[i][1], &res, sizeof res) != (ssize_t)sizeof res) _exit(2);
            vsh_close(w); _exit(0);
        }
        close(fds[i][1]);
    }
    double wsecs = 0, ctot = 0, opstot = 0; uint64_t stalls = 0; double stall_ms = 0;
    for (int i = 0; i < total; i++) {
        result r; if (read(fds[i][0], &r, sizeof r) != (ssize_t)sizeof r) { printf("process %d failed\n", i); continue; }
        if (r.kind == 0) {
            printf("  writer %2d: %6.0f commits/s  commit p50 %6.1f us p99 %7.1f us max %9.0f us | lookup %.2f us/cell | lock wait %.1f us/commit | stalls %llu (%.0f ms) | flushed %llu runs (%.0f ms), merged %llu (%.0f ms) | rss %ld MB\n", i, (double)r.commits / r.secs, r.commit_p50_us, r.commit_p99_us, r.commit_max_us, r.look_mean_us, r.lock_wait_us_per_commit, (unsigned long long)r.stalls, r.stall_ms, (unsigned long long)r.flushes, r.flush_ms, (unsigned long long)r.merges, r.merge_ms, r.rss_mb);
            ctot += (double)r.commits / r.secs; opstot += (double)r.ops / r.secs; if (r.secs > wsecs) wsecs = r.secs; stalls += r.stalls; stall_ms += r.stall_ms;
        } else printf("  reader %2d: %.2f us per lookup mean, p50 %.2f us, p99 %.2f us, max %.0f us (retries after a manifest change: %llu) | rss %ld MB\n", i, r.look_mean_us, r.look_p50, r.look_p99, r.look_max, (unsigned long long)r.retries, r.rss_mb);
        close(fds[i][0]);
    }
    for (int i = 0; i < total; i++) { int st; wait(&st); }
    printf("-- all writers together: %.0f commits/s (%.0f cell updates/s), %llu stalls (%.0f ms in total)\n", ctot, opstot, (unsigned long long)stalls, stall_ms);

    // verification: every update must be in the store exactly once (shared mode), and a lookup after the run finds everything
    vsh_params q = { dir, mem, false, false }; vsh *v = vsh_open(&q);
    vsh_stats fin; vsh_stats_get(v, &fin);
    if (shared) {
        uint64_t sum = 0, missing = 0;
        for (uint64_t r = 0; r < rows; r++) for (int c = 0; c < cols; c++) { vsh_val x; if (!vsh_get(v, (vsh_key){ (1ull << 32) | (uint64_t)c, scramble(r) }, &x)) missing++; else sum += x.cv - 1; }
        uint64_t want = (uint64_t)procs * commits * (uint64_t)k;
        printf("-- check: %llu updates expected, column versions add up to %llu, %llu cells missing -> %s\n", (unsigned long long)want, (unsigned long long)sum, (unsigned long long)missing, (sum == want && !missing) ? "OK" : "FAILED");
    } else {
        uint64_t missing = 0; for (uint64_t r = 0; r < rows; r += 7) for (int c = 0; c < cols; c++) { vsh_val x; if (!vsh_get(v, (vsh_key){ (1ull << 32) | (uint64_t)c, scramble(r) }, &x)) missing++; }
        printf("-- check: %llu sampled cells missing -> %s\n", (unsigned long long)missing, missing ? "FAILED" : "OK");
    }
    printf("-- store now: %llu runs, %.0f MB on disk\n", (unsigned long long)fin.runs, (double)fin.disk_bytes / 1048576.0);
    vsh_close(v);
    return 0;
}
