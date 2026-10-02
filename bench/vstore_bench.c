// The cost of the update path of a per-cell version store (docs §53): the prototype log-structured store against sqlite-sync's metadata tables (the same table
// layout and indexes, in stock SQLite), for the same stream of operations. A commit changes K rows; every changed cell asks for the previous version of the cell
// (updates only: an insert knows its row is new) and writes the new one.
//   dist/mw/vstore_bench --rows 2000000 --cols 5 --commits 20000 --k 20 --dist zipf --mem 1000000 [--cold]
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include "multiwriter_vstore.h"
#include "sqlite3.h"

static uint64_t now_ns (void) { struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec; }
static uint64_t rng_s = 88172645463325252ull;
static uint64_t rnd (void) { rng_s ^= rng_s << 13; rng_s ^= rng_s >> 7; rng_s ^= rng_s << 17; return rng_s; }
static double rndf (void) { return (double)(rnd() >> 11) / 9007199254740992.0; }

// YCSB zipfian
typedef struct { uint64_t n; double theta, zetan, alpha, eta; } zipf_t;
static double zeta (uint64_t n, double theta) { double s = 0; for (uint64_t i = 1; i <= n; i++) s += 1.0 / pow((double)i, theta); return s; }
static void zipf_init (zipf_t *z, uint64_t n, double theta) { z->n = n; z->theta = theta; z->zetan = zeta(n, theta); z->alpha = 1.0 / (1.0 - theta); z->eta = (1 - pow(2.0 / (double)n, 1 - theta)) / (1 - zeta(2, theta) / z->zetan); }
static uint64_t zipf_next (zipf_t *z) {
    double u = rndf(), uz = u * z->zetan;
    if (uz < 1.0) return 0; if (uz < 1.0 + pow(0.5, z->theta)) return 1;
    return (uint64_t)((double)z->n * pow(z->eta * u - z->eta + 1, z->alpha));
}
static uint64_t scramble (uint64_t x) { x ^= x >> 33; x *= 0xff51afd7ed558ccdull; x ^= x >> 33; return x; }       // (zipf ranks scattered over the key space, as a hash of a primary key would)

static int cmp_d (const void *a, const void *b) { double x = *(const double *)a, y = *(const double *)b; return x < y ? -1 : x > y; }
static void report (const char *what, double *v, size_t n) { if (!n) return; qsort(v, n, sizeof *v, cmp_d); double s = 0; for (size_t i = 0; i < n; i++) s += v[i]; printf("    %-26s mean %7.2f us  p50 %7.2f  p99 %8.2f  max %9.2f  (%zu ops)\n", what, s / (double)n, v[n / 2], v[(size_t)((double)n * 0.99)], v[n - 1], n); }

static void ex_cb (void *a, vs_key k, vs_val v) { (void)a; (void)k; (void)v; }
static int64_t fsz (const char *p) { struct stat sb; return stat(p, &sb) == 0 ? (int64_t)sb.st_size : 0; }

int main (int argc, char **argv) {
    uint64_t rows = 1000000, commits = 20000, mem = 1000000; int cols = 5, k = 20, upd_cols = 1; bool cold = false, zipf = false, skip_sqlite = false, skip_lsm = false; const char *dir = "/tmp/vstore_bench";
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--rows")) rows = strtoull(argv[++i], 0, 10); else if (!strcmp(argv[i], "--cols")) cols = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--commits")) commits = strtoull(argv[++i], 0, 10); else if (!strcmp(argv[i], "--k")) k = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--upd-cols")) upd_cols = atoi(argv[++i]); else if (!strcmp(argv[i], "--mem")) mem = strtoull(argv[++i], 0, 10);
        else if (!strcmp(argv[i], "--dist")) zipf = !strcmp(argv[++i], "zipf"); else if (!strcmp(argv[i], "--cold")) cold = true;
        else if (!strcmp(argv[i], "--no-sqlite")) skip_sqlite = true; else if (!strcmp(argv[i], "--no-lsm")) skip_lsm = true; else if (!strcmp(argv[i], "--dir")) dir = argv[++i];
    }
    zipf_t z; if (zipf) zipf_init(&z, rows, 0.9);
    printf("== %llu rows x %d cells = %.1f M cells; %llu update commits of %d rows x %d column(s); keys %s; store reads %s\n", (unsigned long long)rows, cols, (double)rows * cols / 1e6, (unsigned long long)commits, k, upd_cols, zipf ? "zipfian (theta 0.9)" : "uniform", cold ? "cold (every block read goes to the drive)" : "through the page cache");

    // ------------------------------------------------------------------ the log-structured store
    if (!skip_lsm) {
        char cmd[600]; snprintf(cmd, sizeof cmd, "rm -rf %s && mkdir -p %s", dir, dir); if (system(cmd)) return 1;
        vs_params p = { .dir = dir, .mem_entries = mem, .fanout = 4, .cold = cold };
        vstore *vs = vstore_open(&p);
        uint64_t t0 = now_ns();
        for (uint64_t r = 0; r < rows; r++) { for (int c = 0; c < cols; c++) vstore_put(vs, (vs_key){ (1ull << 32) | (uint64_t)c, scramble(r) }, (vs_val){ 1, (uint32_t)(r / 100 + 1) }); }
        vstore_flush(vs);
        double load_s = (double)(now_ns() - t0) / 1e9; vs_stats s0; vstore_stats(vs, &s0);
        printf("-- log-structured store: load %.1f s (%.0f ns per cell), %llu runs, %.0f MB on disk, %.1f MB resident, written %.0f MB (%.2fx the data)\n", load_s, load_s * 1e9 / ((double)rows * cols), (unsigned long long)s0.runs, (double)s0.disk_bytes / 1048576.0, (double)s0.resident_bytes / 1048576.0, (double)(s0.bytes_flushed + s0.bytes_merged) / 1048576.0, (double)(s0.bytes_flushed + s0.bytes_merged) / ((double)rows * cols * 24.0));
        double *tget = malloc((size_t)commits * k * upd_cols * sizeof(double)), *tput = malloc((size_t)commits * k * upd_cols * sizeof(double)), *tcommit = malloc((size_t)commits * sizeof(double)); size_t ng = 0, nc = 0;
        uint64_t w0 = s0.bytes_flushed + s0.bytes_merged, fl0 = s0.flush_ns + s0.merge_ns;
        uint64_t g0 = s0.gets, gm0 = s0.get_mem, gr0 = s0.get_runs, bs0 = s0.bloom_skips, bf0 = s0.bloom_false, br0 = s0.block_reads;
        uint64_t T0 = now_ns();
        for (uint64_t c = 0; c < commits; c++) {
            uint64_t ct0 = now_ns();
            for (int j = 0; j < k; j++) {
                uint64_t row = zipf ? zipf_next(&z) : rnd() % rows;
                for (int u = 0; u < upd_cols; u++) {
                    vs_key key = { (1ull << 32) | (uint64_t)((int)(rnd() % (uint64_t)cols)), scramble(row) }; vs_val v;
                    uint64_t a = now_ns(); bool hit = vstore_get(vs, key, &v); uint64_t b = now_ns();
                    if (!hit) v = (vs_val){ 0, 0 };
                    vstore_put(vs, key, (vs_val){ v.cv + 1, (uint32_t)(rows / 100 + 2 + c) }); uint64_t d = now_ns();
                    tget[ng] = (double)(b - a) / 1000.0; tput[ng] = (double)(d - b) / 1000.0; ng++;
                }
            }
            tcommit[nc++] = (double)(now_ns() - ct0) / 1000.0;
        }
        double secs = (double)(now_ns() - T0) / 1e9; vs_stats s1; vstore_stats(vs, &s1);
        printf("   update phase: %.2f s, %.0f commits/s\n", secs, (double)commits / secs);
        report("lookup of the old version", tget, ng); report("write of the new version", tput, ng); report("whole commit (K rows)", tcommit, nc);
        printf("    lookups answered by: memory table %.1f%%, a run %.1f%%; runs skipped by the Bloom filter %llu, false positives %llu (%.2f per lookup), block reads %llu (%.2f per lookup)\n",
               100.0 * (double)(s1.get_mem - gm0) / (double)(s1.gets - g0), 100.0 * (double)(s1.get_runs - gr0) / (double)(s1.gets - g0), (unsigned long long)(s1.bloom_skips - bs0), (unsigned long long)(s1.bloom_false - bf0), (double)(s1.bloom_false - bf0) / (double)(s1.gets - g0), (unsigned long long)(s1.block_reads - br0), (double)(s1.block_reads - br0) / (double)(s1.gets - g0));
        printf("    writes: %.1f bytes per commit (flush + merge, amortised), flush+merge time %.1f%% of the phase; now %llu runs, %.0f MB on disk, %.1f MB resident\n", (double)(s1.bytes_flushed + s1.bytes_merged - w0) / (double)commits, 100.0 * (double)(s1.flush_ns + s1.merge_ns - fl0) / 1e9 / secs, (unsigned long long)s1.runs, (double)s1.disk_bytes / 1048576.0, (double)s1.resident_bytes / 1048576.0);
        {   // "what has changed since version X" (the export of a sync): the last 100 commits, the last 5000 commits
            static uint64_t cnt; (void)cnt;
            for (int span = 0; span < 2; span++) {
                uint32_t after = (uint32_t)(rows / 100 + 2 + commits - (span ? 5000 : 100));
                uint64_t a0 = now_ns(); uint64_t n1 = vstore_feed_after(vs, after, ex_cb, NULL); uint64_t a1 = now_ns(); uint64_t n2 = vstore_scan_after(vs, after, ex_cb, NULL); uint64_t a2 = now_ns();
                printf("    export of the last %d commits: change feed %llu entries in %.0f us; scanning the sorted runs %llu entries in %.0f us\n", span ? 5000 : 100, (unsigned long long)n1, (double)(a1 - a0) / 1000.0, (unsigned long long)n2, (double)(a2 - a1) / 1000.0);
            }
            vs_stats s2; vstore_stats(vs, &s2); printf("    feed written: %.1f bytes per commit\n", (double)s2.bytes_feed / (double)commits);
        }
        free(tget); free(tput); free(tcommit); vstore_close(vs);
    }

    // ------------------------------------------------------------------ sqlite-sync's metadata tables in stock SQLite
    if (!skip_sqlite) {
        rng_s = 88172645463325252ull; if (zipf) zipf_init(&z, rows, 0.9);
        char path[600]; snprintf(path, sizeof path, "%s/meta.db", dir); unlink(path);
        { char c2[700]; snprintf(c2, sizeof c2, "mkdir -p %s", dir); if (system(c2)) return 1; }
        sqlite3 *db; if (sqlite3_open(path, &db) != SQLITE_OK) return 1;
        sqlite3_exec(db, "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA wal_autocheckpoint=0; PRAGMA cache_size=-65536;", 0, 0, 0);      // (no fsync per commit: only the structure is measured)
        sqlite3_exec(db, "CREATE TABLE meta (pk BLOB NOT NULL, col_name TEXT NOT NULL, col_version INTEGER, db_version INTEGER, site_id INTEGER DEFAULT 0, seq INTEGER, PRIMARY KEY (pk, col_name)) WITHOUT ROWID;"
                         "CREATE INDEX meta_db_idx ON meta (db_version);", 0, 0, 0);
        sqlite3_stmt *ins, *sel, *upd;
        sqlite3_prepare_v2(db, "INSERT INTO meta(pk, col_name, col_version, db_version, seq) VALUES(?1, ?2, ?3, ?4, 0)", -1, &ins, 0);
        sqlite3_prepare_v2(db, "SELECT col_version FROM meta WHERE pk=?1 AND col_name=?2", -1, &sel, 0);
        sqlite3_prepare_v2(db, "UPDATE meta SET col_version=?3, db_version=?4 WHERE pk=?1 AND col_name=?2", -1, &upd, 0);
        static const char *cn[16] = { "c0","c1","c2","c3","c4","c5","c6","c7","c8","c9","c10","c11","c12","c13","c14","c15" };
        uint64_t t0 = now_ns();
        sqlite3_exec(db, "BEGIN", 0, 0, 0);
        for (uint64_t r = 0; r < rows; r++) {
            uint64_t pk = scramble(r);
            for (int c = 0; c < cols; c++) { sqlite3_bind_blob(ins, 1, &pk, 8, SQLITE_STATIC); sqlite3_bind_text(ins, 2, cn[c], -1, SQLITE_STATIC); sqlite3_bind_int(ins, 3, 1); sqlite3_bind_int64(ins, 4, (int64_t)(r / 100 + 1)); sqlite3_step(ins); sqlite3_reset(ins); }
            if (r % 100 == 99) { sqlite3_exec(db, "COMMIT; BEGIN", 0, 0, 0); }
        }
        sqlite3_exec(db, "COMMIT", 0, 0, 0); sqlite3_exec(db, "PRAGMA wal_checkpoint(TRUNCATE)", 0, 0, 0);
        double load_s = (double)(now_ns() - t0) / 1e9;
        printf("-- sqlite-sync's metadata tables in stock SQLite (WITHOUT ROWID table + index on db_version): load %.1f s (%.0f ns per cell), %.0f MB on disk\n", load_s, load_s * 1e9 / ((double)rows * cols), (double)fsz(path) / 1048576.0);
        double *tget = malloc((size_t)commits * k * upd_cols * sizeof(double)), *tput = malloc((size_t)commits * k * upd_cols * sizeof(double)), *tcommit = malloc((size_t)commits * sizeof(double)); size_t ng = 0, nc = 0;
        char wal[700]; snprintf(wal, sizeof wal, "%s-wal", path); int64_t wal_max = 0;
        uint64_t T0 = now_ns();
        for (uint64_t c = 0; c < commits; c++) {
            uint64_t ct0 = now_ns();
            sqlite3_exec(db, "BEGIN", 0, 0, 0);
            for (int j = 0; j < k; j++) {
                uint64_t row = zipf ? zipf_next(&z) : rnd() % rows; uint64_t pk = scramble(row);
                for (int u = 0; u < upd_cols; u++) {
                    int col = (int)(rnd() % (uint64_t)cols); int64_t cv = 0;
                    uint64_t a = now_ns();
                    sqlite3_bind_blob(sel, 1, &pk, 8, SQLITE_STATIC); sqlite3_bind_text(sel, 2, cn[col], -1, SQLITE_STATIC);
                    if (sqlite3_step(sel) == SQLITE_ROW) cv = sqlite3_column_int64(sel, 0); sqlite3_reset(sel);
                    uint64_t b = now_ns();
                    sqlite3_bind_blob(upd, 1, &pk, 8, SQLITE_STATIC); sqlite3_bind_text(upd, 2, cn[col], -1, SQLITE_STATIC); sqlite3_bind_int64(upd, 3, cv + 1); sqlite3_bind_int64(upd, 4, (int64_t)(rows / 100 + 2 + c));
                    sqlite3_step(upd); sqlite3_reset(upd); uint64_t d = now_ns();
                    tget[ng] = (double)(b - a) / 1000.0; tput[ng] = (double)(d - b) / 1000.0; ng++;
                }
            }
            sqlite3_exec(db, "COMMIT", 0, 0, 0);
            tcommit[nc++] = (double)(now_ns() - ct0) / 1000.0;
        }
        double secs = (double)(now_ns() - T0) / 1e9; wal_max = fsz(wal);
        printf("   update phase: %.2f s, %.0f commits/s\n", secs, (double)commits / secs);
        report("SELECT of the old version", tget, ng); report("UPDATE (row + index)", tput, ng); report("whole commit (K rows)", tcommit, nc);
        printf("    writes: %.0f bytes per commit into the WAL (%.1f pages), file %.0f MB + WAL %.0f MB\n", (double)wal_max / (double)commits, (double)wal_max / (double)commits / 4120.0, (double)fsz(path) / 1048576.0, (double)wal_max / 1048576.0);
        {
            sqlite3_stmt *ex; sqlite3_prepare_v2(db, "SELECT pk, col_name, col_version, db_version FROM meta WHERE db_version > ?1", -1, &ex, 0);
            for (int span = 0; span < 2; span++) {
                int64_t after = (int64_t)(rows / 100 + 2 + commits - (span ? 5000 : 100)); uint64_t a0 = now_ns(), n = 0;
                sqlite3_bind_int64(ex, 1, after); while (sqlite3_step(ex) == SQLITE_ROW) n++; sqlite3_reset(ex);
                printf("    export of the last %d commits: index on db_version %llu entries in %.0f us\n", span ? 5000 : 100, (unsigned long long)n, (double)(now_ns() - a0) / 1000.0);
            }
            sqlite3_finalize(ex);
        }
        free(tget); free(tput); free(tcommit); sqlite3_finalize(ins); sqlite3_finalize(sel); sqlite3_finalize(upd); sqlite3_close(db);
    }
    return 0;
}
