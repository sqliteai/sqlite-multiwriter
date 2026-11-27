// The workload of the external comparison "SQLite vs WAL vs Turso MVCC vs sqlite-multiwriter" (sqlite-writer-bench),
// in C: W writer threads, one connection each, insert 100 rows of a 64-byte payload per transaction into one table
// with an INTEGER PRIMARY KEY that SQLite assigns (the rowid is not given) and an index on writer_id; 4 reader threads
// run MAX(id) and an indexed lookup with a pause of 1 ms. Every connection: journal_mode=WAL, busy_timeout=5000,
// temp_store=MEMORY, the synchronous of the run. A refused transaction (SQLITE_BUSY, SQLITE_LOCKED, their extended codes)
// is rolled back and run again; its latency includes the retries.
//
//   events_bench [--mode mw|wal|delete] [--writers N] [--readers N] [--duration S] [--sync full|normal|off]
//                [--rebase 0|1] [--dir D] [--rows R]
// Prints a line of numbers and a JSON line.
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "sqlite3.h"
#include "multiwriter.h"

static const char *g_mode = "mw", *g_sync = "full", *g_dir = ".";
static int g_writers = 8, g_readers = 4, g_duration = 10, g_rebase = 1, g_rows = 100;
static char g_path[512];
static sqlite3 *g_keep;             // the connection of the schema, open for the whole run: the statistics are read from it
static atomic_int g_stop;
static atomic_uint_fast64_t g_tx, g_retries, g_errors, g_reads;

static uint64_t now_ns (void) { struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec; }
static void die (const char *m, sqlite3 *db) { fprintf(stderr, "events_bench: %s: %s\n", m, db ? sqlite3_errmsg(db) : ""); exit(2); }

static sqlite3 *open_conn (void) {
    char uri[700]; sqlite3 *db;
    if (!strcmp(g_mode, "mw")) snprintf(uri, sizeof uri, "file:%s?vfs=multiwriter&mw_rebase=%d", g_path, g_rebase);
    else snprintf(uri, sizeof uri, "file:%s", g_path);
    if (sqlite3_open_v2(uri, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL) != SQLITE_OK) die("open", db);
    sqlite3_extended_result_codes(db, 1);
    char sql[200];
    snprintf(sql, sizeof sql, "PRAGMA journal_mode=%s", !strcmp(g_mode, "delete") ? "DELETE" : "WAL");
    if (sqlite3_exec(db, sql, NULL, NULL, NULL) != SQLITE_OK) die(sql, db);
    snprintf(sql, sizeof sql, "PRAGMA synchronous=%s", g_sync);
    if (sqlite3_exec(db, sql, NULL, NULL, NULL) != SQLITE_OK) die(sql, db);
    if (sqlite3_exec(db, "PRAGMA busy_timeout=5000; PRAGMA temp_store=MEMORY", NULL, NULL, NULL) != SQLITE_OK) die("pragmas", db);
    return db;
}

static bool retryable (int rc) { int p = rc & 0xff; return p == SQLITE_BUSY || p == SQLITE_LOCKED; }

typedef struct { int id; uint64_t *lat; size_t n, cap; } writer_t;

static void *writer (void *arg) {
    writer_t *w = arg;
    sqlite3 *db = open_conn();
    sqlite3_stmt *ins;
    if (sqlite3_prepare_v2(db, "INSERT INTO events(writer_id, seq, payload, created_at) VALUES (?1, ?2, ?3, ?4)", -1, &ins, NULL) != SQLITE_OK) die("prepare", db);
    unsigned char payload[64]; for (int i = 0; i < 64; i++) payload[i] = (unsigned char)(i * 37 + w->id);
    const char *begin = !strcmp(g_mode, "mw") ? "BEGIN" : "BEGIN IMMEDIATE";
    int64_t seq = 0;
    while (!atomic_load(&g_stop)) {
        uint64_t t0 = now_ns(); int retries = 0; bool ok = false;
        for (int attempt = 0; attempt <= 1000 && !ok; attempt++) {
            sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
            int rc = sqlite3_exec(db, begin, NULL, NULL, NULL);
            for (int r = 0; rc == SQLITE_OK && r < g_rows; r++) {
                sqlite3_bind_int(ins, 1, w->id); sqlite3_bind_int64(ins, 2, seq++);
                sqlite3_bind_blob(ins, 3, payload, sizeof payload, SQLITE_STATIC);
                sqlite3_bind_text(ins, 4, "2026-10-09T18:06:04.0000000+00:00", -1, SQLITE_STATIC);
                rc = sqlite3_step(ins); sqlite3_reset(ins);
                if (rc == SQLITE_DONE) rc = SQLITE_OK;
            }
            if (rc == SQLITE_OK) rc = sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
            if (rc == SQLITE_OK) { ok = true; break; }
            if (!retryable(rc)) { fprintf(stderr, "write error %d: %s\n", rc, sqlite3_errmsg(db)); break; }
            retries++;
        }
        if (!ok) { sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL); atomic_fetch_add(&g_errors, 1); }
        atomic_fetch_add(&g_retries, (uint64_t)retries);
        if (ok && !atomic_load(&g_stop)) {
            atomic_fetch_add(&g_tx, 1);
            if (w->n == w->cap) { w->cap = w->cap ? w->cap * 2 : 4096; w->lat = realloc(w->lat, w->cap * sizeof *w->lat); }
            w->lat[w->n++] = now_ns() - t0;
        }
    }
    sqlite3_finalize(ins);
    sqlite3_close(db);
    return NULL;
}

static void *reader (void *arg) {
    int nw = g_writers > 0 ? g_writers : 1;
    sqlite3 *db = open_conn();
    sqlite3_stmt *mx, *rg;
    if (sqlite3_prepare_v2(db, "SELECT MAX(id) FROM events", -1, &mx, NULL) != SQLITE_OK) die("prepare", db);
    if (sqlite3_prepare_v2(db, "SELECT seq, length(payload) FROM events WHERE writer_id = ?1 ORDER BY seq DESC LIMIT 16", -1, &rg, NULL) != SQLITE_OK) die("prepare", db);
    int flip = 0;
    while (!atomic_load(&g_stop)) {
        int rc;
        if ((flip++ & 1) == 0) { while ((rc = sqlite3_step(mx)) == SQLITE_ROW) {} sqlite3_reset(mx); }
        else { sqlite3_bind_int(rg, 1, flip % nw); while ((rc = sqlite3_step(rg)) == SQLITE_ROW) {} sqlite3_reset(rg); }
        if (rc == SQLITE_DONE) atomic_fetch_add(&g_reads, 1);
        usleep(1000);
    }
    sqlite3_finalize(mx); sqlite3_finalize(rg);
    sqlite3_close(db);
    return NULL;
}

static int cmp_u64 (const void *a, const void *b) { uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b; return x < y ? -1 : x > y; }
static double pct (uint64_t *v, size_t n, double p) { if (!n) return 0; size_t i = (size_t)(p * (double)(n - 1) + 0.5); return (double)v[i] / 1e6; }

int main (int argc, char **argv) {
    for (int i = 1; i < argc; i++) {
        #define ARG(s) (!strcmp(argv[i], s) && i + 1 < argc)
        if ARG("--mode") g_mode = argv[++i];
        else if ARG("--writers") g_writers = atoi(argv[++i]);
        else if ARG("--readers") g_readers = atoi(argv[++i]);
        else if ARG("--duration") g_duration = atoi(argv[++i]);
        else if ARG("--sync") g_sync = argv[++i];
        else if ARG("--rebase") g_rebase = atoi(argv[++i]);
        else if ARG("--dir") g_dir = argv[++i];
        else if ARG("--rows") g_rows = atoi(argv[++i]);
        else { fprintf(stderr, "usage: see the head of bench/events_bench.c\n"); return 2; }
    }
    snprintf(g_path, sizeof g_path, "%s/events_bench_%d.db", g_dir, (int)getpid());
    {
        sqlite3 *db = open_conn();
        if (sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, writer_id INTEGER NOT NULL, seq INTEGER NOT NULL, payload BLOB NOT NULL, created_at TEXT NOT NULL);"
                             "CREATE INDEX IF NOT EXISTS idx_events_writer ON events(writer_id);", NULL, NULL, NULL) != SQLITE_OK) die("schema", db);
        g_keep = db;
    }
    pthread_t *th = calloc((size_t)(g_writers + g_readers), sizeof *th);
    writer_t *ws = calloc((size_t)g_writers, sizeof *ws);
    uint64_t t0 = now_ns();
    for (int i = 0; i < g_writers; i++) { ws[i].id = i; pthread_create(&th[i], NULL, writer, &ws[i]); }
    for (int i = 0; i < g_readers; i++) pthread_create(&th[g_writers + i], NULL, reader, NULL);
    sleep((unsigned)g_duration);
    atomic_store(&g_stop, 1);
    double secs = (double)(now_ns() - t0) / 1e9;
    for (int i = 0; i < g_writers + g_readers; i++) pthread_join(th[i], NULL);

    size_t n = 0; for (int i = 0; i < g_writers; i++) n += ws[i].n;
    uint64_t *all = malloc((n ? n : 1) * sizeof *all); size_t k = 0;
    for (int i = 0; i < g_writers; i++) { memcpy(all + k, ws[i].lat, ws[i].n * sizeof *all); k += ws[i].n; free(ws[i].lat); }
    qsort(all, n, sizeof *all, cmp_u64);

    // validation: every committed row is there exactly once, the file is sound
    sqlite3 *db = open_conn();
    sqlite3_stmt *st; int64_t rows = -1, dup = -1; char ic[200] = "?";
    sqlite3_prepare_v2(db, "SELECT count(*), count(*) - count(DISTINCT writer_id || ':' || seq) FROM events", -1, &st, NULL);
    if (sqlite3_step(st) == SQLITE_ROW) { rows = sqlite3_column_int64(st, 0); dup = sqlite3_column_int64(st, 1); }
    sqlite3_finalize(st);
    sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &st, NULL);
    if (sqlite3_step(st) == SQLITE_ROW) snprintf(ic, sizeof ic, "%s", (const char *)sqlite3_column_text(st, 0));
    sqlite3_finalize(st);
    mw_db_stats s; memset(&s, 0, sizeof s);
    if (!strcmp(g_mode, "mw")) sqlite3_file_control(g_keep, "main", MW_FCNTL_DBSTATS, &s);
    sqlite3_close(db); sqlite3_close(g_keep);
    uint64_t tx = atomic_load(&g_tx);
    bool valid = !strcmp(ic, "ok") && dup == 0 && rows >= (int64_t)(tx * (uint64_t)g_rows);
    printf("%-6s W=%-3d sync=%-4s tx/s %8.0f  reads/s %7.0f  p50 %7.2f  p99 %8.2f  p99.9 %8.2f  max %8.2f ms  retries/100tx %5.1f  errors %llu  rebases %llu unrebasable %llu conflicts %llu syncs %llu hot %llu  %s\n",
           g_mode, g_writers, g_sync, (double)tx / secs, (double)atomic_load(&g_reads) / secs,
           pct(all, n, .50), pct(all, n, .99), pct(all, n, .999), n ? (double)all[n - 1] / 1e6 : 0,
           tx ? 100.0 * (double)atomic_load(&g_retries) / (double)tx : 0, (unsigned long long)atomic_load(&g_errors),
           (unsigned long long)s.rebases, (unsigned long long)s.unrebasable, (unsigned long long)s.page_conflicts, (unsigned long long)s.log_syncs, (unsigned long long)s.hot_serialised,
           valid ? "valid" : "INVALID");
    printf("JSON {\"mode\":\"%s\",\"writers\":%d,\"sync\":\"%s\",\"tx_per_s\":%.1f,\"reads_per_s\":%.1f,\"p50_ms\":%.3f,\"p99_ms\":%.3f,\"p999_ms\":%.3f,\"max_ms\":%.3f,\"retries_per_100\":%.2f,\"valid\":%d}\n",
           g_mode, g_writers, g_sync, (double)tx / secs, (double)atomic_load(&g_reads) / secs, pct(all, n, .50), pct(all, n, .99), pct(all, n, .999), n ? (double)all[n - 1] / 1e6 : 0,
           tx ? 100.0 * (double)atomic_load(&g_retries) / (double)tx : 0, valid);
    char p2[600];
    unlink(g_path);
    const char *sfx[] = { "-wal", "-shm", "-mw", "-journal" };
    for (int i = 0; i < 4; i++) { snprintf(p2, sizeof p2, "%s%s", g_path, sfx[i]); unlink(p2); }
    free(all); free(ws); free(th);
    return valid ? 0 : 1;
}
