// Concurrency benchmark: stock SQLite (rollback journal), stock WAL and Multi-Writer lanes with identical
// PRAGMAs. Prints the exact configuration, a human summary, a JSON line and (optionally) a CSV row.
// Every run is validated (application-level check + PRAGMA integrity_check); a run whose data is wrong is
// reported INVALID and exits non-zero: such numbers must never be used.
//
//   mw_bench --mode stock|stock-wal|multiwriter --workload W --agents N --duration S
//            [--warmup S] [--seed K] [--rows R] [--threads T] [--sync off|normal|full] [--read-pct P]
//            [--rebase 0|1] [--csv file] [--think-us U]
//
// An "agent" is one independent SQLite connection. Agents are multiplexed over T OS threads (default
// min(agents, 2*cores)): each thread round-robins its agents, one transaction per turn. With 1000 agents,
// one OS thread per agent would measure the scheduler instead of SQLite; 1000 connection states are kept
// alive regardless. Workloads (see docs/multiwriter.md, "Benchmarks"):
//   independent   A/B  own row per agent, one row per page             
//   readonly      A    random primary-key reads
//   samepage      C    own row per agent, rows share pages             
//   cols          D    agents update different columns of shared rows  
//   samecol       E    all agents update one column of one row         
//   crdtinsert    F    unique-key inserts into the table ct         
//   mixed         G    read-pct% reads / rest writes on own rows
//   longtx        H    several reads, then one write
//   groups              each agent its own row, four groups of agents whose rows share a page (four contended pages)
//   hot           I    4 hot rows of the table ct (true conflicts: UPDATE ct SET a=a+1)
//   bulk          -    100-row INSERT per transaction on disjoint keys; --poisson-tps N: open-loop arrivals
//   longreader    J    independent writers + one reader pinning an old snapshot
//   insert-uuid / insert-int / insert-autoinc   K   concurrent INSERT with different key strategies
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <math.h>
#include <stdio.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/resource.h>
#include <execinfo.h>
#include <signal.h>
#include "sqlite3.h"
#include "multiwriter.h"
#ifdef __APPLE__
#include <mach/mach.h>
#include <mach/mach_time.h>
#include <mach/thread_policy.h>
#define thread_t bench_thread_t     /* (the benchmark's own thread_t must not clash with mach's) */
#endif


typedef enum { M_STOCK, M_STOCK_WAL, M_MW } bmode_t;
enum { W_INDEPENDENT, W_READONLY, W_SAMEPAGE, W_COLS, W_SAMECOL, W_CRDTINSERT, W_MIXED, W_LONGTX, W_HOT, W_LONGREADER, W_INS_UUID, W_INS_INT, W_INS_AUTO, W_BULK, W_SLOWTX, W_SLOWHOT, W_SLOWPAGE, W_GROUPS, W_COUNT };
static const char *wl_name[W_COUNT] = { "independent", "readonly", "samepage", "cols", "samecol", "crdtinsert", "mixed", "longtx", "hot", "longreader", "insert-uuid", "insert-int", "insert-autoinc", "bulk", "slowtx", "slowhot", "slowpage", "groups" };
static const char *mode_name[] = { "stock", "stock-wal", "multiwriter" };

static struct {
    bmode_t mode; int wl;
    int agents, threads, rows, read_pct, warmup, duration, seed, rebase, think_us, busy_ms, begin_wait, mp, setup_only, no_setup, agent_base;
    long long verify_sum;
    const char *sync, *path, *csv;
} cfg = { M_MW, W_INDEPENDENT, 10, 0, 0, 80, 1, 5, 1, -1, 0, -1, 0, 0, 0, 0, 0, -1, "off", NULL, NULL };

static uint64_t now_ns (void) { struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec; }

typedef struct { sqlite3 *db; int id; uint64_t rng; uint64_t commits; int64_t last_value; uint64_t begin_ok_ns; } agent_t;
typedef struct {
    agent_t *agents; int n;
    uint64_t *lat; size_t nlat, cap;
    uint64_t *wt; size_t nwt, capw;                     // --begin-wait: time from the first attempt of a transaction to the moment its write may start
    uint64_t reads, writes, busy, errors, max_retries, gave_up;
} thread_t;
static int g_retry;
static const char *g_barrier;          // multi-process runs: a directory; every process announces itself there once its connections are open and waits for "go"
static double g_poisson_tps;    // >0: open loop, Poisson arrivals at this total rate; latency = scheduled time -> commit   // --retry N: retry a refused transaction (with jittered exponential backoff) up to N times; 0 = surface the failure

static volatile int phase;          // 0 = warm-up, 1 = measure, 2 = stop
static uint64_t xs (uint64_t *s) { uint64_t x = *s; x ^= x << 13; x ^= x >> 7; x ^= x << 17; return *s = x; }
static int exec_ (sqlite3 *db, const char *sql) { return sqlite3_exec(db, sql, NULL, NULL, NULL); }
static int64_t scalar_ (sqlite3 *db, const char *sql) {
    sqlite3_stmt *st; int64_t v = -1;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) v = sqlite3_column_int64(st, 0);
    sqlite3_finalize(st); return v;
}
static char *text_ (sqlite3 *db, const char *sql) {
    static __thread char buf[4][128]; static __thread int k;
    char *b = buf[k++ & 3]; b[0] = 0;
    sqlite3_stmt *st;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0)) snprintf(b, 128, "%s", (const char *)sqlite3_column_text(st, 0));
    sqlite3_finalize(st); return b;
}

static int open_agent (agent_t *a) {
    int rc;
    if (cfg.mode == M_MW) {
        char uri[600]; snprintf(uri, sizeof uri, "file:%s?mw=1&mw_gc=64&mw_mp=%d%s%s", cfg.path, cfg.mp, cfg.rebase ? "&mw_rebase=1" : "", getenv("MW_BENCH_URI_EXTRA") ? getenv("MW_BENCH_URI_EXTRA") : "");
        rc = sqlite3_open_v2(uri, &a->db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI | SQLITE_OPEN_NOMUTEX, NULL);
    } else {
        rc = sqlite3_open_v2(cfg.path, &a->db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX, "unix");   // no wrapper in the baselines
    }
    if (rc != SQLITE_OK) return rc;
    sqlite3_extended_result_codes(a->db, 1);
    sqlite3_busy_timeout(a->db, cfg.busy_ms >= 0 ? cfg.busy_ms : cfg.mode == M_MW ? 0 : 60000);
    char sql[64]; snprintf(sql, sizeof sql, "PRAGMA synchronous=%s", cfg.sync);
    exec_(a->db, sql);
    if (cfg.mode == M_STOCK_WAL || cfg.mode == M_MW) exec_(a->db, "PRAGMA wal_autocheckpoint=1000");
    return SQLITE_OK;
}

static bool retryable (int rc) { int p = rc & 0xff; return p == SQLITE_BUSY || p == SQLITE_LOCKED; }

// one transaction; returns 1 = committed write, 2 = read, 0 = retryable failure, -1 = hard error
static int do_txn (agent_t *a) {
    char sql[512];
    int rc;
    int w = cfg.wl;
    bool is_write = true;
    if (w == W_READONLY) is_write = false;
    else if (w == W_MIXED) is_write = (int)(xs(&a->rng) % 100) >= cfg.read_pct;
    if (!is_write) {
        snprintf(sql, sizeof sql, "SELECT v FROM t WHERE id=%lld", (long long)(1 + xs(&a->rng) % (uint64_t)cfg.rows));
        rc = exec_(a->db, sql);
        return rc == SQLITE_OK ? 2 : retryable(rc) ? 0 : -1;
    }
    switch (w) {
        case W_INDEPENDENT: case W_MIXED:
            snprintf(sql, sizeof sql, "UPDATE t SET v=v+1 WHERE id=%d", 1 + a->id); break;
        case W_HOT:
            snprintf(sql, sizeof sql, "UPDATE ct SET a=a+1 WHERE id='r%d'", (int)(xs(&a->rng) % 4)); break;
        case W_LONGTX:
            snprintf(sql, sizeof sql, "BEGIN;"
                "SELECT v FROM t WHERE id=%d; SELECT v FROM t WHERE id=%d; SELECT v FROM t WHERE id=%d; SELECT v FROM t WHERE id=%d; SELECT v FROM t WHERE id=%d;"
                "UPDATE t SET v=v+1 WHERE id=%d; COMMIT;",
                1 + (int)(xs(&a->rng) % cfg.rows), 1 + (int)(xs(&a->rng) % cfg.rows), 1 + (int)(xs(&a->rng) % cfg.rows),
                1 + (int)(xs(&a->rng) % cfg.rows), 1 + (int)(xs(&a->rng) % cfg.rows), 1 + a->id);
            break;
        case W_LONGREADER:
            snprintf(sql, sizeof sql, "UPDATE t SET v=v+1 WHERE id=%d", 1 + a->id); break;
        case W_SAMEPAGE:
            snprintf(sql, sizeof sql, "UPDATE ct SET a=a+1 WHERE id='r%d'", a->id); break;
        case W_GROUPS:      // each agent its own row; the agents are in four groups, the rows of a group share a page and the groups are 200 rows apart: four pages that agents contend for
            snprintf(sql, sizeof sql, "UPDATE ct SET a=a+1 WHERE id='r%d'", (a->id % 4) * 200 + a->id / 4); break;
        case W_COLS: {
            static const char *col[4] = { "a", "b", "c", "d" };
            a->last_value = (int64_t)a->commits + 1;
            snprintf(sql, sizeof sql, "UPDATE ct SET %s=%lld WHERE id='r%d'", col[a->id % 4], (long long)a->last_value, a->id / 4);
            break;
        }
        case W_SAMECOL:
            a->last_value = (int64_t)a->id * 1000000000ll + (int64_t)a->commits + 1;
            snprintf(sql, sizeof sql, "UPDATE ct SET a=%lld WHERE id='r0'", (long long)a->last_value);
            break;
        case W_CRDTINSERT:
            if (getenv("MW_BENCH_PADKEY")) snprintf(sql, sizeof sql, "INSERT INTO ct(id,a,b,c,d,pad) VALUES('n%04d-%012llu',1,2,3,4,'x')", a->id, (unsigned long long)a->commits);     // (keys that sort in insertion order: appends)
            else snprintf(sql, sizeof sql, "INSERT INTO ct(id,a,b,c,d,pad) VALUES('n%d-%llu',1,2,3,4,'x')", a->id, (unsigned long long)a->commits);
            break;
        case W_INS_UUID:
            snprintf(sql, sizeof sql, "INSERT INTO tu(id,v) VALUES('u%d-%llu-%llu',1)", a->id, (unsigned long long)a->commits, (unsigned long long)(xs(&a->rng) & 0xffffff)); break;
        case W_INS_INT:  snprintf(sql, sizeof sql, "INSERT INTO ti(v) VALUES(1)"); break;
        case W_INS_AUTO: snprintf(sql, sizeof sql, "INSERT INTO ta(v) VALUES(1)"); break;
        case W_SLOWTX: case W_SLOWHOT: case W_SLOWPAGE: {      // an explicit transaction that stays open --think-us after its write (an application doing work in the middle)
            if (w == W_SLOWTX) snprintf(sql, sizeof sql, "UPDATE t SET v=v+1 WHERE id=%d", 1 + a->id);
            else snprintf(sql, sizeof sql, "UPDATE ct SET a=a+1 WHERE id='r%d'", w == W_SLOWPAGE ? a->id : (int)(xs(&a->rng) % 4));   // slowpage: each agent its own row, all in the same page
            rc = exec_(a->db, "BEGIN");
            if (rc == SQLITE_OK) rc = exec_(a->db, sql);
            if (rc == SQLITE_OK && cfg.think_us) { struct timespec ts = { 0, (long)cfg.think_us * 1000 }; nanosleep(&ts, NULL); }
            if (rc == SQLITE_OK) rc = exec_(a->db, "COMMIT");
            if (rc == SQLITE_OK) { a->commits++; return 1; }
            if (!retryable(rc) && getenv("MW_BENCH_DEBUG")) fprintf(stderr, "slow txn error rc=%d: %s\n", rc, sqlite3_errmsg(a->db));
            if (!sqlite3_get_autocommit(a->db)) exec_(a->db, "ROLLBACK");
            return retryable(rc) ? 0 : -1;
        }
        case W_BULK: {
            char big[100 * 160 + 64]; bool wide = getenv("MW_BENCH_BULK_WIDE") != NULL;      // (wide: five columns besides the key, six cells a row with the sentinel)
            size_t n = (size_t)snprintf(big, sizeof big, wide ? "INSERT INTO bkw(id,a,b,c,d,v) VALUES" : "INSERT INTO bk(id,v) VALUES");
            static int rnd_keys = -1; if (rnd_keys < 0) rnd_keys = getenv("MW_BENCH_BULK_RANDOM") != NULL;             // (random keys, like UUIDs: the rows of a flush are spread over the whole key space)
            for (int i = 0; i < 100; i++) {
                unsigned long long key = ((unsigned long long)a->id << 32) | (a->commits * 100 + (uint64_t)i);
                if (rnd_keys) { key ^= key >> 33; key *= 0xff51afd7ed558ccdull; key ^= key >> 33; key *= 0xc4ceb9fe1a85ec53ull; key ^= key >> 33; key &= 0x7fffffffffffffffull; }
                n += (size_t)snprintf(big + n, sizeof big - n, wide ? "%s(%llu,1,2,3,4,'0123456789012345678901234567890123456789')" : "%s(%llu,'0123456789012345678901234567890123456789012345678901234567890123')", i ? "," : "", key);
            }
            if (cfg.think_us) { struct timespec ts = { 0, (long)cfg.think_us * 1000 }; nanosleep(&ts, NULL); }
            if (cfg.begin_wait) {                                  // explicit transaction: BEGIN IMMEDIATE is where SQLite takes the write lock (and waits for it)
                rc = exec_(a->db, "BEGIN IMMEDIATE");
                if (rc == SQLITE_OK) rc = exec_(a->db, "SELECT count(*) FROM sqlite_master");     // (Multi-Writer takes its snapshot, and waits for admission, at the first read: the write may start after this)
                if (rc != SQLITE_OK) { if (!retryable(rc) && getenv("MW_BENCH_DEBUG")) fprintf(stderr, "bulk begin error rc=%d: %s\n", rc, sqlite3_errmsg(a->db)); if (!sqlite3_get_autocommit(a->db)) exec_(a->db, "ROLLBACK"); return retryable(rc) ? 0 : -1; }
                a->begin_ok_ns = now_ns();
                rc = exec_(a->db, big);
                if (rc == SQLITE_OK) rc = exec_(a->db, "COMMIT");
                if (rc == SQLITE_OK) { a->commits++; return 1; }
                if (!sqlite3_get_autocommit(a->db)) exec_(a->db, "ROLLBACK");
                if (!retryable(rc) && getenv("MW_BENCH_DEBUG")) fprintf(stderr, "bulk error rc=%d: %s\n", rc, sqlite3_errmsg(a->db));
                return retryable(rc) ? 0 : -1;
            }
            rc = exec_(a->db, big);
            if (rc == SQLITE_OK) { a->commits++; return 1; }
            if (!sqlite3_get_autocommit(a->db)) exec_(a->db, "ROLLBACK");
            return retryable(rc) ? 0 : -1;
        }
        default: return -1;
    }
    if (cfg.think_us) { struct timespec ts = { 0, (long)cfg.think_us * 1000 }; nanosleep(&ts, NULL); }
    rc = exec_(a->db, sql);
    if (rc == SQLITE_OK) { a->commits++; return 1; }
    if (!sqlite3_get_autocommit(a->db)) exec_(a->db, "ROLLBACK");
    if (retryable(rc) || (rc & 0xff) == SQLITE_CONSTRAINT) return 0;           // (a constraint on a retried auto-id/PK insert = collision)
    return -1;
}

static double exp_draw (uint64_t *rng, double mean_ns) { double u = ((double)(xs(rng) >> 11) + 1.0) / 9007199254740993.0; return -mean_ns * log(u); }

// Open loop (--poisson-tps): every agent has Poisson arrivals at tps/agents; a transaction's latency runs from its scheduled arrival to its commit,
// so queueing behind a lock or a slow commit is part of the number.
#ifdef __APPLE__
// macOS coalesces the timers of idle threads: a thread that sleeps until its next scheduled arrival wakes up 2 ms late at the median and 10 ms late at p99, which would be
// reported as transaction latency. A real-time (time-constraint) policy removes the slack (p99 35 us in an isolated test). Used only for the open-loop latency test.
static void set_realtime (void) {
    mach_timebase_info_data_t tb; mach_timebase_info(&tb);
    double c = (double)tb.denom / (double)tb.numer * 1e6;
    thread_time_constraint_policy_data_t p = { (uint32_t)(1.0 * c), (uint32_t)(0.1 * c), (uint32_t)(2.0 * c), 1 };
    thread_policy_set(mach_thread_self(), THREAD_TIME_CONSTRAINT_POLICY, (thread_policy_t)&p, THREAD_TIME_CONSTRAINT_POLICY_COUNT);
}
#else
static void set_realtime (void) {}
#endif
static void *thread_main_poisson (void *arg) {
    thread_t *t = arg;
    if (!getenv("MW_BENCH_NO_RT")) set_realtime();
    double mean = 1e9 * (double)cfg.agents / g_poisson_tps;
    uint64_t *next = malloc((size_t)t->n * sizeof(uint64_t));
    uint64_t start = now_ns();
    for (int i = 0; i < t->n; i++) next[i] = start + (uint64_t)exp_draw(&t->agents[i].rng, mean);
    while (phase < 2) {
        int cur = 0; for (int i = 1; i < t->n; i++) if (next[i] < next[cur]) cur = i;
        agent_t *a = &t->agents[cur];
        uint64_t sched = next[cur], now = now_ns();
        if (sched > now) { struct timespec ts = { (time_t)((sched - now) / 1000000000ull), (long)((sched - now) % 1000000000ull) }; nanosleep(&ts, NULL); }
        next[cur] = sched + (uint64_t)exp_draw(&a->rng, mean);
        int64_t lv = a->last_value;
        int r = do_txn(a);
        uint64_t tries = 0;
        while (r == 0 && g_retry && tries < (uint64_t)g_retry && phase < 2) { tries++; struct timespec ts = { 0, 20000 }; nanosleep(&ts, NULL); r = do_txn(a); }
        uint64_t dt = now_ns() - sched;
        if (r == 0) { a->last_value = lv; if (phase == 1) { t->gave_up++; t->errors++; } continue; }
        if (phase != 1) continue;
        t->busy += tries;
        if (tries > t->max_retries) t->max_retries = tries;
        if (r == 1) t->writes++; else if (r == 2) t->reads++; else { t->errors++; continue; }
        if (t->nlat == t->cap) { t->cap = t->cap ? t->cap * 2 : 1 << 16; t->lat = realloc(t->lat, t->cap * sizeof(uint64_t)); }
        t->lat[t->nlat++] = dt;
    }
    free(next);
    return NULL;
}

static void *thread_main (void *arg) {
    thread_t *t = arg;
    if (g_poisson_tps > 0) return thread_main_poisson(arg);
    int cur = 0;
    while (phase < 2) {
        agent_t *a = &t->agents[cur]; cur = (cur + 1) % t->n;
        uint64_t t0 = now_ns();
        int64_t lv = a->last_value;                                 // (a transaction that never commits must not leave its value behind)
        int r = do_txn(a);
        uint64_t tries = 0;
        if (r == 0 && g_retry) {                                   // application-level retry: same transaction until it commits
            uint64_t backoff_us = 5;
            while (r == 0 && tries < (uint64_t)g_retry && phase < 2) {
                tries++;
                struct timespec ts = { 0, (long)((backoff_us + xs(&a->rng) % (backoff_us + 1)) * 1000) };
                nanosleep(&ts, NULL);
                if (backoff_us < 2000) backoff_us *= 2;
                r = do_txn(a);
            }
            if (r == 0 && phase == 1) { t->gave_up++; t->errors++; a->last_value = lv; continue; }
            if (r == 0) { a->last_value = lv; continue; }
        }
        uint64_t dt = now_ns() - t0;
        if (phase != 1) continue;
        if (cfg.begin_wait && r == 1) {
            if (t->nwt == t->capw) { t->capw = t->capw ? t->capw * 2 : 1 << 16; t->wt = realloc(t->wt, t->capw * sizeof(uint64_t)); }
            t->wt[t->nwt++] = a->begin_ok_ns - t0;
        }
        if (tries) { t->busy += tries; if (tries > t->max_retries) t->max_retries = tries; }
        if (r == 1) t->writes++;
        else if (r == 2) t->reads++;
        else if (r == 0) { t->busy++; a->last_value = lv; continue; }
        else { t->errors++; continue; }
        if (t->nlat == t->cap) { t->cap = t->cap ? t->cap * 2 : 1 << 16; t->lat = realloc(t->lat, t->cap * sizeof(uint64_t)); }
        t->lat[t->nlat++] = dt;
    }
    return NULL;
}

// long-lived reader (workload J): pins a snapshot for the whole run
static sqlite3 *g_pinned;
static void pin_reader (void) {
    agent_t r; memset(&r, 0, sizeof r);
    if (open_agent(&r) != SQLITE_OK) return;
    exec_(r.db, "BEGIN");
    scalar_(r.db, "SELECT count(*) FROM t");
    g_pinned = r.db;
}

static int cmp_u64 (const void *a, const void *b) { uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b; return x < y ? -1 : x > y; }
static void die (const char *m) { fprintf(stderr, "mw_bench: %s\n", m); exit(2); }
static int64_t fsize (const char *p) { struct stat sb; return stat(p, &sb) == 0 ? (int64_t)sb.st_size : 0; }

static void on_segv (int sig) { void *bt[40]; int n = backtrace(bt, 40); backtrace_symbols_fd(bt, n, 2); _exit(128 + sig); }

#include <execinfo.h>
#include <signal.h>
static void crash_bt (int sig) { void *bt[40]; int n = backtrace(bt, 40); fprintf(stderr, "CRASH signal %d\n", sig); backtrace_symbols_fd(bt, n, 2); _exit(139); }

int main (int argc, char **argv) {
    if (getenv("MW_BENCH_BT")) { signal(SIGBUS, crash_bt); signal(SIGSEGV, crash_bt); }
    if (getenv("MW_BT")) { signal(SIGSEGV, on_segv); signal(SIGBUS, on_segv); }
    sqlite3_config(SQLITE_CONFIG_MEMSTATUS, 0);      // applications may drop memory accounting: it takes a global mutex on every allocation
    for (int i = 1; i < argc; i++) {
        #define ARG(n) (!strcmp(argv[i], n) && i + 1 < argc)
        if ARG("--mode") { i++; cfg.mode = !strcmp(argv[i], "stock") ? M_STOCK : !strcmp(argv[i], "stock-wal") ? M_STOCK_WAL : M_MW; }
        else if ARG("--agents") cfg.agents = atoi(argv[++i]);
        else if ARG("--threads") cfg.threads = atoi(argv[++i]);
        else if ARG("--duration") cfg.duration = atoi(argv[++i]);
        else if ARG("--warmup") cfg.warmup = atoi(argv[++i]);
        else if ARG("--seed") cfg.seed = atoi(argv[++i]);
        else if ARG("--rows") cfg.rows = atoi(argv[++i]);
        else if ARG("--read-pct") cfg.read_pct = atoi(argv[++i]);
        else if ARG("--rebase") cfg.rebase = atoi(argv[++i]);
        else if ARG("--mp") cfg.mp = atoi(argv[++i]);
        else if ARG("--path") cfg.path = argv[++i];
        else if (!strcmp(argv[i], "--setup-only")) cfg.setup_only = 1;
        else if (!strcmp(argv[i], "--no-setup")) cfg.no_setup = 1;
        else if ARG("--agent-base") cfg.agent_base = atoi(argv[++i]);
        else if ARG("--barrier") g_barrier = argv[++i];
        else if ARG("--verify-sum") cfg.verify_sum = atoll(argv[++i]);
        else if ARG("--retry") g_retry = atoi(argv[++i]);
        else if ARG("--poisson-tps") g_poisson_tps = atof(argv[++i]);
        else if (!strcmp(argv[i], "--begin-wait")) cfg.begin_wait = 1;
        else if ARG("--busy-ms") cfg.busy_ms = atoi(argv[++i]);
        else if ARG("--think-us") cfg.think_us = atoi(argv[++i]);
        else if ARG("--sync") cfg.sync = argv[++i];
        else if ARG("--csv") cfg.csv = argv[++i];
        else if ARG("--workload") {
            i++; cfg.wl = -1;
            for (int k = 0; k < W_COUNT; k++) if (!strcmp(argv[i], wl_name[k])) cfg.wl = k;
            if (cfg.wl < 0) die("unknown workload");
        } else die("unknown argument");
    }
    long cores = sysconf(_SC_NPROCESSORS_ONLN);
    if (cfg.threads <= 0) cfg.threads = cfg.agents < 2 * cores ? cfg.agents : (int)(2 * cores);
    if (cfg.rows < cfg.agents + 1) cfg.rows = cfg.agents + 1;
    if (cfg.rows < 8) cfg.rows = 8;
    if (cfg.wl == W_GROUPS && cfg.rows < 1100) cfg.rows = 1100;

    char path[300]; if (!cfg.path) { snprintf(path, sizeof path, "/tmp/mw_bench_%d.db", (int)getpid()); cfg.path = path; } else snprintf(path, sizeof path, "%s", cfg.path);
    bool shared_db = cfg.no_setup || cfg.setup_only || cfg.verify_sum >= 0;
    char p2[340]; const char *sfx[] = {"", "-wal", "-shm", "-journal", "-mw"};
    if (!cfg.no_setup && cfg.verify_sum < 0) for (int i = 0; i < 5; i++) { snprintf(p2, sizeof p2, "%s%s", path, sfx[i]); unlink(p2); }
    int64_t ct_rows0 = 0;
    char sql[700];
    if (!cfg.no_setup && cfg.verify_sum < 0) {
    // ---- setup with a stock connection. t = one row per page (3000-byte pad); ct = 4 int columns, small rows.
    sqlite3 *s;
    if (sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix") != SQLITE_OK) die("setup open");
    exec_(s, cfg.mode == M_STOCK ? "PRAGMA journal_mode=DELETE" : "PRAGMA journal_mode=WAL");
    exec_(s, "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER, pad BLOB)");
    snprintf(sql, sizeof sql, "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<%d) INSERT INTO t SELECT i, 0, zeroblob(3000) FROM n", cfg.rows);
    exec_(s, sql);
    exec_(s, (getenv("MW_BENCH_TRACK_BK") || getenv("MW_BENCH_BK_TEXT")) ? "CREATE TABLE bk(id TEXT PRIMARY KEY NOT NULL, v TEXT)" : "CREATE TABLE bk(id INTEGER PRIMARY KEY, v TEXT)");
    exec_(s, "CREATE TABLE bkw(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER, d INTEGER, v TEXT)");
    exec_(s, "CREATE TABLE tu(id TEXT PRIMARY KEY, v INTEGER); CREATE TABLE ti(id INTEGER PRIMARY KEY, v INTEGER); CREATE TABLE ta(id INTEGER PRIMARY KEY AUTOINCREMENT, v INTEGER)");
    {
        exec_(s, "CREATE TABLE ct(id TEXT PRIMARY KEY NOT NULL, a INTEGER, b INTEGER, c INTEGER, d INTEGER, pad TEXT)");
        snprintf(sql, sizeof sql, "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<%d) INSERT INTO ct SELECT 'r'||i, 0, 0, 0, 0, 'p' FROM n", cfg.rows);
        exec_(s, sql);
    }
    if (cfg.wl == W_LONGREADER) { /* t has cfg.rows rows */ }
    int64_t seed_rows = scalar_(s, "SELECT count(*) FROM tu") + scalar_(s, "SELECT count(*) FROM ti") + scalar_(s, "SELECT count(*) FROM ta");
    ct_rows0 = scalar_(s, "SELECT count(*) FROM ct");
    sqlite3_close(s);
    (void)seed_rows;
    if (cfg.setup_only) { printf("setup done: %s rows=%d\n", path, cfg.rows); return 0; }
    }

    if (cfg.verify_sum >= 0) {                               // multi-process runs: check the shared database once, after every process exited
        agent_t v; memset(&v, 0, sizeof v);
        if (open_agent(&v) != SQLITE_OK) die("verify open");
        long long want = cfg.verify_sum;
        if (cfg.wl == W_BULK) want = cfg.verify_sum * 100;                 // (bulk: 100 rows per committed transaction, counted)
        int64_t sum = cfg.wl == W_BULK ? scalar_(v.db, "SELECT count(*) FROM bk") : scalar_(v.db, (cfg.wl == W_HOT || cfg.wl == W_SLOWHOT || cfg.wl == W_SLOWPAGE || cfg.wl == W_SAMEPAGE || cfg.wl == W_GROUPS) ? "SELECT sum(a) FROM ct" : "SELECT sum(v) FROM t");
        char icv[64]; snprintf(icv, sizeof icv, "%s", text_(v.db, "PRAGMA integrity_check"));
        bool okv = sum == want && !strcmp(icv, "ok");
        printf("   verify: sum(v)=%lld expected=%lld integrity_check=%s -> %s\n", (long long)sum, want, icv, okv ? "VALID" : "INVALID");
        sqlite3_close(v.db);
        for (int i = 0; i < 5; i++) { snprintf(p2, sizeof p2, "%s%s", path, sfx[i]); unlink(p2); }
        snprintf(p2, sizeof p2, "%s-mwlock", path); unlink(p2); snprintf(p2, sizeof p2, "%s-mwlk", path); unlink(p2);
        return okv ? 0 : 1;
    }
    agent_t *agents = calloc((size_t)cfg.agents, sizeof(agent_t));
    for (int i = 0; i < cfg.agents; i++) {
        agents[i].id = cfg.agent_base + i;
        agents[i].rng = 0x9E3779B97F4A7C15ull * (uint64_t)(cfg.seed * 1000003 + i + 1);
        { int orc = open_agent(&agents[i]); if (orc != SQLITE_OK) { fprintf(stderr, "agent open rc=%d %s\n", orc, agents[i].db ? sqlite3_errmsg(agents[i].db) : ""); die("agent open"); } }
    }
    static const char *pragmas[] = { "journal_mode", "synchronous", "page_size", "cache_size", "mmap_size", "locking_mode", "wal_autocheckpoint", "busy_timeout", "auto_vacuum", "temp_store" };
    printf("== mw_bench: mode=%s workload=%s agents=%d threads=%d duration=%ds warmup=%ds seed=%d rows=%d rebase=%d read_pct=%d\n",
           mode_name[cfg.mode], wl_name[cfg.wl], cfg.agents, cfg.threads, cfg.duration, cfg.warmup, cfg.seed, cfg.rows, cfg.rebase, cfg.read_pct);
    printf("   sqlite %s cores=%ld  PRAGMAs (as read back from an agent connection):", sqlite3_libversion(), cores);
    for (unsigned i = 0; i < sizeof pragmas / sizeof pragmas[0]; i++) { char q[64]; snprintf(q, sizeof q, "PRAGMA %s", pragmas[i]); printf(" %s=%s", pragmas[i], text_(agents[0].db, q)); }
    printf("%s\n", cfg.mode == M_MW ? "\n   (Multi-Writer: durable commit log fsynced per commit batch when synchronous>=FULL; mmap forced off; cold page cache per transaction)" : "");

    thread_t *th = calloc((size_t)cfg.threads, sizeof(thread_t));
    for (int i = 0; i < cfg.threads; i++) th[i].agents = calloc((size_t)(cfg.agents / cfg.threads + 2), sizeof(agent_t));
    for (int i = 0; i < cfg.agents; i++) { thread_t *t = &th[i % cfg.threads]; t->agents[t->n++] = agents[i]; }
    if (cfg.wl == W_LONGREADER) pin_reader();

    mw_db_stats m0; memset(&m0, 0, sizeof m0);
    if (cfg.mode == M_MW) sqlite3_file_control(th[0].agents[0].db, "main", MW_FCNTL_DBSTATS, &m0);
    struct rusage r0, r1;
    phase = 0;
    pthread_t tid[cfg.threads];
    if (g_barrier) {                                                         // all processes start their measured loops together
        char f[400]; snprintf(f, sizeof f, "%s/ready.%d", g_barrier, (int)getpid());
        FILE *rf = fopen(f, "w"); if (rf) fclose(rf);
        snprintf(f, sizeof f, "%s/go", g_barrier);
        for (int k = 0; k < 60000 && access(f, F_OK) != 0; k++) usleep(1000);
    }
    for (int i = 0; i < cfg.threads; i++) pthread_create(&tid[i], NULL, thread_main, &th[i]);
    if (cfg.warmup) sleep((unsigned)cfg.warmup);
    for (int i = 0; i < cfg.threads; i++) { th[i].nlat = 0; th[i].nwt = 0; th[i].reads = th[i].writes = th[i].busy = th[i].errors = th[i].max_retries = th[i].gave_up = 0; }   // discard warm-up samples
    if (cfg.mode == M_MW) sqlite3_file_control(th[0].agents[0].db, "main", MW_FCNTL_DBSTATS, &m0);
    getrusage(RUSAGE_SELF, &r0);
    uint64_t t0 = now_ns();
    phase = 1;
    sleep((unsigned)cfg.duration);
    phase = 2;
    uint64_t elapsed = now_ns() - t0;
    getrusage(RUSAGE_SELF, &r1);
    for (int i = 0; i < cfg.threads; i++) pthread_join(tid[i], NULL);

    // ---- aggregate
    uint64_t reads = 0, writes = 0, busy = 0, errors = 0, total = 0;
    for (int i = 0; i < cfg.threads; i++) { reads += th[i].reads; writes += th[i].writes; busy += th[i].busy; errors += th[i].errors; total += th[i].nlat; }
    uint64_t *all = malloc((total ? total : 1) * sizeof(uint64_t)); size_t k = 0;
    for (int i = 0; i < cfg.threads; i++) { memcpy(all + k, th[i].lat, th[i].nlat * sizeof(uint64_t)); k += th[i].nlat; }
    qsort(all, total, sizeof(uint64_t), cmp_u64);
    uint64_t nw = 0; for (int i = 0; i < cfg.threads; i++) nw += th[i].nwt;
    uint64_t *wall = malloc((nw ? nw : 1) * sizeof(uint64_t)); { size_t kk = 0; for (int i = 0; i < cfg.threads; i++) { memcpy(wall + kk, th[i].wt, th[i].nwt * sizeof(uint64_t)); kk += th[i].nwt; } }
    qsort(wall, nw, sizeof(uint64_t), cmp_u64);
    double wsum = 0; for (size_t i = 0; i < nw; i++) wsum += (double)wall[i];
    double w_mean = nw ? wsum / (double)nw / 1000.0 : 0, w_p50 = nw ? wall[(size_t)(0.50 * (double)(nw - 1))] / 1000.0 : 0, w_p99 = nw ? wall[(size_t)(0.99 * (double)(nw - 1))] / 1000.0 : 0, w_max = nw ? wall[nw - 1] / 1000.0 : 0;
    #define PCT(p) (total ? all[(size_t)((p) * (double)(total - 1))] / 1000.0 : 0.0)
    double secs = (double)elapsed / 1e9;
    double cpu = ((r1.ru_utime.tv_sec - r0.ru_utime.tv_sec) + (r1.ru_utime.tv_usec - r0.ru_utime.tv_usec) / 1e6 +
                  (r1.ru_stime.tv_sec - r0.ru_stime.tv_sec) + (r1.ru_stime.tv_usec - r0.ru_stime.tv_usec) / 1e6);
    #ifdef __APPLE__
    double rss_mb = (double)r1.ru_maxrss / (1024.0 * 1024.0);
    #else
    double rss_mb = (double)r1.ru_maxrss / 1024.0;
    #endif

    mw_db_stats m1; memset(&m1, 0, sizeof m1);
    sqlite3 *v = th[0].agents[0].db;
    if (cfg.mode == M_MW) sqlite3_file_control(v, "main", MW_FCNTL_DBSTATS, &m1);
    int64_t db_bytes = fsize(path), wal_bytes = fsize("") ; (void)wal_bytes;
    char wp[340]; snprintf(wp, sizeof wp, "%s-wal", path); int64_t wal_size = fsize(wp);
    snprintf(wp, sizeof wp, "%s-mw", path); int64_t log_size = fsize(wp);

    // ---- release the pinned reader after measuring retention (it is the point of workload J)
    if (g_pinned) { exec_(g_pinned, "COMMIT"); sqlite3_close(g_pinned); g_pinned = NULL; }

    // ---- correctness validation: never accept a result whose data is wrong
    int valid = 1; char why[200] = "";
    uint64_t expect = 0;
    for (int i = 0; i < cfg.threads; i++) for (int j = 0; j < th[i].n; j++) expect += th[i].agents[j].commits;
    if (shared_db) { /* several processes share the database: the driver verifies once at the end */ } else
    switch (cfg.wl) {
        case W_BULK: {
            int64_t n = scalar_(v, getenv("MW_BENCH_BULK_WIDE") ? "SELECT count(*) FROM bkw" : "SELECT count(*) FROM bk");
            if ((uint64_t)n != expect * 100) { valid = 0; snprintf(why, sizeof why, "count(bk)=%lld != 100 x committed %llu", (long long)n, (unsigned long long)expect); }
            break;
        }
        case W_INDEPENDENT: case W_MIXED: case W_LONGTX: case W_LONGREADER: case W_SLOWTX: {
            int64_t sum = scalar_(v, "SELECT sum(v) FROM t");
            if ((uint64_t)sum != expect) { valid = 0; snprintf(why, sizeof why, "sum(t.v)=%lld != committed %llu", (long long)sum, (unsigned long long)expect); }
            break;
        }
        case W_SAMEPAGE: case W_HOT: case W_SLOWHOT: case W_SLOWPAGE: case W_GROUPS: {
            int64_t sum = scalar_(v, "SELECT sum(a) FROM ct");
            if ((uint64_t)sum != expect) { valid = 0; snprintf(why, sizeof why, "sum(ct.a)=%lld != committed %llu", (long long)sum, (unsigned long long)expect); }
            break;
        }
        case W_COLS:   // every (row, column) cell holds its agent's last committed value: no update lost across columns
            for (int i = 0; i < cfg.threads && valid; i++) for (int j = 0; j < th[i].n && valid; j++) {
                agent_t *a = &th[i].agents[j];
                if (!a->commits) continue;
                static const char *col[4] = { "a", "b", "c", "d" };
                snprintf(sql, sizeof sql, "SELECT %s FROM ct WHERE id='r%d'", col[a->id % 4], a->id / 4);
                int64_t got = scalar_(v, sql);
                if (got != a->last_value) { valid = 0; snprintf(why, sizeof why, "agent %d cell holds %lld, last committed %lld", a->id, (long long)got, (long long)a->last_value); }
            }
            break;
        case W_SAMECOL: {   // deterministic convergence: the surviving value is one some agent really wrote
            int64_t got = scalar_(v, "SELECT a FROM ct WHERE id='r0'");
            int64_t agent = got / 1000000000ll, seq = got % 1000000000ll;
            bool ok = got == 0 && writes == 0;
            for (int i = 0; i < cfg.threads && !ok; i++) for (int j = 0; j < th[i].n && !ok; j++) {
                agent_t *a = &th[i].agents[j];
                if (a->id == agent && seq >= 1 && (uint64_t)seq <= a->commits) ok = true;
            }
            if (!ok) { valid = 0; snprintf(why, sizeof why, "surviving value %lld was never committed", (long long)got); }
            break;
        }
        case W_CRDTINSERT: {
            int64_t n = scalar_(v, "SELECT count(*) FROM ct");
            if ((uint64_t)(n - ct_rows0) != expect) { valid = 0; snprintf(why, sizeof why, "rows=%lld, expected %lld", (long long)(n - ct_rows0), (long long)(ct_rows0 + (int64_t)expect)); }
            break;
        }
        case W_INS_UUID: if ((uint64_t)scalar_(v, "SELECT count(*) FROM tu") != expect) { valid = 0; snprintf(why, sizeof why, "tu rows != committed"); } break;
        case W_INS_INT:  if ((uint64_t)scalar_(v, "SELECT count(*) FROM ti") != expect) { valid = 0; snprintf(why, sizeof why, "ti rows != committed"); } break;
        case W_INS_AUTO: if ((uint64_t)scalar_(v, "SELECT count(*) FROM ta") != expect) { valid = 0; snprintf(why, sizeof why, "ta rows != committed"); } break;
        default: break;
    }
    char ic[64]; snprintf(ic, sizeof ic, "%s", shared_db ? "ok" : text_(v, "PRAGMA integrity_check"));
    if (strcmp(ic, "ok")) { valid = 0; snprintf(why, sizeof why, "integrity_check: %s", ic); }

    printf("   result: %s%s%s  (integrity_check=%s, committed=%llu)\n", valid ? "VALID" : "INVALID", why[0] ? " - " : "", why, ic, (unsigned long long)expect);
    printf("   tx/s=%.0f  writes/s=%.0f  reads/s=%.0f  busy+retry=%llu  errors=%llu\n", (double)(reads + writes) / secs, (double)writes / secs, (double)reads / secs, (unsigned long long)busy, (unsigned long long)errors);
    printf("   latency us: p50=%.1f p95=%.1f p99=%.1f max=%.1f   cpu=%.2fs (%.0f%% of one core)  peak_rss=%.0f MB\n", PCT(0.50), PCT(0.95), PCT(0.99), total ? all[total - 1] / 1000.0 : 0.0, cpu, 100.0 * cpu / secs, rss_mb);
    if (g_retry) {
        uint64_t mr = 0, gu = 0; for (int i = 0; i < cfg.threads; i++) { if (th[i].max_retries > mr) mr = th[i].max_retries; gu += th[i].gave_up; }
        printf("   RETRY MODE (limit %d): retried attempts=%llu  max retries for one transaction=%llu  transactions that gave up=%llu  -> %s\n", g_retry, (unsigned long long)busy, (unsigned long long)mr, (unsigned long long)gu, gu ? "SOME FAILED" : "NO TRANSACTION FAILED");
    }
    printf("   FAILED attempts (SQLITE_BUSY/LOCKED/BUSY_SNAPSHOT surfaced to the application; in retry mode: refused attempts that were retried): %llu of %llu = %.2f%%\n", (unsigned long long)busy, (unsigned long long)(busy + reads + writes), (busy + reads + writes) ? 100.0 * (double)busy / (double)(busy + reads + writes) : 0.0);
    printf("   size: db=%.1f MB wal=%.1f MB mw_log=%.1f MB\n", (double)db_bytes / 1048576.0, (double)wal_size / 1048576.0, (double)log_size / 1048576.0);
    { extern void mw_reloc_dump(void); if (getenv("MW_DEBUG") || getenv("MW_TIMING")) mw_reloc_dump(); }
    uint64_t dcommits = m1.commits - m0.commits;
    if (cfg.mode == M_MW) {
        printf("   mw: commits=%llu fast=%llu page_conflicts=%llu read_conflicts=%llu rebases=%llu rebase_retries=%llu max_rebase_attempts=%llu relocations=%llu reads_saved=%llu merges=%llu\n",
               (unsigned long long)dcommits, (unsigned long long)(m1.fast_commits - m0.fast_commits), (unsigned long long)(m1.page_conflicts - m0.page_conflicts),
               (unsigned long long)(m1.read_conflicts - m0.read_conflicts), (unsigned long long)(m1.rebases - m0.rebases), (unsigned long long)(m1.rebase_retries - m0.rebase_retries),
               (unsigned long long)m1.rebase_max_attempts, (unsigned long long)(m1.relocations - m0.relocations), (unsigned long long)(m1.reads_saved - m0.reads_saved), (unsigned long long)(m1.merges - m0.merges));
        printf("   mw: compaction: %llu runs, %.0f ms busy, %llu pages written (%.1f per commit), log now %.1f MB\n", (unsigned long long)(m1.compactions - m0.compactions), (double)(m1.compaction_ns - m0.compaction_ns) / 1e6, (unsigned long long)(m1.compacted_pages - m0.compacted_pages), dcommits ? (double)(m1.compacted_pages - m0.compacted_pages) / (double)dcommits : 0.0, (double)m1.log_bytes / 1048576.0);
        printf("   mw: pages/commit=%.2f fsync_ms=%.1f rebase_ms=%.1f  versions=%llu retained=%.1f MB reclaimed=%llu gc_runs=%llu gc_ms=%.2f oldest_snapshot=%llu epoch=%llu backlog=%llu\n",
               dcommits ? (double)(m1.pages_published - m0.pages_published) / (double)dcommits : 0.0, (double)(m1.log_sync_ns - m0.log_sync_ns) / 1e6, (double)(m1.rebase_ns - m0.rebase_ns) / 1e6,
               (unsigned long long)m1.page_versions, (double)m1.bytes_retained / 1048576.0, (unsigned long long)(m1.versions_reclaimed - m0.versions_reclaimed),
               (unsigned long long)(m1.gc_runs - m0.gc_runs), (double)(m1.gc_ns - m0.gc_ns) / 1e6, (unsigned long long)m1.oldest_active_snapshot, (unsigned long long)m1.epoch, (unsigned long long)m1.compaction_backlog);
    }
    printf("JSON {\"mode\":\"%s\",\"workload\":\"%s\",\"agents\":%d,\"threads\":%d,\"duration\":%.2f,\"sync\":\"%s\",\"rebase\":%d,\"seed\":%d,\"valid\":%d,\"tx_per_s\":%.1f,\"writes_per_s\":%.1f,\"reads_per_s\":%.1f,"
           "\"busy\":%llu,\"errors\":%llu,\"p50_us\":%.1f,\"p95_us\":%.1f,\"p99_us\":%.1f,\"p999_us\":%.1f,\"max_us\":%.1f,\"cpu_s\":%.2f,\"rss_mb\":%.0f,\"db_mb\":%.2f,\"wal_mb\":%.2f,\"log_mb\":%.2f,"
           "\"wait_mean_us\":%.1f,\"wait_p50_us\":%.1f,\"wait_p99_us\":%.1f,\"wait_max_us\":%.1f,\"mw_fast\":%llu,\"mw_page_conflicts\":%llu,\"mw_read_conflicts\":%llu,\"mw_rebase_ms\":%.1f,\"mw_rebases\":%llu,\"mw_rebase_retries\":%llu,\"mw_versions\":%llu,\"mw_retained_mb\":%.2f,\"mw_reclaimed\":%llu,\"mw_gc_ms\":%.2f,\"mw_pages_per_commit\":%.2f,\"committed\":%llu}\n",
           mode_name[cfg.mode], wl_name[cfg.wl], cfg.agents, cfg.threads, secs, cfg.sync, cfg.rebase, cfg.seed, valid, (double)(reads + writes) / secs, (double)writes / secs, (double)reads / secs,
           (unsigned long long)busy, (unsigned long long)errors, PCT(0.50), PCT(0.95), PCT(0.99), PCT(0.999), total ? all[total - 1] / 1000.0 : 0.0, cpu, rss_mb,
           (double)db_bytes / 1048576.0, (double)wal_size / 1048576.0, (double)log_size / 1048576.0,
           w_mean, w_p50, w_p99, w_max, (unsigned long long)(m1.fast_commits - m0.fast_commits), (unsigned long long)(m1.page_conflicts - m0.page_conflicts), (unsigned long long)(m1.read_conflicts - m0.read_conflicts),
           (double)(m1.rebase_ns - m0.rebase_ns) / 1e6, (unsigned long long)(m1.rebases - m0.rebases), (unsigned long long)(m1.rebase_retries - m0.rebase_retries), (unsigned long long)m1.page_versions, (double)m1.bytes_retained / 1048576.0,
           (unsigned long long)(m1.versions_reclaimed - m0.versions_reclaimed), (double)(m1.gc_ns - m0.gc_ns) / 1e6, dcommits ? (double)(m1.pages_published - m0.pages_published) / (double)dcommits : 0.0, (unsigned long long)expect);
    if (cfg.csv) {
        bool fresh = access(cfg.csv, F_OK) != 0;
        FILE *f = fopen(cfg.csv, "a");
        if (f) {
            if (fresh) fprintf(f, "mode,workload,agents,threads,sync,rebase,seed,valid,tx_per_s,writes_per_s,reads_per_s,busy,errors,p50_us,p95_us,p99_us,max_us,cpu_s,rss_mb,db_mb,wal_mb,log_mb,mw_fast,mw_page_conflicts,mw_read_conflicts,mw_rebases,mw_rebase_retries,mw_versions,mw_retained_mb,mw_reclaimed,mw_gc_ms,mw_pages_per_commit\n");
            fprintf(f, "%s,%s,%d,%d,%s,%d,%d,%d,%.1f,%.1f,%.1f,%llu,%llu,%.1f,%.1f,%.1f,%.1f,%.2f,%.0f,%.2f,%.2f,%.2f,%llu,%llu,%llu,%llu,%llu,%llu,%.2f,%llu,%.2f,%.2f\n",
                    mode_name[cfg.mode], wl_name[cfg.wl], cfg.agents, cfg.threads, cfg.sync, cfg.rebase, cfg.seed, valid, (double)(reads + writes) / secs, (double)writes / secs, (double)reads / secs,
                    (unsigned long long)busy, (unsigned long long)errors, PCT(0.50), PCT(0.95), PCT(0.99), total ? all[total - 1] / 1000.0 : 0.0, cpu, rss_mb,
                    (double)db_bytes / 1048576.0, (double)wal_size / 1048576.0, (double)log_size / 1048576.0,
                    (unsigned long long)(m1.fast_commits - m0.fast_commits), (unsigned long long)(m1.page_conflicts - m0.page_conflicts), (unsigned long long)(m1.read_conflicts - m0.read_conflicts),
                    (unsigned long long)(m1.rebases - m0.rebases), (unsigned long long)(m1.rebase_retries - m0.rebase_retries), (unsigned long long)m1.page_versions, (double)m1.bytes_retained / 1048576.0,
                    (unsigned long long)(m1.versions_reclaimed - m0.versions_reclaimed), (double)(m1.gc_ns - m0.gc_ns) / 1e6, dcommits ? (double)(m1.pages_published - m0.pages_published) / (double)dcommits : 0.0);
            fclose(f);
        }
    }

    for (int i = 0; i < cfg.threads; i++) for (int j = 0; j < th[i].n; j++) {
        sqlite3_close(th[i].agents[j].db);
    }
    if (getenv("MW_BENCH_KEEP") && cfg.mode == M_MW) { mw_compact_result cr; memset(&cr, 0, sizeof cr); sqlite3_file_control(th[0].agents[0].db, "main", MW_FCNTL_COMPACT, &cr); }   // (inspect the database afterwards: the file holds the final state)
    if (!shared_db && !getenv("MW_BENCH_KEEP")) for (int i = 0; i < 5; i++) { snprintf(p2, sizeof p2, "%s%s", path, sfx[i]); unlink(p2); }
    return valid ? 0 : 1;
}
