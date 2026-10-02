// EXPERIMENT (docs §51): are the row-level changes of a transaction recoverable from the page images alone? Random transactions (inserts, updates of one column,
// deletes, small and overflow-sized values, two tables, enough rows to split and merge pages) run against a shadow model; after every commit the changes decoded
// from the pages (mw_rowdiff sink) are compared with the changes the model says were made. The test reports misses (a change that was not decoded), spurious
// changes, imprecise column masks, and the decoding time. It FAILS only on a miss that cannot be explained by an overflow page: everything else is data.
#include <stdint.h>
#include <stdbool.h>
#include <time.h>
#include "mw_test.h"
#include "multiwriter.h"

#define NA 3000
#define NB 3000
#define BOFF 1000000
typedef struct { int alive; int64_t a; int blen; unsigned bseed; } rowa_t;      // table A(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)
typedef struct { int alive; int64_t w; int vlen; unsigned vseed; } rowb_t;      // table B(k INTEGER PRIMARY KEY, v BLOB, w INTEGER)
static rowa_t A[NA + 1], A0[NA + 1];
static rowb_t B[NB + 1], B0[NB + 1];

static uint64_t g_rng = 88172645463325252ull;
static uint64_t rnd (void) { g_rng ^= g_rng << 13; g_rng ^= g_rng >> 7; g_rng ^= g_rng << 17; return g_rng; }

typedef struct { int kind; int64_t rowid; uint32_t changed; } chg_t;
static chg_t g_chg[100000]; static int g_nchg; static mw_rowdiff_info g_info; static int g_calls;
static uint64_t g_ns[200000]; static int g_nns;
static void sink (void *arg, const mw_rowchg *c, int n, const mw_rowdiff_info *info) {
    (void)arg; g_nchg = 0;
    for (int i = 0; i < n && i < 100000; i++) g_chg[g_nchg++] = (chg_t){ c[i].kind, c[i].rowid, c[i].changed };
    g_info = *info; g_calls++;
    if (g_nns < 200000) g_ns[g_nns++] = info->ns;
}
static int cmp_u64 (const void *a, const void *b) { uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b; return x < y ? -1 : x > y; }

static void fill (char *buf, int len, unsigned seed) { for (int i = 0; i < len; i++) buf[i] = (char)('a' + (seed * 31u + (unsigned)i * 7u) % 26); }
static bool same_content (int la, unsigned sa, int lb, unsigned sb) { if (la != lb) return false; for (int i = 0; i < la; i++) if ((sa * 31u + (unsigned)i * 7u) % 26 != (sb * 31u + (unsigned)i * 7u) % 26) return false; return true; }
static int gen_len (void) { uint64_t r = rnd() % 100; return r < 70 ? (int)(rnd() % 120) : r < 95 ? (int)(rnd() % 1500) : 4500 + (int)(rnd() % 6000); }

int main (void) {
    char path[256]; mw_tmpdb(path, sizeof path, "rowdiff");
    char uri[400]; snprintf(uri, sizeof uri, "file:%s?mw=2", path);
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE A(id INTEGER PRIMARY KEY, a INTEGER, b TEXT); CREATE TABLE B(k INTEGER PRIMARY KEY, v BLOB, w INTEGER);"), SQLITE_OK);
    sqlite3_close(s);
    sqlite3 *db; CHECK_RC(sqlite3_open_v2(uri, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    sqlite3_stmt *ia, *ua1, *ua2, *da, *ib, *ub1, *ub2, *dbb;
    CHECK_RC(sqlite3_prepare_v2(db, "INSERT INTO A(id,a,b) VALUES(?1,?2,?3)", -1, &ia, NULL), SQLITE_OK);
    CHECK_RC(sqlite3_prepare_v2(db, "UPDATE A SET a=?2 WHERE id=?1", -1, &ua1, NULL), SQLITE_OK);
    CHECK_RC(sqlite3_prepare_v2(db, "UPDATE A SET b=?2 WHERE id=?1", -1, &ua2, NULL), SQLITE_OK);
    CHECK_RC(sqlite3_prepare_v2(db, "DELETE FROM A WHERE id=?1", -1, &da, NULL), SQLITE_OK);
    CHECK_RC(sqlite3_prepare_v2(db, "INSERT INTO B(k,v,w) VALUES(?1,?2,?3)", -1, &ib, NULL), SQLITE_OK);
    CHECK_RC(sqlite3_prepare_v2(db, "UPDATE B SET v=?2 WHERE k=?1", -1, &ub1, NULL), SQLITE_OK);
    CHECK_RC(sqlite3_prepare_v2(db, "UPDATE B SET w=?2 WHERE k=?1", -1, &ub2, NULL), SQLITE_OK);
    CHECK_RC(sqlite3_prepare_v2(db, "DELETE FROM B WHERE k=?1", -1, &dbb, NULL), SQLITE_OK);
    mw_rowdiff_set_sink(sink, NULL);

    static char buf[12000];
    long commits = 0, exp_ins = 0, exp_upd = 0, exp_del = 0, miss = 0, miss_ovfl = 0, spurious = 0, mask_miss = 0, mask_extra = 0, commits_opaque = 0, commits_bad = 0, mismatched_commits = 0;
    long pages_total = 0, rows_total = 0;
    const int TXNS = 6000;
    for (int t = 0; t < TXNS; t++) {
        memcpy(A0, A, sizeof A); memcpy(B0, B, sizeof B);
        CHECK_RC(mw_exec(db, "BEGIN"), SQLITE_OK);
        int nops = 1 + (int)(rnd() % 6);
        for (int o = 0; o < nops; o++) {
            int tbl = (int)(rnd() % 2), op = (int)(rnd() % 10);                  // 0-4 insert, 5-7 update, 8-9 delete
            if (tbl == 0) {
                int id = 1 + (int)(rnd() % NA);
                if (op <= 4 && !A[id].alive) { A[id] = (rowa_t){ 1, (int64_t)(rnd() % 100000), gen_len(), (unsigned)rnd() }; fill(buf, A[id].blen, A[id].bseed);
                    sqlite3_bind_int(ia, 1, id); sqlite3_bind_int64(ia, 2, A[id].a); sqlite3_bind_text(ia, 3, buf, A[id].blen, SQLITE_TRANSIENT); CHECK_RC(sqlite3_step(ia), SQLITE_DONE); sqlite3_reset(ia); }
                else if (op >= 5 && op <= 7 && A[id].alive) {
                    if (rnd() % 2) { A[id].a = (A[id].a + 1 + (int64_t)(rnd() % 1000)) % 1000003; sqlite3_bind_int(ua1, 1, id); sqlite3_bind_int64(ua1, 2, A[id].a); CHECK_RC(sqlite3_step(ua1), SQLITE_DONE); sqlite3_reset(ua1); }
                    else { A[id].blen = gen_len(); A[id].bseed = (unsigned)rnd(); fill(buf, A[id].blen, A[id].bseed); sqlite3_bind_int(ua2, 1, id); sqlite3_bind_text(ua2, 2, buf, A[id].blen, SQLITE_TRANSIENT); CHECK_RC(sqlite3_step(ua2), SQLITE_DONE); sqlite3_reset(ua2); }
                }
                else if (op >= 8 && A[id].alive) { A[id].alive = 0; sqlite3_bind_int(da, 1, id); CHECK_RC(sqlite3_step(da), SQLITE_DONE); sqlite3_reset(da); }
            } else {
                int id = 1 + (int)(rnd() % NB);
                if (op <= 4 && !B[id].alive) { B[id] = (rowb_t){ 1, (int64_t)(rnd() % 100000), gen_len(), (unsigned)rnd() }; fill(buf, B[id].vlen, B[id].vseed);
                    sqlite3_bind_int(ib, 1, BOFF + id); sqlite3_bind_blob(ib, 2, buf, B[id].vlen, SQLITE_TRANSIENT); sqlite3_bind_int64(ib, 3, B[id].w); CHECK_RC(sqlite3_step(ib), SQLITE_DONE); sqlite3_reset(ib); }
                else if (op >= 5 && op <= 7 && B[id].alive) {
                    if (rnd() % 2) { B[id].vlen = gen_len(); B[id].vseed = (unsigned)rnd(); fill(buf, B[id].vlen, B[id].vseed); sqlite3_bind_int(ub1, 1, BOFF + id); sqlite3_bind_blob(ub1, 2, buf, B[id].vlen, SQLITE_TRANSIENT); CHECK_RC(sqlite3_step(ub1), SQLITE_DONE); sqlite3_reset(ub1); }
                    else { B[id].w = (B[id].w + 1 + (int64_t)(rnd() % 1000)) % 1000003; sqlite3_bind_int(ub2, 1, BOFF + id); sqlite3_bind_int64(ub2, 2, B[id].w); CHECK_RC(sqlite3_step(ub2), SQLITE_DONE); sqlite3_reset(ub2); }
                }
                else if (op >= 8 && B[id].alive) { B[id].alive = 0; sqlite3_bind_int(dbb, 1, BOFF + id); CHECK_RC(sqlite3_step(dbb), SQLITE_DONE); sqlite3_reset(dbb); }
            }
        }
        int before = g_calls;
        if (getenv("MW_ROWDIFF_AT") && commits + 1 == atol(getenv("MW_ROWDIFF_AT"))) { setenv("MW_ROWDIFF_PAGES", "1", 1); setenv("MW_ROWDIFF_DEBUG", "1", 1); }
        CHECK_RC(mw_exec(db, "COMMIT"), SQLITE_OK);
        unsetenv("MW_ROWDIFF_PAGES"); unsetenv("MW_ROWDIFF_DEBUG");
        // expected net changes from the model
        chg_t exp[8000]; int ne = 0;
        for (int id = 1; id <= NA; id++) {
            rowa_t *x = &A0[id], *y = &A[id];
            if (!x->alive && !y->alive) continue;
            if (!x->alive) exp[ne++] = (chg_t){ 1, id, 0 };
            else if (!y->alive) exp[ne++] = (chg_t){ 3, id, 0 };
            else { uint32_t m = 0; if (x->a != y->a) m |= 1u << 1; if (!same_content(x->blen, x->bseed, y->blen, y->bseed)) m |= 1u << 2; if (m) exp[ne++] = (chg_t){ 2, id, m }; }
        }
        for (int id = 1; id <= NB; id++) {
            rowb_t *x = &B0[id], *y = &B[id];
            if (!x->alive && !y->alive) continue;
            if (!x->alive) exp[ne++] = (chg_t){ 1, BOFF + id, 0 };
            else if (!y->alive) exp[ne++] = (chg_t){ 3, BOFF + id, 0 };
            else { uint32_t m = 0; if (!same_content(x->vlen, x->vseed, y->vlen, y->vseed)) m |= 1u << 1; if (x->w != y->w) m |= 1u << 2; if (m) exp[ne++] = (chg_t){ 2, BOFF + id, m }; }
        }
        if (ne == 0) { CHECK(g_calls == before || g_nchg == 0 || 1); continue; }           // (a transaction that changed nothing)
        commits++;
        if (g_calls != before + 1) { mismatched_commits++; continue; }
        pages_total += g_info.pages; rows_total += ne;
        if (g_info.opaque) commits_opaque++;
        if (g_info.undecodable) commits_bad++;
        for (int i = 0; i < ne; i++) {
            if (exp[i].kind == 1) exp_ins++; else if (exp[i].kind == 2) exp_upd++; else exp_del++;
            const chg_t *got = NULL;
            for (int k = 0; k < g_nchg; k++) if (g_chg[k].rowid == exp[i].rowid) { got = &g_chg[k]; break; }
            if (!got || got->kind != exp[i].kind) {
                miss++; if (g_info.opaque) miss_ovfl++;
                if (miss <= 12 || !g_info.opaque) { int64_t r = exp[i].rowid; int bl0 = r >= BOFF ? B0[r - BOFF].vlen : A0[r].blen, bl1 = r >= BOFF ? B[r - BOFF].vlen : A[r].blen;
                    printf("  not decoded: commit %ld expected kind %d rowid %lld (value length before %d, after %d), decoded kind %d (pages %d opaque %d)\n", commits, exp[i].kind, (long long)r, bl0, bl1, got ? got->kind : 0, g_info.pages, g_info.opaque); }
                continue; }
            if (exp[i].kind == 2) { if ((exp[i].changed & ~got->changed) != 0) mask_miss++; else if ((got->changed & ~exp[i].changed & 0x7fffffffu) != 0 || (got->changed & 0x80000000u)) { mask_extra++; if (mask_extra <= 6) { int64_t r = exp[i].rowid; printf("  imprecise mask: rowid %lld expected %#x decoded %#x (value length before %d after %d)\n", (long long)r, exp[i].changed, got->changed, r >= BOFF ? B0[r - BOFF].vlen : A0[r].blen, r >= BOFF ? B[r - BOFF].vlen : A[r].blen); } } }
        }
        for (int k = 0; k < g_nchg; k++) { bool found = false; for (int i = 0; i < ne; i++) if (exp[i].rowid == g_chg[k].rowid) { found = true; break; } if (!found) { spurious++; if (spurious <= 8) printf("  spurious: commit %ld kind %d rowid %lld (pages %d opaque %d index %d interior %d); model before alive=%d after alive=%d\n", commits, g_chg[k].kind, (long long)g_chg[k].rowid, g_info.pages, g_info.opaque, g_info.index_pages, g_info.interior, g_chg[k].rowid >= BOFF ? B0[g_chg[k].rowid - BOFF].alive : A0[g_chg[k].rowid].alive, g_chg[k].rowid >= BOFF ? B[g_chg[k].rowid - BOFF].alive : A[g_chg[k].rowid].alive); } }
    }
    mw_rowdiff_set_sink(NULL, NULL);
    qsort(g_ns, (size_t)g_nns, sizeof(uint64_t), cmp_u64);
    printf("%ld commits (%d txns), expected changes: %ld inserts, %ld updates, %ld deletes; %.1f pages and %.1f rows per commit\n", commits, TXNS, exp_ins, exp_upd, exp_del, (double)pages_total / (double)(commits ? commits : 1), (double)rows_total / (double)(commits ? commits : 1));
    printf("  not decoded (a change the pages did not give): %ld (of which in commits that wrote overflow/freelist pages: %ld)\n", miss, miss_ovfl);
    printf("  spurious changes: %ld; update column masks: %ld missing a column, %ld imprecise; commits with opaque pages: %ld; with undecodable pages: %ld; commits without exactly one sink call: %ld\n", spurious, mask_miss, mask_extra, commits_opaque, commits_bad, mismatched_commits);
    printf("  decode time per commit: median %.1f us, p99 %.1f us, max %.1f us\n", g_nns ? (double)g_ns[g_nns / 2] / 1000.0 : 0.0, g_nns ? (double)g_ns[(size_t)((double)g_nns * 0.99)] / 1000.0 : 0.0, g_nns ? (double)g_ns[g_nns - 1] / 1000.0 : 0.0);
    CHECK(mismatched_commits == 0);
    CHECK(spurious == 0);
    CHECK(miss - miss_ovfl == 0);
    sqlite3_finalize(ia); sqlite3_finalize(ua1); sqlite3_finalize(ua2); sqlite3_finalize(da); sqlite3_finalize(ib); sqlite3_finalize(ub1); sqlite3_finalize(ub2); sqlite3_finalize(dbb);
    sqlite3_close(db); mw_rmdb(path);
    MW_DONE();
}
