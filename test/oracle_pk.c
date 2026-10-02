// Differential test: crdt_pk_encode against sqlite-sync's cloudsync_pk_encode on random keys (every type, the edge values of integers and floats, text and blobs of every
// length class), and the decode of what sqlite-sync produced.
#include <math.h>
#include <stdint.h>
#include "mw_test.h"
#include "crdt.h"

extern int sqlite3_cloudsync_init (sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi);
static uint64_t rng = 0x9E3779B97F4A7C15ull;
static uint64_t rnd (void) { rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17; return rng; }

static int64_t gen_int (void) {
    static const int64_t edge[] = { 0, 1, -1, 127, 128, 255, 256, -255, -256, 65535, 65536, 16777215, 16777216, 4294967295LL, 4294967296LL, 1099511627775LL, 1099511627776LL, 281474976710655LL,
                                    281474976710656LL, 72057594037927935LL, 72057594037927936LL, INT64_MAX, INT64_MIN, INT64_MIN + 1, INT64_MAX - 1 };
    uint64_t r = rnd() % 4;
    if (r == 0) return edge[rnd() % (sizeof edge / sizeof *edge)];
    if (r == 1) return (int64_t)(rnd() % 1000) - 500;
    return (int64_t)rnd() >> (rnd() % 63);
}
static double gen_float (void) {
    static const double edge[] = { 0.0, -0.0, 1.0, -1.0, 0.5, 1e300, -1e300, 5e-324, -5e-324, 3.141592653589793, 1.7976931348623157e308, 2.2250738585072014e-308 };
    return rnd() % 3 == 0 ? edge[rnd() % (sizeof edge / sizeof *edge)] : ((double)(int64_t)rnd() / 1e6);
}

int main (void) {
    sqlite3 *db; CHECK_RC(sqlite3_open(":memory:", &db), SQLITE_OK);
    CHECK_RC(sqlite3_cloudsync_init(db, NULL, NULL), SQLITE_OK);
    static uint8_t blob[70000];
    int mism = 0, dmis = 0, total = 0;
    for (int iter = 0; iter < 20000; iter++) {
        int n = 1 + (int)(rnd() % 4); crdt_value v[4]; char sql[64] = "SELECT cloudsync_pk_encode(";
        for (int i = 0; i < n; i++) strcat(sql, i ? ",?" : "?");
        strcat(sql, ")");
        sqlite3_stmt *st; CHECK_RC(sqlite3_prepare_v2(db, sql, -1, &st, NULL), SQLITE_OK);
        for (int i = 0; i < n; i++) {
            switch (rnd() % 5) {
                case 0: v[i] = (crdt_value){ CRDT_INTEGER, gen_int(), 0, NULL, 0 }; sqlite3_bind_int64(st, i + 1, v[i].i); break;
                case 1: v[i] = (crdt_value){ CRDT_FLOAT, 0, gen_float(), NULL, 0 }; sqlite3_bind_double(st, i + 1, v[i].d); break;
                case 2: case 3: {
                    size_t len = rnd() % 8 == 0 ? (size_t)(rnd() % 70000) : (size_t)(rnd() % 40);
                    for (size_t k = 0; k < len; k++) blob[k] = (uint8_t)(rnd() % 7 == 0 ? rnd() : 'a' + rnd() % 26);
                    bool text = rnd() % 2; uint8_t *copy = malloc(len + 1); memcpy(copy, blob, len); copy[len] = 0;
                    v[i] = (crdt_value){ text ? CRDT_TEXT : CRDT_BLOB, 0, 0, copy, len };
                    if (text) { copy[len] = 0; sqlite3_bind_text(st, i + 1, (const char *)copy, (int)len, SQLITE_STATIC); } else sqlite3_bind_blob(st, i + 1, copy, (int)len, SQLITE_STATIC);
                    break;
                }
                default: v[i] = (crdt_value){ CRDT_NULL, 0, 0, NULL, 0 }; sqlite3_bind_null(st, i + 1);
            }
        }
        bool has_null = false; for (int i = 0; i < n; i++) if (v[i].type == CRDT_NULL) has_null = true;
        int rc = sqlite3_step(st);
        const void *ob = rc == SQLITE_ROW ? sqlite3_column_blob(st, 0) : NULL; int on = rc == SQLITE_ROW ? sqlite3_column_bytes(st, 0) : 0;
        size_t need = crdt_pk_encode(v, n, NULL, 0); uint8_t *mine = malloc(need); size_t got = crdt_pk_encode(v, n, mine, need);
        total++;
        if (has_null) { if (ob) mism++; }                                                  // (sqlite-sync refuses a NULL in a primary key: returns NULL)
        else {
            if (!ob || (size_t)on != got || memcmp(ob, mine, got) != 0) { mism++; if (mism < 5) printf("  mismatch: iteration %d, %d values, sqlite-sync %d bytes, ours %zu\n", iter, n, on, got); }
            crdt_value back[4]; int bn = crdt_pk_decode(ob, (size_t)on, back, 4); bool ok = bn == n;
            for (int i = 0; ok && i < n; i++) {
                if (back[i].type != v[i].type) ok = false;
                else if (v[i].type == CRDT_INTEGER) ok = back[i].i == v[i].i;
                else if (v[i].type == CRDT_FLOAT) ok = memcmp(&back[i].d, &v[i].d, 8) == 0 || (back[i].d == v[i].d);
                else if (v[i].type != CRDT_NULL) ok = back[i].n == v[i].n && (v[i].n == 0 || memcmp(back[i].p, v[i].p, v[i].n) == 0);
            }
            if (!ok) { dmis++; if (dmis < 5) printf("  decode mismatch: iteration %d\n", iter); }
        }
        sqlite3_finalize(st); free(mine);
        for (int i = 0; i < n; i++) if (v[i].type == CRDT_TEXT || v[i].type == CRDT_BLOB) free((void *)v[i].p);
    }
    printf("pk encoding: %d random keys, %d encode mismatches, %d decode mismatches\n", total, mism, dmis);
    CHECK(mism == 0 && dmis == 0);
    sqlite3_close(db);
    MW_DONE();
}
