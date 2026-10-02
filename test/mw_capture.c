// Change capture (docs/design.md): the row-level changes of every commit, derived from the pages, for every kind of table: INTEGER PRIMARY KEY, TEXT key, composite key, WITHOUT ROWID,
// no key at all, values big enough to spill to overflow pages (in the WITHOUT ROWID case the overflow is inside the key's b-tree), keys that change. Random transactions run against a
// model; after every commit the changes the capture reported must equal the net change the model says was made: same tables, same keys (as sqlite-sync encodes them), same kind, same cells.
#include <stdint.h>
#include <stdbool.h>
#include <time.h>
#include "mw_test.h"
#include "multiwriter.h"
#include "crdt.h"

#define NSLOT 120
#define NTAB 7
typedef struct { bool ex; int64_t x; size_t ylen; uint64_t yhash; } mrow;
static mrow M[NTAB][NSLOT], B[NTAB][NSLOT];            // the model now, and as of the start of the transaction
static const char *TN[NTAB] = { "t1", "t2", "t3", "t4", "t5", "t6", "t7" };
static const char *DDL[NTAB] = {
    "CREATE TABLE t1(id INTEGER PRIMARY KEY, x INTEGER, y TEXT)",
    "CREATE TABLE t2(k TEXT PRIMARY KEY, x INTEGER, y TEXT)",
    "CREATE TABLE t3(a INTEGER, b TEXT, x INTEGER, y TEXT, PRIMARY KEY(a, b))",
    "CREATE TABLE t4(a INTEGER, b TEXT, x INTEGER, y TEXT, PRIMARY KEY(a, b)) WITHOUT ROWID",
    "CREATE TABLE t5(x INTEGER, y TEXT)",
    "CREATE TABLE t6(k TEXT PRIMARY KEY, x INTEGER, y BLOB) WITHOUT ROWID",
    "CREATE TABLE t7(id INTEGER PRIMARY KEY, x INTEGER, y BLOB)" };
static const bool REKEY[NTAB] = { false, true, true, false, false, false, false };       // an UPDATE of the key is one update with the old key (rowid tables with a non-rowid key)
static const bool BIG[NTAB] = { false, false, false, false, false, true, true };

static uint64_t rng = 0x9E3779B97F4A7C15ull;
static uint64_t rnd (void) { rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17; return rng; }

static size_t keyenc (int t, int s, uint8_t *out, size_t cap) {
    crdt_value v[2]; char kb[32]; int n = 1;
    switch (t) {
        case 0: case 6: v[0] = (crdt_value){ CRDT_INTEGER, s + 1, 0, NULL, 0 }; break;
        case 4: v[0] = (crdt_value){ CRDT_INTEGER, s + 1, 0, NULL, 0 }; break;                                       // (no key: the rowid)
        case 1: case 5: snprintf(kb, sizeof kb, "key%03d", s); v[0] = (crdt_value){ CRDT_TEXT, 0, 0, kb, strlen(kb) }; break;
        default: snprintf(kb, sizeof kb, "b%d", s % 10); v[0] = (crdt_value){ CRDT_INTEGER, s / 10, 0, NULL, 0 }; v[1] = (crdt_value){ CRDT_TEXT, 0, 0, kb, strlen(kb) }; n = 2;
    }
    return crdt_pk_encode(v, n, out, cap);
}
static void keysql (int t, int s, char *out, size_t cap) {          // the WHERE / VALUES form of a key
    switch (t) { case 0: case 6: snprintf(out, cap, "id=%d", s + 1); break; case 4: snprintf(out, cap, "rowid=%d", s + 1); break; case 1: case 5: snprintf(out, cap, "k='key%03d'", s); break;
                 default: snprintf(out, cap, "a=%d AND b='b%d'", s / 10, s % 10); }
}
static void fillval (char *buf, size_t len, unsigned seed) { for (size_t i = 0; i < len; i++) buf[i] = (char)('a' + (seed * 2654435761u + (unsigned)i * 40503u) % 26); }
static uint64_t hashbuf (const char *b, size_t n) { uint64_t h = 1469598103934665603ull; for (size_t i = 0; i < n; i++) { h ^= (unsigned char)b[i]; h *= 1099511628211ull; } return h; }

static char *g_got[400]; static int g_ngot, g_calls;
static void hexs (const uint8_t *b, size_t n, char *o) { for (size_t i = 0; i < n; i++) sprintf(o + 2 * i, "%02x", b[i]); o[2 * n] = 0; }
static void sink (void *arg, const mw_capture_row *rows, int n) {
    (void)arg; g_calls++;
    for (int i = 0; i < n && g_ngot < 400; i++) {
        char h1[400] = "-", h2[400] = "-", buf[900];
        if (rows[i].pk) hexs(rows[i].pk, rows[i].pklen, h1);
        if (rows[i].oldpk) hexs(rows[i].oldpk, rows[i].oldpklen, h2);
        snprintf(buf, sizeof buf, "%s|%d|%s|%s|%llu", rows[i].table ? rows[i].table : "?", rows[i].kind, h1, h2, (unsigned long long)rows[i].changed);
        g_got[g_ngot++] = strdup(buf);
    }
}
static int cmp_s (const void *a, const void *b) { return strcmp(*(char *const *)a, *(char *const *)b); }

static char *g_exp[400]; static int g_nexp;
static void expect (int t, int kind, int slot, int oslot, uint64_t mask) {
    uint8_t pk[200], opk[200]; char h1[400], h2[400] = "-", buf[900];
    hexs(pk, keyenc(t, slot, pk, sizeof pk), h1);
    if (oslot >= 0) hexs(opk, keyenc(t, oslot, opk, sizeof opk), h2);
    snprintf(buf, sizeof buf, "%s|%d|%s|%s|%llu", TN[t], kind, h1, h2, (unsigned long long)mask);
    g_exp[g_nexp++] = strdup(buf);
}
static void diff_model (int movedfrom[NTAB], int movedto[NTAB]) {
    g_nexp = 0;
    for (int t = 0; t < NTAB; t++) for (int s = 0; s < NSLOT; s++) {
        mrow *b = &B[t][s], *a = &M[t][s];
        if (REKEY[t] && movedfrom[t] == s) continue;                                         // (handled with its destination)
        if (REKEY[t] && movedto[t] == s) {
            mrow *o = &B[t][movedfrom[t]]; uint64_t m = 0; if (o->x != a->x) m |= 1; if (o->ylen != a->ylen || o->yhash != a->yhash) m |= 2;
            expect(t, 2, s, movedfrom[t], m); continue;
        }
        if (!b->ex && !a->ex) continue;
        if (!b->ex) expect(t, 1, s, -1, 0);
        else if (!a->ex) expect(t, 3, s, -1, 0);
        else { uint64_t m = 0; if (b->x != a->x) m |= 1; if (b->ylen != a->ylen || b->yhash != a->yhash) m |= 2; if (m) expect(t, 2, s, -1, m); }
    }
    qsort(g_exp, (size_t)g_nexp, sizeof *g_exp, cmp_s);
}

int main (void) {
    char path[256]; mw_tmpdb(path, sizeof path, "capture");
    char uri[300]; snprintf(uri, sizeof uri, "file:%s?mw=2&mw_cdc=1", path);
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL"), SQLITE_OK);
    for (int t = 0; t < NTAB; t++) CHECK_RC(mw_exec(s, DDL[t]), SQLITE_OK);
    CHECK_RC(mw_exec(s, "CREATE TABLE t8(id INTEGER PRIMARY KEY, y BLOB, z INTEGER)"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "CREATE INDEX t2_x ON t2(x); CREATE INDEX t4_x ON t4(x); CREATE INDEX t7_x ON t7(x)"), SQLITE_OK);        // (secondary indexes: derived data, not captured)
    sqlite3_close(s);
    sqlite3 *db; CHECK_RC(sqlite3_open_v2(uri, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    mw_capture_sink cs = { sink, NULL }; CHECK_RC(sqlite3_file_control(db, "main", MW_FCNTL_CDC_SINK, &cs), SQLITE_OK);

    static char buf[30000]; char sql[40000], ks[100];
    long commits = 0, bad = 0, ops = 0;
    for (int txn = 0; txn < 3000 && bad < 3; txn++) {
        memcpy(B, M, sizeof M);
        int movedfrom[NTAB], movedto[NTAB]; for (int t = 0; t < NTAB; t++) movedfrom[t] = movedto[t] = -1;
        bool touched[NTAB][NSLOT]; memset(touched, 0, sizeof touched);
        CHECK_RC(mw_exec(db, "BEGIN"), SQLITE_OK);
        int nops = 1 + (int)(rnd() % 6);
        for (int o = 0; o < nops; o++) {
            int t = (int)(rnd() % NTAB), sl = (int)(rnd() % NSLOT), op = (int)(rnd() % 10); mrow *m = &M[t][sl];
            size_t len = rnd() % 100 < 70 ? rnd() % 60 : rnd() % 100 < 90 ? 100 + rnd() % 600 : 0;
            if (BIG[t] && len == 0) len = 5000 + rnd() % 15000;
            if (len == 0) len = 700 + rnd() % 400;
            keysql(t, sl, ks, sizeof ks); ops++;
            if (op <= 3 && !m->ex) {                                                          // insert
                unsigned seed = (unsigned)rnd(); fillval(buf, len, seed); buf[len] = 0; int64_t x = (int64_t)(rnd() % 100000);
                char *cols; switch (t) { case 0: case 6: cols = "id"; break; case 4: cols = "rowid"; break; case 1: case 5: cols = "k"; break; default: cols = "a, b"; }
                char vals[80]; switch (t) { case 0: case 6: snprintf(vals, sizeof vals, "%d", sl + 1); break; case 4: snprintf(vals, sizeof vals, "%d", sl + 1); break; case 1: case 5: snprintf(vals, sizeof vals, "'key%03d'", sl); break; default: snprintf(vals, sizeof vals, "%d, 'b%d'", sl / 10, sl % 10); }
                if (BIG[t]) snprintf(sql, sizeof sql, "INSERT INTO %s(%s, x, y) VALUES(%s, %lld, x'", TN[t], cols, vals, (long long)x); else snprintf(sql, sizeof sql, "INSERT INTO %s(%s, x, y) VALUES(%s, %lld, '", TN[t], cols, vals, (long long)x);
                size_t n0 = strlen(sql);
                if (BIG[t]) { for (size_t i = 0; i < len; i++) sprintf(sql + n0 + 2 * i, "%02x", (unsigned char)buf[i]); n0 += 2 * len; } else { memcpy(sql + n0, buf, len); n0 += len; }
                strcpy(sql + n0, "')");
                CHECK_RC(mw_exec(db, sql), SQLITE_OK); *m = (mrow){ true, x, len, hashbuf(buf, len) }; touched[t][sl] = true;
            } else if (op >= 4 && op <= 7 && m->ex) {                                         // update x, or y
                if (rnd() % 2) { int64_t x = (int64_t)(rnd() % 100000); snprintf(sql, sizeof sql, "UPDATE %s SET x=%lld WHERE %s", TN[t], (long long)x, ks); CHECK_RC(mw_exec(db, sql), SQLITE_OK); m->x = x; }
                else { unsigned seed = (unsigned)rnd(); fillval(buf, len, seed); buf[len] = 0;
                    if (BIG[t]) snprintf(sql, sizeof sql, "UPDATE %s SET y=x'", TN[t]); else snprintf(sql, sizeof sql, "UPDATE %s SET y='", TN[t]);
                    size_t n0 = strlen(sql);
                    if (BIG[t]) { for (size_t i = 0; i < len; i++) sprintf(sql + n0 + 2 * i, "%02x", (unsigned char)buf[i]); n0 += 2 * len; } else { memcpy(sql + n0, buf, len); n0 += len; }
                    snprintf(sql + n0, sizeof sql - n0, "' WHERE %s", ks); CHECK_RC(mw_exec(db, sql), SQLITE_OK); m->ylen = len; m->yhash = hashbuf(buf, len); }
                touched[t][sl] = true;
            } else if (op == 8 && m->ex) { snprintf(sql, sizeof sql, "DELETE FROM %s WHERE %s", TN[t], ks); CHECK_RC(mw_exec(db, sql), SQLITE_OK); m->ex = false; touched[t][sl] = true; }
            else if (op == 9 && m->ex && t != 4 && movedfrom[t] < 0) {                      // the key changes
                int ns = (int)(rnd() % NSLOT); if (M[t][ns].ex || touched[t][ns] || touched[t][sl]) continue;
                char nk[100]; keysql(t, ns, nk, sizeof nk);
                if (t == 3 || t == 2) snprintf(sql, sizeof sql, "UPDATE %s SET a=%d, b='b%d' WHERE %s", TN[t], ns / 10, ns % 10, ks);
                else if (t == 0 || t == 6) snprintf(sql, sizeof sql, "UPDATE %s SET id=%d WHERE %s", TN[t], ns + 1, ks);
                else snprintf(sql, sizeof sql, "UPDATE %s SET k='key%03d' WHERE %s", TN[t], ns, ks);
                if (mw_exec(db, sql) != SQLITE_OK) continue;
                M[t][ns] = *m; m->ex = false; touched[t][sl] = touched[t][ns] = true;
                if (REKEY[t]) { movedfrom[t] = sl; movedto[t] = ns; }
            }
        }
        g_ngot = 0; g_calls = 0;
        CHECK_RC(mw_exec(db, "COMMIT"), SQLITE_OK);
        diff_model(movedfrom, movedto);
        qsort(g_got, (size_t)g_ngot, sizeof *g_got, cmp_s);
        bool ok = g_nexp == g_ngot;
        for (int i = 0; ok && i < g_nexp; i++) if (strcmp(g_exp[i], g_got[i])) ok = false;
        if (g_calls == 0 && g_nexp != 0) ok = false;
        commits += g_calls ? 1 : 0;
        if (!ok) {
            bad++; printf("  txn %d: expected %d changes, captured %d (sink called %d times)\n", txn, g_nexp, g_ngot, g_calls);
            for (int i = 0, sh = 0; i < g_nexp && sh < 6; i++) { bool f = false; for (int j = 0; j < g_ngot; j++) if (!strcmp(g_exp[i], g_got[j])) f = true; if (!f) { printf("    missing : %s\n", g_exp[i]); sh++; } }
            for (int j = 0, sh = 0; j < g_ngot && sh < 6; j++) { bool f = false; for (int i = 0; i < g_nexp; i++) if (!strcmp(g_exp[i], g_got[j])) f = true; if (!f) { printf("    spurious: %s\n", g_got[j]); sh++; } }
        }
        for (int i = 0; i < g_nexp; i++) free(g_exp[i]); for (int i = 0; i < g_ngot; i++) free(g_got[i]);
    }
    printf("capture: %ld operations in %ld commits across 7 table kinds, %ld commits whose captured changes differ from the model\n", ops, commits, bad);
    CHECK(bad == 0);

    // ---- the update that leaves the leaf alone: the tail of a big value rewritten in place changes overflow pages only; and the database has been reopened since the row was written, so
    //      the capture has to find out whose overflow page it is (it scans the tables once)
    CHECK_RC(mw_exec(db, "INSERT INTO t7(id, x, y) VALUES(5000, 1, zeroblob(40000))"), SQLITE_OK);
    CHECK_RC(mw_exec(db, "INSERT INTO t6(k, x, y) VALUES('big', 1, zeroblob(40000))"), SQLITE_OK);
    CHECK_RC(mw_exec(db, "INSERT INTO t8(id, y, z) VALUES(1, zeroblob(40000), 1000)"), SQLITE_OK);
    sqlite3_close(db);
    CHECK_RC(sqlite3_open_v2(uri, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    CHECK_RC(sqlite3_file_control(db, "main", MW_FCNTL_CDC_SINK, &cs), SQLITE_OK);
    g_ngot = 0; g_calls = 0;
    CHECK_RC(mw_exec(db, "UPDATE t7 SET y = substr(y, 1, 39900) || x'0102030405060708090a' || substr(y, 39911) WHERE id=5000"), SQLITE_OK);
    printf("overflow-only update: %d commit(s), %d captured:", g_calls, g_ngot); for (int i = 0; i < g_ngot; i++) printf(" [%s]", g_got[i]); printf("\n");
    CHECK(g_ngot == 1 && strstr(g_got[0], "t7|2|") && g_got[0][strlen(g_got[0]) - 1] == '2' && g_got[0][strlen(g_got[0]) - 2] == '|');           // an update, cell 1 (y) only
    for (int i = 0; i < g_ngot; i++) free(g_got[i]); g_ngot = 0;
    // the column after the big value: its bytes are the last bytes of the record, in the last overflow page; same size, so SQLite overwrites in place and the leaf is not written
    CHECK_RC(mw_exec(db, "UPDATE t8 SET z=1001 WHERE id=1"), SQLITE_OK);
    printf("tail-column update: %d captured:", g_ngot); for (int i = 0; i < g_ngot; i++) printf(" [%s]", g_got[i]); printf("\n");
    CHECK(g_ngot == 1 && strstr(g_got[0], "t8|2|") && g_got[0][strlen(g_got[0]) - 1] == '2');                      // cell 1 (z)
    for (int i = 0; i < g_ngot; i++) free(g_got[i]); g_ngot = 0;
    CHECK_RC(mw_exec(db, "UPDATE t6 SET y = substr(y, 1, 39900) || x'0a0b0c0d' || substr(y, 39905) WHERE k='big'"), SQLITE_OK);      // (WITHOUT ROWID: the overflow belongs to a key b-tree)
    printf("overflow-only update, WITHOUT ROWID: %d captured:", g_ngot); for (int i = 0; i < g_ngot; i++) printf(" [%s]", g_got[i]); printf("\n");
    CHECK(g_ngot == 1 && strstr(g_got[0], "t6|2|"));
    for (int i = 0; i < g_ngot; i++) free(g_got[i]);
    sqlite3_close(db); mw_rmdb(path);
    MW_DONE();
}
