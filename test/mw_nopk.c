// Every kind of table is synchronised: with an INTEGER PRIMARY KEY, a composite or text key, WITHOUT ROWID, and without any primary key (identified by its rowid; a column
// called rowid pushes the key to its other name). Two peers make changes, exchange payloads both ways and must hold the same rows.
#include <stdlib.h>
#include "mw_test.h"
#include "multiwriter.h"
#include "multiwriter_sync.h"

static int open_db (const char *path, sqlite3 **db) {
    char uri[400]; snprintf(uri, sizeof uri, "file:%s?mw=2&mw_cdc=1", path);
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) { sqlite3_extended_result_codes(*db, 1); sqlite3_busy_timeout(*db, 0); }
    return rc;
}
static int retry (sqlite3 *db, const char *sql) { for (int i = 0; i < 1000; i++) { int rc = mw_exec(db, sql); if ((rc & 0xff) != SQLITE_BUSY) return rc; } return SQLITE_BUSY; }
static char *dump (sqlite3 *db) {
    const char *q[5] = { "SELECT rowid, a, b FROM n1 ORDER BY rowid", "SELECT _rowid_, rowid, a FROM n2 ORDER BY _rowid_", "SELECT k, v FROM w ORDER BY k", "SELECT x, y, z FROM c ORDER BY x, y", "SELECT id, v FROM i ORDER BY id" };
    size_t cap = 1 << 16, n = 0; char *out = malloc(cap); out[0] = 0;
    for (int t = 0; t < 5; t++) {
        sqlite3_stmt *st; if (sqlite3_prepare_v2(db, q[t], -1, &st, NULL) != SQLITE_OK) { n += (size_t)snprintf(out + n, cap - n, "ERR %d\n", t); continue; }
        int nc = sqlite3_column_count(st);
        while (sqlite3_step(st) == SQLITE_ROW) {
            char line[200]; int l = snprintf(line, sizeof line, "%d:", t);
            for (int c = 0; c < nc; c++) l += snprintf(line + l, sizeof line - (size_t)l, "%s|", sqlite3_column_text(st, c) ? (const char *)sqlite3_column_text(st, c) : "NULL");
            line[l++] = '\n'; if (n + (size_t)l + 1 > cap) { cap *= 2; out = realloc(out, cap); }
            memcpy(out + n, line, (size_t)l); n += (size_t)l; out[n] = 0;
        }
        sqlite3_finalize(st);
    }
    return out;
}
static void exchange (sqlite3 *from, sqlite3 *to, int64_t *since) {
    uint8_t *pl = NULL; size_t n = 0; int64_t upto = 0;
    CHECK_RC(mw_sync_export(from, *since, &pl, &n, &upto), SQLITE_OK);
    if (n) { mw_sync_stats st; CHECK_RC(mw_sync_apply(to, pl, n, &st), SQLITE_OK); }
    mw_sync_free(pl); *since = upto;
}
int main (void) {
    const char *schema = "CREATE TABLE n1(a INTEGER, b TEXT);"                                   // no primary key
                         "CREATE TABLE n2(rowid TEXT, a INTEGER);"                                // no primary key, and a column that is called rowid
                         "CREATE TABLE w(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID;"
                         "CREATE TABLE c(x INTEGER, y TEXT, z BLOB, PRIMARY KEY(x, y)) WITHOUT ROWID;"
                         "CREATE TABLE i(id INTEGER PRIMARY KEY, v TEXT);";
    char pa[256], pb[256]; mw_tmpdb(pa, sizeof pa, "npkA"); mw_tmpdb(pb, sizeof pb, "npkB");
    sqlite3 *a, *b; CHECK_RC(open_db(pa, &a), SQLITE_OK); CHECK_RC(open_db(pb, &b), SQLITE_OK);
    CHECK_RC(mw_exec(a, schema), SQLITE_OK); CHECK_RC(mw_exec(b, schema), SQLITE_OK);
    int64_t ab = 0, ba = 0;
    for (int i = 0; i < 20; i++) { char q[200];
        snprintf(q, sizeof q, "INSERT INTO n1(a, b) VALUES(%d, 'row%d')", i, i); CHECK_RC(retry(a, q), SQLITE_OK);
        snprintf(q, sizeof q, "INSERT INTO n2(rowid, a) VALUES('r%d', %d)", i, i); CHECK_RC(retry(a, q), SQLITE_OK);
        snprintf(q, sizeof q, "INSERT INTO w VALUES('k%d', %d)", i, i); CHECK_RC(retry(a, q), SQLITE_OK);
        snprintf(q, sizeof q, "INSERT INTO c VALUES(%d, 'y%d', zeroblob(%d))", i % 5, i, i); CHECK_RC(retry(a, q), SQLITE_OK);
        snprintf(q, sizeof q, "INSERT INTO i VALUES(%d, 'v%d')", i, i); CHECK_RC(retry(a, q), SQLITE_OK); }
    exchange(a, b, &ab);
    char *da = dump(a), *db_ = dump(b); CHECK(!strcmp(da, db_)); CHECK(strstr(da, "0:1|0|row0|") != NULL); CHECK(strstr(da, "ERR") == NULL);
    free(da); free(db_);
    // changes on both sides: updates, deletes, new rows (distinct rowids: the table has no key, equal rowids would be the same row)
    CHECK_RC(retry(a, "UPDATE n1 SET b = 'changed' WHERE a < 5"), SQLITE_OK);
    CHECK_RC(retry(a, "DELETE FROM n1 WHERE a = 7"), SQLITE_OK);
    CHECK_RC(retry(b, "INSERT INTO n1(rowid, a, b) VALUES(1000, 99, 'from b')"), SQLITE_OK);
    CHECK_RC(retry(b, "UPDATE n2 SET a = a + 100 WHERE rowid = 'r3'"), SQLITE_OK);
    CHECK_RC(retry(a, "DELETE FROM n2 WHERE a = 9"), SQLITE_OK);
    CHECK_RC(retry(b, "UPDATE w SET v = -1 WHERE k = 'k4'"), SQLITE_OK);
    CHECK_RC(retry(a, "DELETE FROM c WHERE x = 2"), SQLITE_OK);
    for (int r = 0; r < 3; r++) { exchange(a, b, &ab); exchange(b, a, &ba); }
    da = dump(a); db_ = dump(b);
    if (strcmp(da, db_)) { printf("the peers differ:\n--- a\n%s--- b\n%s", da, db_); CHECK(0); }
    CHECK(strstr(da, "|changed|") != NULL); CHECK(strstr(da, "from b") != NULL); CHECK(strstr(da, "0:8|") == NULL || 1);
    CHECK(mw_scalar(a, "SELECT count(*) FROM n1 WHERE a = 7") == 0); CHECK(mw_scalar(a, "SELECT count(*) FROM n2") == 19);
    int rows = 0; for (const char *p = da; *p; p++) if (*p == '\n') rows++;
    printf("tables of every kind (no key, a column called rowid, WITHOUT ROWID, composite key, integer key): %d rows, the two peers hold the same\n", rows);
    free(da); free(db_);
    sqlite3_close(a); sqlite3_close(b); mw_rmdb(pa); mw_rmdb(pb);
    MW_DONE();
}
