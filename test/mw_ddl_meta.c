// DDL and the metadata: tables created and filled in one transaction, ADD / DROP COLUMN, DROP TABLE, dropped and created again, RENAME, VACUUM. The cells follow the rows (and the
// tables) as documented in docs/design.md.
#include <stdint.h>
#include "mw_test.h"
#include "multiwriter.h"
#include "multiwriter_meta.h"
#include "multiwriter_catalog.h"
#include "crdt.h"

static mw_meta *g_m;
static int64_t cv_of (const char *tab, int64_t id, const char *col) {
    crdt_value v = { CRDT_INTEGER, id, 0, NULL, 0 }; uint8_t pk[32]; size_t pl = crdt_pk_encode(&v, 1, pk, 32);
    mw_mcell *c; int n; mw_meta_row(g_m, mw_name_id(tab), pk, pl, &c, &n);
    uint32_t want = col ? mw_name_id(col) : CRDT_COL_SENTINEL; int64_t r = -1;
    for (int i = 0; i < n; i++) if (c[i].col == want) r = c[i].cv;
    free(c); return r;
}
static int ncells (const char *tab, int64_t id) {
    crdt_value v = { CRDT_INTEGER, id, 0, NULL, 0 }; uint8_t pk[32]; size_t pl = crdt_pk_encode(&v, 1, pk, 32);
    mw_mcell *c; int n; mw_meta_row(g_m, mw_name_id(tab), pk, pl, &c, &n); free(c); return n;
}

int main (void) {
    char path[256]; mw_tmpdb(path, sizeof path, "ddlmeta");
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL"), SQLITE_OK); sqlite3_close(s);
    char uri[300]; snprintf(uri, sizeof uri, "file:%s?mw=2&mw_cdc=1", path);
    sqlite3 *db; CHECK_RC(sqlite3_open_v2(uri, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    CHECK_RC(sqlite3_file_control(db, "main", MW_FCNTL_META, &g_m), SQLITE_OK);

    // 1. a table created and filled in the same transaction
    CHECK_RC(mw_exec(db, "BEGIN; CREATE TABLE a(id INTEGER PRIMARY KEY, x TEXT, y INTEGER); INSERT INTO a VALUES(1,'p',10),(2,'q',20),(3,'r',30); COMMIT"), SQLITE_OK);
    CHECK(cv_of("a", 1, "x") == 1 && cv_of("a", 1, "y") == 1 && cv_of("a", 3, "x") == 1);
    printf("1. created and filled in one transaction: row 1 has x:%lld y:%lld, row 3 x:%lld\n", (long long)cv_of("a", 1, "x"), (long long)cv_of("a", 1, "y"), (long long)cv_of("a", 3, "x"));

    // 2. ADD COLUMN: old rows have no cell for it until they are written
    CHECK_RC(mw_exec(db, "ALTER TABLE a ADD COLUMN z TEXT DEFAULT 'd'"), SQLITE_OK);
    CHECK(cv_of("a", 1, "z") == -1);
    CHECK_RC(mw_exec(db, "UPDATE a SET z='new' WHERE id=1; UPDATE a SET x='pp' WHERE id=2"), SQLITE_OK);
    CHECK(cv_of("a", 1, "z") == 1 && cv_of("a", 1, "x") == 1 && cv_of("a", 2, "x") == 3 && cv_of("a", 3, "z") == -1);       // (a rewritten row gets a cell for the column that is new to its record: its default is not known to the capture)
    CHECK_RC(mw_exec(db, "INSERT INTO a VALUES(4,'s',40,'zz')"), SQLITE_OK);
    CHECK(cv_of("a", 4, "z") == 1);
    printf("2. ADD COLUMN: z appears with the first write (row 1 z:%lld, row 3 untouched z:%lld)\n", (long long)cv_of("a", 1, "z"), (long long)cv_of("a", 3, "z"));

    // 3. VACUUM: nothing changes logically
    int64_t before[4]; for (int i = 0; i < 4; i++) before[i] = cv_of("a", i + 1, "x") * 1000 + cv_of("a", i + 1, "y");
    CHECK_RC(mw_exec(db, "VACUUM"), SQLITE_OK);
    for (int i = 0; i < 4; i++) CHECK(before[i] == cv_of("a", i + 1, "x") * 1000 + cv_of("a", i + 1, "y"));
    CHECK(cv_of("a", 1, NULL) == -1);                                                                  // no sentinel: no row was deleted
    CHECK_RC(mw_exec(db, "UPDATE a SET y=11 WHERE id=1"), SQLITE_OK); CHECK(cv_of("a", 1, "y") == 3);
    printf("3. VACUUM leaves the cells alone, later writes continue from them (row 1 y:%lld)\n", (long long)cv_of("a", 1, "y"));

    // 4. DROP COLUMN: the table is rewritten; the cells of the other columns stay, no deletes appear
    CHECK_RC(mw_exec(db, "ALTER TABLE a DROP COLUMN y"), SQLITE_OK);
    CHECK(cv_of("a", 1, "x") == 1 && cv_of("a", 2, "x") == 3 && cv_of("a", 1, NULL) == -1);
    CHECK_RC(mw_exec(db, "UPDATE a SET x='again' WHERE id=1"), SQLITE_OK); CHECK(cv_of("a", 1, "x") == 3);
    printf("4. DROP COLUMN: other cells intact (row 1 x:%lld)\n", (long long)cv_of("a", 1, "x"));

    // 5. DROP TABLE and create it again with the same name, in one transaction: the table is the same, its rows changed (the net change of the content)
    CHECK_RC(mw_exec(db, "BEGIN; DROP TABLE a; CREATE TABLE a(id INTEGER PRIMARY KEY, x TEXT); INSERT INTO a VALUES(1,'fresh'); COMMIT"), SQLITE_OK);
    CHECK(cv_of("a", 1, "x") == 5 && cv_of("a", 2, NULL) == 2 && cv_of("a", 4, NULL) == 2 && cv_of("a", 2, "x") == -1);
    printf("5. dropped and created again: net change of the rows (row 1 x:%lld, rows 2..4 deleted: sentinel %lld, no cells left: %d)\n", (long long)cv_of("a", 1, "x"), (long long)cv_of("a", 2, NULL), ncells("a", 2) - 1);

    // 6. DROP TABLE
    CHECK_RC(mw_exec(db, "CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO b VALUES(1,'x'),(2,'y')"), SQLITE_OK);
    CHECK(cv_of("b", 2, "v") == 1);
    CHECK_RC(mw_exec(db, "DROP TABLE b"), SQLITE_OK);
    CHECK(ncells("b", 1) == 0 && ncells("b", 2) == 0);
    printf("6. DROP TABLE: cells removed (row 2: %d cells)\n", ncells("b", 2));

    // 7. RENAME: the history of the old name goes (documented limit); the rows are not deleted
    CHECK_RC(mw_exec(db, "ALTER TABLE a RENAME TO a2"), SQLITE_OK);
    CHECK(ncells("a", 1) == 0);
    CHECK_RC(mw_exec(db, "UPDATE a2 SET x='moved' WHERE id=1"), SQLITE_OK);
    CHECK(cv_of("a2", 1, "x") == 1);
    printf("7. RENAME: new name starts clean (row 1 x:%lld)\n", (long long)cv_of("a2", 1, "x"));

    // 8. after a flush and a reopen all of it is the same
    sqlite3_close(db);
    CHECK_RC(sqlite3_open_v2(uri, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    CHECK_RC(sqlite3_file_control(db, "main", MW_FCNTL_META, &g_m), SQLITE_OK);
    CHECK(cv_of("a2", 1, "x") == 1 && ncells("a", 1) == 0 && ncells("b", 1) == 0);
    CHECK(mw_scalar(db, "SELECT count(*) FROM mw_cells") == 1);                                       // a2: row 1's x and nothing else
    printf("8. after reopen: mw_cells holds %lld cells\n", (long long)mw_scalar(db, "SELECT count(*) FROM mw_cells"));
    sqlite3_close(db); mw_rmdb(path);
    MW_DONE();
}
