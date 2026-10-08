// The shortcut of the read validation (mw_interior_routes_same): a page that a transaction only read and that somebody changed is still valid if it is an interior page of a TABLE b-tree that routes the children
// the transaction went through as before. It must never be used for an interior page of an index: its cells are entries (the rows of a WITHOUT ROWID table), and what a transaction read from one of them is not
// covered by the routes of the children. Measured with mw_serial on a WITHOUT ROWID table, in the processes mode: stale reads accepted. The pages are built by hand here.
#include "mw_test.h"
#include "multiwriter_internal.h"

enum { PG = 4096 };
static void wr32 (uint8_t *p, uint32_t v) { p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16); p[2] = (uint8_t)(v >> 8); p[3] = (uint8_t)v; }
// An interior page with n cells (child i, key i) and a right-most child. table: 0x05 (the key is a rowid varint), else 0x02 (the key is a payload of 8 bytes: `salt` changes the one of cell `salted`).
static void make (uint8_t *pg, bool table, int n, uint32_t child0, int salted, uint8_t salt) {
    memset(pg, 0, PG);
    pg[0] = table ? 0x05 : 0x02; pg[3] = (uint8_t)(n >> 8); pg[4] = (uint8_t)n;
    int pos = PG;
    for (int i = 0; i < n; i++) {
        uint8_t cell[32]; int len = 0;
        wr32(cell, child0 + (uint32_t)i); len = 4;
        if (table) cell[len++] = (uint8_t)(10 * (i + 1));                 // rowid 10, 20, ...
        else { cell[len++] = 8; for (int b = 0; b < 8; b++) cell[len++] = (uint8_t)(i * 16 + b + (i == salted ? salt : 0)); }
        pos -= len; memcpy(pg + pos, cell, (size_t)len);
        pg[12 + 2 * i] = (uint8_t)(pos >> 8); pg[13 + 2 * i] = (uint8_t)pos;
    }
    pg[5] = (uint8_t)(pos >> 8); pg[6] = (uint8_t)pos;
    wr32(pg + 8, child0 + (uint32_t)n);
}

int main (void) {
    static uint8_t a[PG], b[PG];
    uint32_t used[] = { 101, 102 };                                          // the transaction went through children 101 and 102 (cells 1 and 2 of the page: the bounds are cells 0..2)
    // a table b-tree: the same routes: the read is still valid
    make(a, true, 6, 100, -1, 0); make(b, true, 6, 100, -1, 0);
    CHECK(mw_interior_routes_same(a, b, PG, 0, used, 2));
    // ... a divider that bounds a used child changed: not valid
    make(b, true, 6, 100, -1, 0);
    { int off = (b[12 + 2 * 1] << 8) | b[13 + 2 * 1]; b[off + 4] = 99; }      // the key of cell 1 (the upper bound of child 101, the lower bound of 102)
    CHECK(!mw_interior_routes_same(a, b, PG, 0, used, 2));
    // an index b-tree: nothing changed in the cells that bound the used children, but a cell that they do not bound did (a row of the index, or of a WITHOUT ROWID table, that was read from the page itself)
    make(a, false, 6, 100, -1, 0); make(b, false, 6, 100, 5, 1);
    CHECK(!mw_interior_routes_same(a, b, PG, 0, used, 2));
    make(b, false, 6, 100, -1, 0);                                           // ... and an identical page is not saved either (it is never asked: its version would not be newer)
    CHECK(!mw_interior_routes_same(a, b, PG, 0, used, 2));
    MW_DONE();
}
