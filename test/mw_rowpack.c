// The packed form of a row in mw_rows: round trips with the extreme values, and refusal of blobs that are not one.
#include "mw_test.h"
#include "multiwriter_meta.h"
#include "crdt/crdt.h"

static uint64_t rng = 88172645463325252ull;
static uint64_t rnd (void) { rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17; return rng; }

int main (void) {
    const int64_t big[] = { 0, 1, 127, 128, 16383, 16384, INT32_MAX, (int64_t)UINT32_MAX, INT64_MAX };
    const uint32_t cols[] = { 0, 1, 2, 63, 64, 1000000, 0xFFFFFFFEu, CRDT_COL_SENTINEL };
    int rounds = 0;
    for (int it = 0; it < 20000; it++) {
        int n = (int)(rnd() % 40); mw_mcell c[40];
        for (int i = 0; i < n; i++) {
            c[i].col = (rnd() & 3) ? cols[rnd() % 8] : (uint32_t)rnd();
            c[i].cv = (rnd() & 3) ? big[rnd() % 9] : (int64_t)(rnd() >> 1);
            c[i].dv = (rnd() & 3) ? big[rnd() % 9] : (int64_t)(rnd() >> 1);
            c[i].site = (rnd() & 3) ? (uint32_t)(rnd() % 5) : (uint32_t)rnd();
            c[i].seq = (rnd() & 3) ? (uint32_t)(rnd() % 100) : (uint32_t)rnd();
        }
        size_t len; int64_t rdv; uint8_t *b = mw_meta_row_pack(c, n, &len, &rdv); CHECK(b != NULL);
        mw_mcell *o; int on; CHECK(mw_meta_row_cells(b, len, rdv, &o, &on)); CHECK(on == n);
        for (int i = 0; i < n && i < on; i++) CHECK(!memcmp(&o[i], &c[i], sizeof c[i]) || (o[i].col == c[i].col && o[i].cv == c[i].cv && o[i].dv == c[i].dv && o[i].site == c[i].site && o[i].seq == c[i].seq));
        free(o);
        // every truncation of it is refused (never read past the end, never accepted as a shorter row)
        if (n) for (size_t cut = 0; cut < len; cut += 1 + len / 7) { mw_mcell *x; int xn; bool ok = mw_meta_row_cells(b, cut, rdv, &x, &xn); CHECK(!ok); free(x); }
        free(b); rounds++;
    }
    // rows written by one commit: runs of cells with the next column, the same version/db_version/site and the next sequence number are stored as a run
    for (int len = 1; len <= 40; len++) for (int variant = 0; variant < 4; variant++) {
        mw_mcell c[48]; int n = 0;
        if (variant == 1) { c[n++] = (mw_mcell){ 3, 77, 9, 2, 5 }; }                                    // something else before the run
        for (int i = 0; i < len; i++) c[n] = (mw_mcell){ 1, 100, (uint32_t)(i + (variant == 1 ? 1 : 0) + 1), 1, (uint32_t)(10 + i) }, n++;
        if (variant == 2) c[n++] = (mw_mcell){ 1, 100, 99, 1, 5 };                                       // and after it
        if (variant == 3 && n > 2) c[n / 2].seq += 7;                                                    // a break in the middle
        size_t blen; int64_t rdv; uint8_t *b = mw_meta_row_pack(c, n, &blen, &rdv); CHECK(b != NULL);
        mw_mcell *o; int on; CHECK(mw_meta_row_cells(b, blen, rdv, &o, &on)); CHECK(on == n);
        for (int i = 0; i < n && i < on; i++) CHECK(o[i].col == c[i].col && o[i].cv == c[i].cv && o[i].dv == c[i].dv && o[i].site == c[i].site && o[i].seq == c[i].seq);
        if (len >= 17 && variant == 0) CHECK(blen < 16);                                                  // 17 cells of a row: the first and a run
        free(o);
        for (size_t cut = 0; cut < blen; cut++) { mw_mcell *x; int xn; CHECK(!mw_meta_row_cells(b, cut, rdv, &x, &xn)); free(x); }
        if (blen > 6) { b[blen - 1] = 0x3f; mw_mcell *x; int xn; (void)mw_meta_row_cells(b, blen, rdv, &x, &xn); free(x); }          // (damage: never read past the end)
        free(b); rounds++;
    }
    { mw_mcell *x; int xn; uint8_t junk[] = { 9, 1, 0, 0, 0, 0, 0 }; CHECK(!mw_meta_row_cells(junk, sizeof junk, 0, &x, &xn)); uint8_t huge[] = { 1, 0xff, 0xff, 0xff, 0xff, 0x0f }; CHECK(!mw_meta_row_cells(huge, sizeof huge, 0, &x, &xn)); }
    printf("%d random rows packed and unpacked (extreme versions, columns and sites), truncations refused\n", rounds);
    MW_DONE();
}
