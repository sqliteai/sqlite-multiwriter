// Sorted runs of packed rows: random updates and deletes written as runs, merged at random (parts, dropping deletes at the bottom), every key checked against a model after each step;
// corrupt blocks and metas are refused.
#include "mw_test.h"
#include "multiwriter_runs.h"

static uint64_t rng = 0x9E3779B97F4A7C15ull;
static uint64_t rnd (void) { rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17; return rng; }

// ---- the store of blocks, in memory ----
typedef struct { int64_t run; uint32_t blk; uint8_t *d; size_t len; } sblk;
static sblk *blks; static size_t nblks, capblks;
static void store_put (int64_t run, uint32_t blk, const uint8_t *d, size_t len) {
    if (nblks == capblks) { capblks = capblks ? capblks * 2 : 256; blks = realloc(blks, capblks * sizeof *blks); }
    blks[nblks] = (sblk){ run, blk, malloc(len), len }; memcpy(blks[nblks].d, d, len); nblks++;
}
static void store_drop (int64_t run) { size_t k = 0; for (size_t i = 0; i < nblks; i++) { if (blks[i].run == run) free(blks[i].d); else blks[k++] = blks[i]; } nblks = k; }
static int store_read (void *ctx, const rs_run *r, uint32_t blk, uint8_t **data, size_t *len) {
    (void)ctx; for (size_t i = 0; i < nblks; i++) if (blks[i].run == r->id && blks[i].blk == blk) { *data = malloc(blks[i].len); memcpy(*data, blks[i].d, blks[i].len); *len = blks[i].len; return 0; }
    return -1;
}

// ---- the runs ----
static rs_run *runs[4096]; static int nruns; static int64_t next_id = 1, next_age = 1;
static int64_t cur_run;
static int emit_blk (void *ctx, uint32_t blk, const uint8_t *d, size_t len, const uint8_t **loc, size_t *loclen) { (void)ctx; store_put(cur_run, blk, d, len); static uint8_t lc[3]; lc[0] = (uint8_t)blk; lc[1] = (uint8_t)(cur_run & 0x7f); *loc = lc; *loclen = 2; return 0; }

// ---- the model ----
#define NKEYS 6000
typedef struct { uint32_t tbl; uint8_t pk[24]; uint32_t pklen; int state; int64_t dv; uint8_t cells[60]; uint32_t nc; } mkey;   // state 0 never, 1 live, 2 deleted
static mkey keys[NKEYS];
static int keycmp (const void *a, const void *b) { const mkey *x = a, *y = b; rs_key kx = { x->tbl, x->pk, x->pklen }, ky = { y->tbl, y->pk, y->pklen }; return rs_key_cmp(&kx, &ky); }

static int newest_first (const void *a, const void *b) { rs_run *x = *(rs_run **)a, *y = *(rs_run **)b; return x->age != y->age ? (x->age < y->age ? 1 : -1) : (x->id < y->id ? -1 : 1); }

// the lookup an engine would do: runs newest first, the filter, the block of the fences, the row
static int lookup (const rs_key *k, int64_t *dv, uint8_t *out, uint32_t *nc) {
    for (int i = 0; i < nruns; i++) {
        if (!rs_run_maybe(runs[i], k)) continue;
        int b = rs_run_block_of(runs[i], k); if (b < 0) continue;
        uint8_t *d; size_t l; if (store_read(NULL, runs[i], (uint32_t)b, &d, &l)) return -2;
        rs_blk blk; uint8_t *own; if (!rs_blk_unpack(&blk, d, l, &own)) { free(d); return -3; }
        const uint8_t *c; uint32_t n; int64_t v; int f = rs_blk_find(&blk, k, &v, &c, &n); rs_blk_close(&blk);
        if (f == 1) { *dv = v; *nc = n; if (n) memcpy(out, c, n); free(own); free(d); return n ? 1 : 0; }
        free(own); free(d); if (f < 0) return -4;
    }
    return 0;
}
static void verify (const char *what) {
    int bad = 0;
    for (int i = 0; i < NKEYS && bad < 5; i++) {
        rs_key k = { keys[i].tbl, keys[i].pk, keys[i].pklen }; int64_t dv = -1; uint8_t c[64]; uint32_t nc = 0;
        int r = lookup(&k, &dv, c, &nc);
        if (r < 0) { printf("   %s: key %d: lookup error %d\n", what, i, r); bad++; continue; }
        bool live = keys[i].state == 1;
        if ((r == 1) != live) { printf("   %s: key %d: found=%d, model live=%d (state %d)\n", what, i, r, live, keys[i].state); bad++; continue; }
        if (live && (dv != keys[i].dv || nc != keys[i].nc || memcmp(c, keys[i].cells, nc))) { printf("   %s: key %d: other content\n", what, i); bad++; }
    }
    CHECK(bad == 0);
}

// ---- writing a run from the model (the keys of `sel`, in key order) ----
static void write_run (const int *sel, int n) {
    cur_run = next_id++;
    rs_builder *b = rs_builder_new((uint64_t)n, emit_blk, NULL); CHECK(b != NULL);
    for (int i = 0; i < n; i++) { const mkey *m = &keys[sel[i]]; rs_key k = { m->tbl, m->pk, m->pklen }; CHECK(rs_builder_add(b, &k, m->state == 1 ? m->dv : 0, m->cells, m->state == 1 ? m->nc : 0) == 0); }
    uint8_t *meta; size_t ml; uint64_t nr; uint32_t nb; int64_t dvm;
    CHECK(rs_builder_finish(b, &meta, &ml, &nr, &nb, &dvm) == 0);
    rs_run *r = rs_run_decode(cur_run, next_age++, 0, nr, nb, dvm, meta, ml); CHECK(r != NULL); free(meta);
    runs[nruns++] = r; qsort(runs, (size_t)nruns, sizeof *runs, newest_first);
}

// ---- merging ----
static int64_t m_age; static int m_part_ids[256]; static int m_nparts;
static int m_begin (void *s, uint64_t hint, rs_emit_fn *emit, void **ec) { (void)s; (void)hint; cur_run = next_id++; *emit = emit_blk; *ec = NULL; return 0; }
static rs_run *new_parts[4096]; static int nnew;
static int m_end (void *s, const uint8_t *meta, size_t ml, uint64_t nr, uint32_t nb, int64_t dvm) {
    (void)s; rs_run *r = rs_run_decode(cur_run, m_age, 1, nr, nb, dvm, meta, ml); if (!r) return -1; new_parts[nnew++] = r; return 0;
}
static void merge_some (void) {
    if (nruns < 2) return;
    // age groups, newest first
    int64_t ages[4096]; int na = 0; for (int i = 0; i < nruns; i++) if (!na || ages[na - 1] != runs[i]->age) ages[na++] = runs[i]->age;
    if (na < 2) return;
    int a = (int)(rnd() % (uint64_t)na), z = a + 1 + (int)(rnd() % (uint64_t)(na - a)); if (z > na) z = na;       // groups [a, z)
    rs_run *in[4096]; int nin = 0; for (int i = 0; i < nruns; i++) { int g = 0; while (ages[g] != runs[i]->age) g++; if (g >= a && g < z) in[nin++] = runs[i]; }
    m_age = in[0]->age; nnew = 0;
    rs_merge_opts o = { store_read, NULL, z == na, NULL, NULL, (rnd() & 1) ? 0 : 30 + rnd() % 700, m_begin, m_end, NULL };
    uint64_t rows = 0; CHECK(rs_merge(in, nin, &o, &rows) == 0);
    // swap
    int k = 0; for (int i = 0; i < nruns; i++) { bool gone = false; for (int j = 0; j < nin; j++) if (runs[i] == in[j]) gone = true; if (gone) { store_drop(runs[i]->id); rs_run_unref(runs[i]); } else runs[k++] = runs[i]; }
    nruns = k; for (int i = 0; i < nnew; i++) runs[nruns++] = new_parts[i];
    qsort(runs, (size_t)nruns, sizeof *runs, newest_first);
    (void)m_part_ids; (void)m_nparts;
}

int main (void) {
    // a block of the first form (offsets of every row, whole keys) is still read
    { uint8_t blk[200]; size_t w = 0; blk[w++] = 1; uint32_t n = 2; memcpy(blk + w, &n, 4); w += 4;
      size_t offs_at = w; w += 8;
      uint32_t o0 = (uint32_t)w; blk[w++] = 1; blk[w++] = 2; blk[w++] = 'a'; blk[w++] = 'b'; blk[w++] = 7; blk[w++] = 3; blk[w++] = 9; blk[w++] = 9; blk[w++] = 9;
      uint32_t o1 = (uint32_t)w; blk[w++] = 1; blk[w++] = 2; blk[w++] = 'a'; blk[w++] = 'c'; blk[w++] = 8; blk[w++] = 0;
      memcpy(blk + offs_at, &o0, 4); memcpy(blk + offs_at + 4, &o1, 4);
      rs_blk b; uint8_t *own; CHECK(rs_blk_unpack(&b, blk, w, &own)); CHECK(b.nrows == 2);
      rs_key k; int64_t dv; const uint8_t *c; uint32_t nc;
      CHECK(rs_blk_row(&b, 1, &k, &dv, &c, &nc) && k.pklen == 2 && k.pk[1] == 'c' && dv == 8 && nc == 0);
      CHECK(rs_blk_row(&b, 0, &k, &dv, &c, &nc) && dv == 7 && nc == 3);
      rs_key q = { 1, (const uint8_t *)"ac", 2 }; CHECK(rs_blk_find(&b, &q, &dv, &c, &nc) == 1 && dv == 8);
      rs_blk_close(&b); }

    // the universe of keys: three tables; integer-like keys of 8 bytes and text keys of varying length (including empty ones and ones that are prefixes of each other)
    for (int i = 0; i < NKEYS; i++) {
        mkey *m = &keys[i]; m->tbl = 1 + (uint32_t)(i % 3);
        if (i % 4 == 3) { m->pklen = (uint32_t)(rnd() % 20); for (uint32_t j = 0; j < m->pklen; j++) m->pk[j] = (uint8_t)('a' + rnd() % 3); }
        else { m->pklen = 8; uint64_t v = rnd() % 100000; for (int j = 0; j < 8; j++) m->pk[j] = (uint8_t)(v >> (56 - 8 * j)); }
    }
    qsort(keys, NKEYS, sizeof *keys, keycmp);
    int u = 0; for (int i = 0; i < NKEYS; i++) if (!i || keycmp(&keys[i], &keys[u - 1])) keys[u++] = keys[i];     // (no duplicates)
    for (int i = u; i < NKEYS; i++) { keys[i].tbl = 9; keys[i].pklen = 8; uint64_t v = (uint64_t)i; for (int j = 0; j < 8; j++) keys[i].pk[j] = (uint8_t)(v >> (56 - 8 * j)); }
    qsort(keys, NKEYS, sizeof *keys, keycmp);
    int64_t dv = 1; int steps = 0;
    for (int round = 0; round < 400; round++) {
        int sel[NKEYS], n = 0; int dens = 1 + (int)(rnd() % 60);
        for (int i = 0; i < NKEYS; i++) if (rnd() % 1000 < (uint64_t)dens * 3) sel[n++] = i;
        for (int j = 0; j < n; j++) {
            mkey *m = &keys[sel[j]];
            if (m->state == 1 && rnd() % 5 == 0) m->state = 2;
            else { m->state = 1; m->dv = dv++; m->nc = 1 + (uint32_t)(rnd() % 50); for (uint32_t c = 0; c < m->nc; c++) m->cells[c] = (uint8_t)rnd(); }
        }
        if (n) write_run(sel, n);
        if (rnd() % 3 == 0) merge_some();
        if (round % 20 == 19 || round < 5) { char w[32]; snprintf(w, sizeof w, "round %d", round); verify(w); }
        steps++;
    }
    verify("end");
    // everything into one run, the oldest data: no deletes are left
    while (nruns > 1) { int64_t first = runs[0]->age; (void)first; rs_run *in[4096]; int nin = nruns; memcpy(in, runs, sizeof(rs_run *) * (size_t)nruns); m_age = in[0]->age; nnew = 0;
        rs_merge_opts o = { store_read, NULL, true, NULL, NULL, 0, m_begin, m_end, NULL }; uint64_t rows; CHECK(rs_merge(in, nin, &o, &rows) == 0);
        for (int i = 0; i < nruns; i++) { store_drop(runs[i]->id); rs_run_unref(runs[i]); } nruns = 0; for (int i = 0; i < nnew; i++) runs[nruns++] = new_parts[i]; qsort(runs, (size_t)nruns, sizeof *runs, newest_first);
        if (nnew > 1) break; }
    verify("after the full merge");
    int live = 0, tomb = 0; for (int i = 0; i < NKEYS; i++) { if (keys[i].state == 1) live++; if (keys[i].state == 2) tomb++; }
    uint64_t rows_in_runs = 0; for (int i = 0; i < nruns; i++) rows_in_runs += runs[i]->nrows;
    printf("%d rounds of updates and deletes, merges at random: %d runs left with %llu rows (model: %d live, %d deleted)\n", steps, nruns, (unsigned long long)rows_in_runs, live, tomb);
    CHECK(rows_in_runs == (uint64_t)live);

    // refusal of damage: truncated and flipped blocks, truncated metas
    if (nblks) {
        int refused = 0, tried = 0;
        for (int t = 0; t < 3000; t++) {
            sblk *s = &blks[rnd() % nblks]; uint8_t *d = malloc(s->len); memcpy(d, s->d, s->len);
            size_t len = s->len; if (rnd() & 1) len = rnd() % s->len; else d[rnd() % s->len] ^= (uint8_t)(1 + rnd() % 255);
            rs_blk b; uint8_t *own; tried++;
            if (!rs_blk_unpack(&b, d, len, &own)) { refused++; free(d); continue; }
            for (uint32_t i = 0; i < b.nrows; i++) { rs_key k; int64_t v; const uint8_t *c; uint32_t nc; (void)rs_blk_row(&b, i, &k, &v, &c, &nc); }       // (never reads outside the block: ASan checks)
            rs_key probe = { 1, (const uint8_t *)"zz", 2 }; int64_t v; const uint8_t *c; uint32_t nc; (void)rs_blk_find(&b, &probe, &v, &c, &nc); rs_blk_close(&b);
            free(own); free(d);
        }
        printf("%d damaged blocks: %d refused at once, the others read without leaving their bytes\n", tried, refused);
    }
    {
        // a meta cut short at any place is refused or decodes to something that does not read outside itself
        cur_run = next_id++; rs_builder *b = rs_builder_new(10, emit_blk, NULL);
        for (uint8_t i = 0; i < 10; i++) { rs_key k = { 1, &i, 1 }; rs_builder_add(b, &k, 5, (const uint8_t *)"xy", 2); }
        uint8_t *meta; size_t ml; uint64_t nr; uint32_t nb; int64_t dvm; CHECK(rs_builder_finish(b, &meta, &ml, &nr, &nb, &dvm) == 0);
        int ok = 0; for (size_t cut = 0; cut < ml; cut++) { rs_run *r = rs_run_decode(1, 1, 0, nr, nb, dvm, meta, cut); if (r) { ok++; rs_run_unref(r); } }
        CHECK(ok == 0); rs_run *full = rs_run_decode(1, 1, 0, nr, nb, dvm, meta, ml); CHECK(full != NULL); rs_run_unref(full); free(meta);
    }
    MW_DONE();
}
