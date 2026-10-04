// CPU of the merge of runs alone (no database): 8 runs of rows like the ones of the bulk benchmark - a key of an INTEGER primary key written by 16 writers, each one
// ascending in a range of its own, and the packed cells of a row of 17 columns - merged into parts of 32768 rows, blocks read from and written to memory.
// usage: bench_merge [rows per run] [repeat]
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "multiwriter_runs.h"

#define NRUNS 8
static uint8_t *blk_data[NRUNS + 64][4096]; static size_t blk_len[NRUNS + 64][4096]; static int cur;
static int emit (void *ctx, uint32_t blk, const uint8_t *d, size_t len, const uint8_t **loc, size_t *loclen) { (void)ctx; (void)blk; (void)d; (void)len; static uint8_t l[2] = {1, 1}; *loc = l; *loclen = 2; return 0; }
static int emit_store (void *ctx, uint32_t blk, const uint8_t *d, size_t len, const uint8_t **loc, size_t *loclen) { (void)ctx; blk_data[cur][blk] = malloc(len); memcpy(blk_data[cur][blk], d, len); blk_len[cur][blk] = len; static uint8_t l[2] = {1, 1}; *loc = l; *loclen = 2; return 0; }
static int rd (void *ctx, const rs_run *r, uint32_t blk, uint8_t **data, size_t *len) { (void)ctx; int i = (int)r->id; *data = malloc(blk_len[i][blk]); memcpy(*data, blk_data[i][blk], blk_len[i][blk]); *len = blk_len[i][blk]; return 0; }
static int mb (void *s, uint64_t hint, rs_emit_fn *e, void **ec) { (void)s; (void)hint; *e = emit; *ec = NULL; return 0; }
static int me (void *s, const uint8_t *meta, size_t ml, uint64_t nr, uint32_t nb, int64_t dvm) { (void)s; (void)meta; (void)ml; (void)nr; (void)nb; (void)dvm; return 0; }
static double now (void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return t.tv_sec + t.tv_nsec / 1e9; }

int main (int argc, char **argv) {
    uint64_t per = argc > 1 ? strtoull(argv[1], 0, 10) : 131072; int rep = argc > 2 ? atoi(argv[2]) : 3;
    rs_run *runs[NRUNS]; uint64_t bytes = 0;
    for (int r = 0; r < NRUNS; r++) {
        cur = r; rs_builder *b = rs_builder_new(per, emit_store, NULL);
        // 16 writers, a piece of per/16 consecutive keys each, the piece of run r continues the piece of run r-1
        uint64_t piece = per / 16;
        for (uint64_t t = 0; t < 16; t++) for (uint64_t i = 0; i < piece; i++) {
            uint64_t id = (t << 32) | (r * piece + i); uint8_t pk[9]; pk[0] = 0x0b; for (int q = 0; q < 8; q++) pk[1 + q] = (uint8_t)(id >> (56 - 8 * q));
            uint8_t cells[64]; int n = 0; cells[n++] = 2; cells[n++] = 18; cells[n++] = 0; cells[n++] = 1; cells[n++] = 0x80 | (r & 0x7f); cells[n++] = 0x01; cells[n++] = 0; cells[n++] = 1;
            for (int c = 1; c < 18; c++) cells[n++] = 0x0b;
            for (int q = 0; q < 6; q++) cells[n++] = (uint8_t)(id >> (q * 3));
            rs_key k = { 1, pk, 9 };
            if (rs_builder_add(b, &k, (int64_t)(r * per + t * piece + i + 1), cells, (uint32_t)n)) return 1;
        }
        uint8_t *meta; size_t ml; uint64_t nr; uint32_t nb; int64_t dvm;
        if (rs_builder_finish(b, &meta, &ml, &nr, &nb, &dvm)) return 1;
        runs[r] = rs_run_decode(r, NRUNS - r, 0, nr, nb, dvm, meta, ml); for (uint32_t k = 0; k < nb; k++) bytes += blk_len[r][k]; free(meta);
    }
    printf("%d runs of %llu rows, %.1f bytes a row stored\n", NRUNS, (unsigned long long)per, (double)bytes / (double)(NRUNS * per));
    double best = 1e9;
    for (int i = 0; i < rep; i++) {
        rs_run *in[NRUNS]; for (int r = 0; r < NRUNS; r++) in[r] = runs[NRUNS - 1 - r];                      // newest first (age NRUNS - r is the oldest for r = 0)
        rs_merge_opts o = { rd, NULL, false, NULL, NULL, 32768, mb, me, NULL };
        double t0 = now(); uint64_t rows = 0; if (rs_merge(in, NRUNS, &o, &rows)) { puts("merge failed"); return 1; } double dt = now() - t0;
        if (dt < best) best = dt;
        if (i == rep - 1) printf("merged %llu rows\n", (unsigned long long)rows);
    }
    printf("best %.1f ms = %.0f ns a row (%.2f M rows/s)\n", best * 1e3, best * 1e9 / (double)(NRUNS * per), (double)(NRUNS * per) / best / 1e6);
    return 0;
}
