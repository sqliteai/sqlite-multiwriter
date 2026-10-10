// The size of the database at an epoch is kept in a ring of 1024 slots that a reader looks at without a lock: it reads the epoch of the slot, the size, and the epoch again. A snapshot that is
// exactly one ring behind the writer asks for a slot that is about to be overwritten; the writer stored the size and then the epoch, with the old epoch still in the slot, and the reader
// could take the new size for the old epoch (the size of the file that a transaction of that snapshot sees was then wrong). The size at an epoch here is a function of the epoch, so
// every answer can be checked.
#include <pthread.h>
#include <stdatomic.h>
#include "mw_test.h"
#include "multiwriter_internal.h"

enum { PGSZ = 4096, N = 1500000, RING = 1024 };
static mw_store *g_st;
static _Atomic uint64_t g_cur;
static uint32_t size_of (uint64_t e) { return (uint32_t)(10 + (e * 2654435761u) % 100000); }
static void *writer (void *a) {
    (void)a;
    for (uint64_t e = 1; e <= N; e++) {
        if (mw_store_install_recovered(g_st, e, size_of(e), 0, NULL, NULL) != SQLITE_OK) { mw_failures++; break; }
        atomic_store_explicit(&g_cur, e, memory_order_release);
    }
    return NULL;
}
int main (void) {
    g_st = mw_store_create(PGSZ, 10);
    CHECK(g_st != NULL);
    pthread_t t; pthread_create(&t, NULL, writer, NULL);
    uint64_t bad = 0, n = 0, c;
    while ((c = atomic_load_explicit(&g_cur, memory_order_acquire)) < N) {
        if (c < RING + 2) continue;
        uint64_t snap = c - RING + 1;                       // one ring behind: its slot is the next one that the writer takes
        n++;
        if (mw_store_dbsize(g_st, snap) != size_of(snap)) bad++;
    }
    pthread_join(t, NULL);
    printf("%llu questions, %llu wrong answers\n", (unsigned long long)n, (unsigned long long)bad);
    CHECK(n > 1000);
    CHECK(bad == 0);
    MW_DONE();
}
