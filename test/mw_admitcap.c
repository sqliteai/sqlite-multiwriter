// admit_cap() caches the number of CPUs in a static: concurrent first calls (the first commits of several threads) wrote it with no synchronisation, a data race (the value is the same, but it
// is undefined behaviour in C, and ThreadSanitizer reports it). The cache is atomic now. Only a run under ThreadSanitizer (test/sanitize.sh tsan mw_admitcap) can see the race: in any other
// build this checks that the limit is in range for every thread.
// The test includes multiwriter_pages.c with sysconf() replaced by one that waits until all the threads are in it, so that their first calls overlap.
#include <pthread.h>
#include <unistd.h>
#include "mw_test.h"

#define WORKERS 8
static pthread_mutex_t gate = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t ready = PTHREAD_COND_INITIALIZER;
static int arrived;
static long concurrent_sysconf (int name) {
    pthread_mutex_lock(&gate);
    if (++arrived == WORKERS) pthread_cond_broadcast(&ready);
    while (arrived < WORKERS) pthread_cond_wait(&ready, &gate);
    pthread_mutex_unlock(&gate);
    return sysconf(name);
}
#define sysconf concurrent_sysconf
#include "multiwriter_pages.c"
#undef sysconf

static void *worker (void *arg) { *(int *)arg = admit_cap(); return NULL; }

int main (void) {
    unsetenv("MW_ADMIT");
    pthread_t th[WORKERS]; int cap[WORKERS];
    for (int i = 0; i < WORKERS; i++) CHECK(pthread_create(&th[i], NULL, worker, &cap[i]) == 0);
    for (int i = 0; i < WORKERS; i++) { pthread_join(th[i], NULL); CHECK(cap[i] >= 4 && cap[i] <= 16); }
    printf("test/mw_admitcap.c: %d failure(s)\n", mw_failures);
    return mw_failures ? 1 : 0;
}
