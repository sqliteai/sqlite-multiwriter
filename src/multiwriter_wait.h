//
//  multiwriter_wait.h
//  cloudsync
//
//  Waiting on a 32-bit word in memory shared between processes, woken by a store + wake from another one (a futex). Polling with nanosleep wakes up 100-400 us late on
//  macOS, which is longer than a commit: admission and lock hand-offs between 1000 processes need the waiter to run when the word changes.
//
//    mw_wait_u32(addr, expected, timeout_us): sleeps while *addr == expected, for at most timeout_us; returns after a wake, a change or the timeout (spurious returns are fine:
//                                             the caller re-checks what it waits for).
//    mw_wake_u32(addr, all):                   wakes one (or all) of the processes sleeping on addr.
//
//  macOS/iOS: __ulock_wait/__ulock_wake (the primitive under pthread and the C++ runtime; the public equivalent, os_sync_wait_on_address, needs macOS 14.4 / iOS 17.4).
//  Linux/Android: futex (not the private variant: the memory is shared between processes). Elsewhere: a short sleep (correct, slower; define MW_NO_FUTEX to force it).
//
#ifndef MULTIWRITER_WAIT_H
#define MULTIWRITER_WAIT_H

#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <time.h>

#if !defined(MW_NO_FUTEX) && defined(__APPLE__)
extern int __ulock_wait (uint32_t operation, void *addr, uint64_t value, uint32_t timeout_us);
extern int __ulock_wake (uint32_t operation, void *addr, uint64_t wake_value);
#define MW_UL_COMPARE_AND_WAIT_SHARED 3
#define MW_ULF_WAKE_ALL               0x00000100
#define MW_ULF_NO_ERRNO               0x01000000
static inline void mw_wait_u32 (_Atomic uint32_t *addr, uint32_t expected, uint32_t timeout_us) {
    if (atomic_load_explicit(addr, memory_order_acquire) != expected) return;
    __ulock_wait(MW_UL_COMPARE_AND_WAIT_SHARED | MW_ULF_NO_ERRNO, (void *)addr, expected, timeout_us ? timeout_us : 1);
}
static inline void mw_wake_u32 (_Atomic uint32_t *addr, bool all) {
    __ulock_wake(MW_UL_COMPARE_AND_WAIT_SHARED | MW_ULF_NO_ERRNO | (all ? MW_ULF_WAKE_ALL : 0), (void *)addr, 0);
}
#elif !defined(MW_NO_FUTEX) && defined(__linux__)
#include <unistd.h>
#include <sys/syscall.h>
#include <linux/futex.h>
static inline void mw_wait_u32 (_Atomic uint32_t *addr, uint32_t expected, uint32_t timeout_us) {
    struct timespec ts = { (time_t)(timeout_us / 1000000u), (long)(timeout_us % 1000000u) * 1000L };
    syscall(SYS_futex, (uint32_t *)addr, FUTEX_WAIT, expected, &ts, NULL, 0);
}
static inline void mw_wake_u32 (_Atomic uint32_t *addr, bool all) {
    syscall(SYS_futex, (uint32_t *)addr, FUTEX_WAKE, all ? 0x7fffffff : 1, NULL, NULL, 0);
}
#else
static inline void mw_wait_u32 (_Atomic uint32_t *addr, uint32_t expected, uint32_t timeout_us) {
    if (atomic_load_explicit(addr, memory_order_acquire) != expected) return;
    struct timespec ts = { 0, (long)(timeout_us < 50 ? timeout_us : 50) * 1000L };
    nanosleep(&ts, NULL);
}
static inline void mw_wake_u32 (_Atomic uint32_t *addr, bool all) { (void)addr; (void)all; }
#endif

#endif
