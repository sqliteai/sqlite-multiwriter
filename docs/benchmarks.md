# Benchmarks

Measured on one Mac with 18 cores, SQLite 3.53.4, `synchronous=FULL` (every commit is on disk when it returns), 2026-10-07. One run of 8 seconds per number, so differences of a few percent are noise.
The numbers are in `bench/results/simple_*_2026-10-07.jsonl`; `bench/make_report.py` makes this page from them.

**How to read the tables**

- **Writers**: threads of one process (first tables) or separate processes (second tables) that all write the same database at once.
- **tx/s**: transactions committed per second, all writers together. More is better.
- **SQLite**: stock SQLite in WAL mode, one writer at a time; a writer that finds the database busy gives up and the application runs the transaction again.
- **Engine**: this project, several writers at once; a commit that conflicts with a commit made meanwhile is refused and the application runs it again.
- **Engine with merge** (`mw_rebase=1`): a commit that conflicts only because it shares a page with another commit, not a row, is not refused: the engine replays its row changes on top of the latest state and commits it.
- **Retries per 100 tx**: how many times, for every 100 committed transactions, the application had to run a transaction again after being refused (`SQLITE_BUSY`). 0 means the application never noticed a conflict.
- **Commits merged**: the share of committed transactions that the engine saved by merging, instead of refusing them.
- **Lost replays per merged commit**: how many times, on average, the engine had to replay a merge again because another commit got in first. It costs time, not retries for the application.
- "vs SQLite": the number of times more transactions per second than SQLite. A `*` after a number: some transactions gave up after 1000 retries.

## Each writer inserts its own rows (100 rows per transaction)

No two writers touch the same row or page, apart from the growth of the file. The common case of many writers.

**Threads of one process**

| Threads | SQLite tx/s | SQLite retries per 100 tx | Engine tx/s | vs SQLite | Engine retries per 100 tx | Engine **with merge** tx/s | vs SQLite | retries per 100 tx | commits merged | lost replays per merged commit |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 13,099 | 0 | 14,877 | 1.1× | 0 | 14,780 | 1.1× | 0 | 0 | - |
| 4 | 9,049 | 107 | 30,984 | 3.4× | 0.0 | 31,094 | 3.4× | 0.0 | 0% | 0.4 |
| 16 | 8,539 | 160 | 46,449 | 5.4× | 0.1 | 46,113 | 5.4× | 0.1 | 0% | 0.3 |
| 64 | 7,890 | 349 | 40,336 | 5.1× | 0.8 | 40,770 | 5.2× | 0.5 | 0% | 0.4 |

**Processes**

| Processes | SQLite tx/s | SQLite retries per 100 tx | Engine tx/s | vs SQLite | Engine retries per 100 tx | Engine **with merge** tx/s | vs SQLite | retries per 100 tx | commits merged | lost replays per merged commit |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 13,122 | 0 | 14,167 | 1.1× | 0 | 14,208 | 1.1× | 0 | 0 | - |
| 4 | 9,051 | 103 | 27,626 | 3.1× | 0.0 | 28,180 | 3.1× | 0.0 | 0% | 0.4 |
| 16 | 8,419 | 147 | 24,257 | 2.9× | 0.1 | 24,454 | 2.9× | 0.1 | 0% | 1.1 |

## Each writer updates its own row; the rows of four groups of writers share a page each

Writers never touch the same **row**, but they do share **pages**: SQLite refuses the second one, the engine can merge them.

**Threads of one process**

| Threads | SQLite tx/s | SQLite retries per 100 tx | Engine tx/s | vs SQLite | Engine retries per 100 tx | Engine **with merge** tx/s | vs SQLite | retries per 100 tx | commits merged | lost replays per merged commit |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 17,683 | 0 | 33,010 | 1.9× | 0 | 33,167 | 1.9× | 0 | 0 | - |
| 4 | 11,128 | 4.4 | 39,510 | 3.6× | 0.5 | 29,104 | 2.6× | 0 | 20% | 1.0 |
| 16 | 10,376 | 25 | 32,977* | 3.2× | 6.2 | 51,061 | 4.9× | 0 | 55% | 0.2 |
| 64 | 10,884 | 106 | 39,871 | 3.7× | 53 | 45,244 | 4.2× | 0 | 65% | 0.0 |

**Processes**

| Processes | SQLite tx/s | SQLite retries per 100 tx | Engine tx/s | vs SQLite | Engine retries per 100 tx | Engine **with merge** tx/s | vs SQLite | retries per 100 tx | commits merged | lost replays per merged commit |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 13,976 | 0 | 30,848 | 2.2× | 0 | 30,752 | 2.2× | 0 | 0 | - |
| 4 | 13,738 | 4.4 | 45,304 | 3.3× | 16 | 46,301 | 3.4× | 0 | 16% | 0.0 |
| 16 | 12,722 | 21 | 43,487 | 3.4× | 85 | 47,184 | 3.7× | 0 | 15% | 0.6 |

## Each writer updates its own row; all the rows are on one single page

The same, in the worst case for page-level conflicts: one page for everybody.

**Threads of one process**

| Threads | SQLite tx/s | SQLite retries per 100 tx | Engine tx/s | vs SQLite | Engine retries per 100 tx | Engine **with merge** tx/s | vs SQLite | retries per 100 tx | commits merged | lost replays per merged commit |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 12,250 | 0 | 31,366 | 2.6× | 0 | 30,475 | 2.5× | 0 | 0 | - |
| 4 | 12,963 | 4.1 | 24,693 | 1.9× | 3.0 | 35,383 | 2.7× | 0 | 48% | 0.6 |
| 16 | 17,229 | 15 | 33,099 | 1.9× | 15 | 61,974 | 3.6× | 0 | 68% | 0.2 |
| 64 | 11,682 | 88 | 31,589 | 2.7× | 79 | 37,622 | 3.2× | 0 | 71% | 0.1 |

**Processes**

| Processes | SQLite tx/s | SQLite retries per 100 tx | Engine tx/s | vs SQLite | Engine retries per 100 tx | Engine **with merge** tx/s | vs SQLite | retries per 100 tx | commits merged | lost replays per merged commit |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 13,426 | 0 | 32,573 | 2.4× | 0 | 32,594 | 2.4× | 0 | 0 | - |
| 4 | 13,404 | 3.9 | 40,056 | 3.0× | 56 | 39,342 | 2.9× | 0 | 31% | 0.9 |
| 16 | 12,688 | 20 | 42,604 | 3.4× | 96 | 35,089 | 2.8× | 0 | 24% | 0.8 |

## Everybody updates the same 4 rows (`a = a + 1`)

A **real** conflict: two writers change the same row. No engine can merge that: one of them must run again.

**Threads of one process**

| Threads | SQLite tx/s | SQLite retries per 100 tx | Engine tx/s | vs SQLite | Engine retries per 100 tx | Engine **with merge** tx/s | vs SQLite | retries per 100 tx | commits merged | lost replays per merged commit |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 13,624 | 0 | 29,799 | 2.2× | 0 | 33,795 | 2.5× | 0 | 0 | - |
| 4 | 13,642 | 3.6 | 29,247 | 2.1× | 3.0 | 20,524 | 1.5× | 8.1 | 4% | 1.3 |
| 16 | 14,684 | 18 | 26,849 | 1.8× | 16 | 28,201 | 1.9× | 30 | 10% | 1.3 |
| 64 | 13,812 | 83 | 28,126 | 2.0× | 79 | 22,484 | 1.6× | 125 | 24% | 1.3 |

**Processes**

| Processes | SQLite tx/s | SQLite retries per 100 tx | Engine tx/s | vs SQLite | Engine retries per 100 tx | Engine **with merge** tx/s | vs SQLite | retries per 100 tx | commits merged | lost replays per merged commit |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 14,233 | 0 | 31,715 | 2.2× | 0 | 32,707 | 2.3× | 0 | 0 | - |
| 4 | 14,062 | 3.8 | 40,909 | 2.9× | 54 | 41,709 | 3.0× | 39 | 14% | 0.3 |
| 16 | 12,716 | 20 | 41,619 | 3.3× | 95 | 43,381 | 3.4× | 87 | 10% | 0.7 |

## Writers update different columns of the same rows

Different columns of rows spread over the table. When the rows coincide it is a real conflict for the merge too (it compares whole rows). Threads only.

**Threads of one process**

| Threads | SQLite tx/s | SQLite retries per 100 tx | Engine tx/s | vs SQLite | Engine retries per 100 tx | Engine **with merge** tx/s | vs SQLite | retries per 100 tx | commits merged | lost replays per merged commit |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 13,657 | 0 | 34,691 | 2.5× | 0 | 25,698 | 1.9× | 0 | 0 | - |
| 4 | 12,357 | 3.5 | 24,790 | 2.0× | 2.8 | 19,576 | 1.6× | 2.8 | 0 | - |
| 16 | 12,200 | 20 | 31,669 | 2.6× | 15 | 33,336 | 2.7× | 13 | 48% | 0.6 |
| 64 | 13,409 | 80 | 28,772 | 2.1× | 80 | 33,787 | 2.5× | 55 | 64% | 0.2 |

