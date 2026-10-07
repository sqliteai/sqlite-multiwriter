# Benchmarks

Measured on one Mac with 18 cores, SQLite 3.53.4, `synchronous=FULL` (every commit is on disk when it returns), 2026-10-07. One run of 8 seconds per number, so differences of a few percent are noise.
The numbers are in `bench/results/simple_*_2026-10-07.jsonl`; `bench/make_report.py` makes this page from them. Part 1 is for threads of one process, part 2 for separate processes; each has the same scenarios and the comparison with SQLite.

**How to read the tables**

- **Writers**: threads of one process (part 1) or separate processes (part 2) that all write the same database at once.
- **tx/s**: transactions committed per second, all writers together. More is better.
- **Rows written**: rows inserted (first scenario) or updated (the others) by the transactions that committed in the 8 seconds of the measured run (2 s of warm-up are not counted).
- **SQLite**: stock SQLite in WAL mode, one writer at a time; a writer that finds the database busy gives up and the application runs the transaction again.
- **Engine**: this project, several writers at once; a commit that conflicts with a commit made meanwhile is refused and the application runs it again.
- **Engine with merge** (`mw_rebase=1`): a commit that conflicts only because it shares a page with another commit, not a row, is not refused: the engine replays its row changes on top of the latest state and commits it.
- **Retries per 100 tx**: how many times, for every 100 committed transactions, the application had to run a transaction again after being refused (`SQLITE_BUSY`). 0 means the application never noticed a conflict.
- **Commits merged**: the share of committed transactions that the engine saved by merging, instead of refusing them.
- **Lost replays per merged commit**: how many times, on average, the engine had to replay a merge again because another commit got in first. It costs time, not retries for the application.
- "vs SQLite": the number of times more transactions per second than SQLite. A `*` after a number: some transactions gave up after 1000 retries.

# Part 1: threads of one process

## Each writer inserts its own rows

100 rows per transaction. No two writers touch the same row or page, apart from the growth of the file. The common case of many writers. Rows inserted per transaction: 100.

| Threads | SQLite tx/s | rows written | retries per 100 tx | Engine tx/s | rows written | vs SQLite | retries per 100 tx | Engine **with merge** tx/s | rows written | vs SQLite | retries per 100 tx | commits merged | lost replays per merged commit |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 13,099 | 10,479,280 | 0 | 14,877 | 11,901,760 | 1.1× | 0 | 14,780 | 11,823,920 | 1.1× | 0 | 0 | - |
| 4 | 9,049 | 7,239,520 | 130 | 30,984 | 24,787,200 | 3.4× | 0.0 | 31,094 | 24,875,040 | 3.4× | 0.0 | 0% | 0.4 |
| 16 | 8,539 | 6,831,520 | 197 | 46,449 | 37,159,040 | 5.4× | 0.2 | 46,113 | 36,890,720 | 5.4× | 0.1 | 0% | 0.3 |
| 64 | 7,890 | 6,312,320 | 430 | 40,336 | 32,268,480 | 5.1× | 1.0 | 40,770 | 32,615,760 | 5.2× | 0.7 | 0% | 0.4 |

## Each writer updates its own row; the rows of four groups of writers share a page each

One row per transaction. Writers never touch the same **row**, but they do share **pages**: SQLite refuses the second one, the engine can merge them. Rows updated per transaction: 1.

| Threads | SQLite tx/s | rows written | retries per 100 tx | Engine tx/s | rows written | vs SQLite | retries per 100 tx | Engine **with merge** tx/s | rows written | vs SQLite | retries per 100 tx | commits merged | lost replays per merged commit |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 17,683 | 141,462 | 0 | 33,010 | 264,080 | 1.9× | 0 | 33,167 | 265,338 | 1.9× | 0 | 0 | - |
| 4 | 11,128 | 89,027 | 5.6 | 39,510 | 316,082 | 3.6× | 0.7 | 29,104 | 232,836 | 2.6× | 0 | 25% | 1.0 |
| 16 | 10,376 | 83,007 | 32 | 32,977* | 263,818 | 3.2× | 8.2 | 51,061 | 408,490 | 4.9× | 0 | 68% | 0.2 |
| 64 | 10,884 | 87,074 | 134 | 39,871 | 318,966 | 3.7× | 65 | 45,244 | 361,952 | 4.2× | 0 | 82% | 0.0 |

## Each writer updates its own row; all the rows are on one single page

One row per transaction. The same, in the worst case for page-level conflicts: one page for everybody. Rows updated per transaction: 1.

| Threads | SQLite tx/s | rows written | retries per 100 tx | Engine tx/s | rows written | vs SQLite | retries per 100 tx | Engine **with merge** tx/s | rows written | vs SQLite | retries per 100 tx | commits merged | lost replays per merged commit |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 12,250 | 98,004 | 0 | 31,366 | 250,930 | 2.6× | 0 | 30,475 | 243,798 | 2.5× | 0 | 0 | - |
| 4 | 12,963 | 103,707 | 5.5 | 24,693 | 197,546 | 1.9× | 4.0 | 35,383 | 283,063 | 2.7× | 0 | 61% | 0.6 |
| 16 | 17,229 | 137,834 | 19 | 33,099 | 264,790 | 1.9× | 19 | 61,974 | 495,796 | 3.6× | 0 | 86% | 0.2 |
| 64 | 11,682 | 93,459 | 118 | 31,589 | 252,712 | 2.7× | 100 | 37,622 | 300,974 | 3.2× | 0 | 89% | 0.1 |

## Everybody updates the same 4 rows (`a = a + 1`)

One row per transaction. A **real** conflict: two writers change the same row. No engine can merge that: one of them must run again. Rows updated per transaction: 1.

| Threads | SQLite tx/s | rows written | retries per 100 tx | Engine tx/s | rows written | vs SQLite | retries per 100 tx | Engine **with merge** tx/s | rows written | vs SQLite | retries per 100 tx | commits merged | lost replays per merged commit |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 13,624 | 108,996 | 0 | 29,799 | 238,392 | 2.2× | 0 | 33,795 | 270,359 | 2.5× | 0 | 0 | - |
| 4 | 13,642 | 109,133 | 5.1 | 29,247 | 233,978 | 2.1× | 3.9 | 20,524 | 164,195 | 1.5× | 11 | 5% | 1.3 |
| 16 | 14,684 | 117,473 | 24 | 26,849 | 214,790 | 1.8× | 21 | 28,201 | 225,605 | 1.9× | 39 | 14% | 1.3 |
| 64 | 13,812 | 110,498 | 109 | 28,126 | 225,004 | 2.0× | 101 | 22,484 | 179,874 | 1.6× | 156 | 29% | 1.3 |

## Writers update different columns of the same rows

One row per transaction. Different columns of rows spread over the table. When the rows coincide it is a real conflict for the merge too (it compares whole rows). Threads only. Rows updated per transaction: 1.

| Threads | SQLite tx/s | rows written | retries per 100 tx | Engine tx/s | rows written | vs SQLite | retries per 100 tx | Engine **with merge** tx/s | rows written | vs SQLite | retries per 100 tx | commits merged | lost replays per merged commit |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 13,657 | 109,253 | 0 | 34,691 | 277,527 | 2.5× | 0 | 25,698 | 205,580 | 1.9× | 0 | 0 | - |
| 4 | 12,357 | 98,858 | 4.8 | 24,790 | 198,317 | 2.0× | 3.8 | 19,576 | 156,608 | 1.6× | 3.7 | 0 | - |
| 16 | 12,200 | 97,602 | 26 | 31,669 | 253,354 | 2.6× | 19 | 33,336 | 266,686 | 2.7× | 17 | 61% | 0.6 |
| 64 | 13,409 | 107,270 | 105 | 28,772 | 230,172 | 2.1× | 102 | 33,787 | 270,298 | 2.5× | 69 | 79% | 0.2 |

# Part 2: separate processes (`mw_mp=1`)

## Each writer inserts its own rows

100 rows per transaction. No two writers touch the same row or page, apart from the growth of the file. The common case of many writers. Rows inserted per transaction: 100.

| Processes | SQLite tx/s | rows written | retries per 100 tx | Engine tx/s | rows written | vs SQLite | retries per 100 tx | Engine **with merge** tx/s | rows written | vs SQLite | retries per 100 tx | commits merged | lost replays per merged commit |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 13,122 | 10,497,760 | 0 | 14,167 | 11,333,840 | 1.1× | 0 | 14,208 | 11,366,000 | 1.1× | 0 | 0 | - |
| 4 | 9,051 | 7,240,960 | 126 | 27,626 | 22,101,040 | 3.1× | 0.0 | 28,180 | 22,543,680 | 3.1× | 0.0 | 0% | 0.4 |
| 16 | 8,419 | 6,735,440 | 180 | 24,257 | 19,405,600 | 2.9× | 0.2 | 24,454 | 19,563,120 | 2.9× | 0.2 | 0% | 1.1 |

## Each writer updates its own row; the rows of four groups of writers share a page each

One row per transaction. Writers never touch the same **row**, but they do share **pages**: SQLite refuses the second one, the engine can merge them. Rows updated per transaction: 1.

| Processes | SQLite tx/s | rows written | retries per 100 tx | Engine tx/s | rows written | vs SQLite | retries per 100 tx | Engine **with merge** tx/s | rows written | vs SQLite | retries per 100 tx | commits merged | lost replays per merged commit |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 13,976 | 111,804 | 0 | 30,848 | 246,786 | 2.2× | 0 | 30,752 | 246,015 | 2.2× | 0 | 0 | - |
| 4 | 13,738 | 109,905 | 5.7 | 45,304 | 362,430 | 3.3× | 20 | 46,301 | 370,410 | 3.4× | 0 | 20% | 0.0 |
| 16 | 12,722 | 101,776 | 28 | 43,487 | 347,898 | 3.4× | 106 | 47,184 | 377,475 | 3.7× | 0 | 19% | 0.6 |

## Each writer updates its own row; all the rows are on one single page

One row per transaction. The same, in the worst case for page-level conflicts: one page for everybody. Rows updated per transaction: 1.

| Processes | SQLite tx/s | rows written | retries per 100 tx | Engine tx/s | rows written | vs SQLite | retries per 100 tx | Engine **with merge** tx/s | rows written | vs SQLite | retries per 100 tx | commits merged | lost replays per merged commit |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 13,426 | 107,410 | 0 | 32,573 | 260,584 | 2.4× | 0 | 32,594 | 260,749 | 2.4× | 0 | 0 | - |
| 4 | 13,404 | 107,236 | 5.1 | 40,056 | 320,446 | 3.0× | 67 | 39,342 | 314,735 | 2.9× | 0 | 39% | 0.9 |
| 16 | 12,688 | 101,507 | 26 | 42,604 | 340,836 | 3.4× | 119 | 35,089 | 280,709 | 2.8× | 0 | 31% | 0.8 |

## Everybody updates the same 4 rows (`a = a + 1`)

One row per transaction. A **real** conflict: two writers change the same row. No engine can merge that: one of them must run again. Rows updated per transaction: 1.

| Processes | SQLite tx/s | rows written | retries per 100 tx | Engine tx/s | rows written | vs SQLite | retries per 100 tx | Engine **with merge** tx/s | rows written | vs SQLite | retries per 100 tx | commits merged | lost replays per merged commit |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 14,233 | 113,867 | 0 | 31,715 | 253,721 | 2.2× | 0 | 32,707 | 261,655 | 2.3× | 0 | 0 | - |
| 4 | 14,062 | 112,493 | 5.0 | 40,909 | 327,274 | 2.9× | 67 | 41,709 | 333,675 | 3.0× | 48 | 17% | 0.3 |
| 16 | 12,716 | 101,731 | 26 | 41,619 | 332,953 | 3.3× | 119 | 43,381 | 347,046 | 3.4× | 108 | 12% | 0.7 |

## Writers update different columns of the same rows

One row per transaction. Different columns of rows spread over the table. When the rows coincide it is a real conflict for the merge too (it compares whole rows). Threads only. Rows updated per transaction: 1.

| Processes | SQLite tx/s | rows written | retries per 100 tx | Engine tx/s | rows written | vs SQLite | retries per 100 tx | Engine **with merge** tx/s | rows written | vs SQLite | retries per 100 tx | commits merged | lost replays per merged commit |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 12,599 | 100,795 | 0 | 32,523 | 260,184 | 2.6× | 0 | 32,077 | 256,618 | 2.5× | 0 | 0 | - |
| 4 | 14,170 | 113,362 | 6.0 | 40,299 | 322,390 | 2.8× | 67 | 38,129 | 305,033 | 2.7× | 66 | 0 | - |
| 16 | 12,771 | 102,170 | 31 | 42,630 | 341,039 | 3.3× | 129 | 44,244 | 353,955 | 3.5× | 101 | 17% | 0.9 |

