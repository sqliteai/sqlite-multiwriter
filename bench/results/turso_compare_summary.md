### Throughput: 100-row INSERT per transaction, disjoint keys, closed loop, synchronous=FULL (tx/s, mean of 3 runs; rows/s = 100x)

| connections | SQLite (rollback journal) | SQLite WAL | Multi-Writer | Turso 0.8.1 (BEGIN CONCURRENT) | Turso / SQLite WAL | MW / SQLite WAL |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 4134 | 12717 | 9398 | 5704 | 0.45x | 0.74x |
| 2 | 3627 | 10638 | 9020 | 8618 | 0.81x | 0.85x |
| 4 | 3678 | 10352 | 8893 | 17151 | 1.66x | 0.86x |
| 8 | 3445 | 10185 | 8535 | 29095 | 2.86x | 0.84x |
| 16 | 3326 | 9815 | 8917 | 32120 | 3.27x | 0.91x |
| 32 | 3296 | 9790 | 7592 | 43377 | 4.43x | 0.78x |
| 64 | 3136 | 9273 | 6645 | 43293 | 4.67x | 0.72x |

### Tail latency of the same transactions under closed loop (p99 / p99.9, ms)

| connections | SQLite WAL | Multi-Writer | Turso |
|---:|---:|---:|---:|
| 1 | 0.1 / 1.2 | 0.9 / 1.2 | 0.2 / 26.6 |
| 2 | 1.0 / 18.3 | 1.1 / 13.5 | 0.1 / 0.2 |
| 4 | 1.2 / 75.1 | 3.3 / 6.8 | 1.4 / 4.0 |
| 8 | 4.6 / 156.9 | 6.1 / 11.3 | 1.0 / 1.7 |
| 16 | 7.0 / 385.1 | 11.2 / 22.8 | 1.0 / 1.2 |
| 32 | 16.1 / 943.3 | 25.5 / 51.0 | 1.5 / 1.9 |
| 64 | 26.9 / 1017.1 | 31.3 / 61.7 | 5.4 / 7.7 |

### Latency at 1000 tx/s (Poisson arrivals over N connections, scheduled arrival -> commit, ms; mean of 3 runs of 10 s)

| connections | engine | p50 | p99 | p99.9 | max |
|---:|---|---:|---:|---:|---:|
| 1 | SQLite WAL | 0.48 | 3.7 | 4.9 | 7.9 |
| 1 | Multi-Writer | 0.53 | 2.2 | 4.6 | 6.8 |
| 1 | Turso (blocking wait) | 0.78 | 45.3 | 49.6 | 52.3 |
| 1 | Turso (tokio sleep) | 1.52 | 46.7 | 51.9 | 54.4 |
| 8 | SQLite WAL | 2.22 | 38.7 | 92.0 | 123.8 |
| 8 | Multi-Writer | 1.47 | 5.6 | 7.6 | 15.2 |
| 8 | Turso (blocking wait) | 2.52 | 63.3 | 70.0 | 76.2 |
| 8 | Turso (tokio sleep) | 1.72 | 61.4 | 66.4 | 71.1 |
| 16 | SQLite WAL | 3.40 | 63.7 | 121.4 | 248.3 |
| 16 | Multi-Writer | 2.06 | 6.2 | 7.7 | 11.3 |
| 16 | Turso (blocking wait) | 4.02 | 67.7 | 75.5 | 82.1 |
| 16 | Turso (tokio sleep) | 1.83 | 64.1 | 69.4 | 73.1 |
| 32 | SQLite WAL | 8.68 | 350.9 | 877.9 | 1329.1 |
| 32 | Multi-Writer | 4.16 | 11.4 | 14.4 | 21.5 |
| 32 | Turso (blocking wait) | 5.39 | 71.0 | 79.1 | 86.6 |
| 32 | Turso (tokio sleep) | 2.06 | 66.0 | 71.8 | 74.6 |
