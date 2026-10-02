### After the improvements: 100-row INSERT per transaction, disjoint keys, closed loop, synchronous=FULL (tx/s, mean of 3 runs)

| connections | SQLite WAL | Multi-Writer before | Multi-Writer after | Turso 0.8.1 | MW after / WAL | Turso / WAL |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 12697 | 9398 | 11832 | 5779 | 0.93x | 0.46x |
| 2 | 10806 | 9020 | 8595 | 8559 | 0.80x | 0.79x |
| 4 | 10234 | 8893 | 10177 | 17434 | 0.99x | 1.70x |
| 8 | 10052 | 8535 | 16399 | 28756 | 1.63x | 2.86x |
| 16 | 9791 | 8917 | 13509 | 31883 | 1.38x | 3.26x |
| 32 | 9736 | 7592 | 11627 | 43362 | 1.19x | 4.45x |
| 64 | 9296 | 6645 | 10770 | 43446 | 1.16x | 4.67x |

### Latency at 1000 tx/s (Poisson arrivals, scheduled arrival -> commit; p99 / p99.9 in ms)

| connections | SQLite WAL | Multi-Writer before | Multi-Writer after | Turso (blocking wait, first run) |
|---:|---:|---:|---:|---:|
| 1 | 3.9 / 5.1 | 2.2 / 4.6 | 3.0 / 6.3 | 45.3 / 49.6 |
| 8 | 61.1 / 172.7 | 5.6 / 7.6 | 10.3 / 10.8 | 63.3 / 70.0 |
| 16 | 119.1 / 267.5 | 6.2 / 7.7 | 10.8 / 12.8 | 67.7 / 75.5 |
| 32 | 76.4 / 257.7 | 11.4 / 14.4 | 6.2 / 8.3 | 71.0 / 79.1 |
