
### cols (synchronous=off)

| agents | stock tx/s | stock WAL tx/s | MultiWriter tx/s | MW vs WAL | WAL p99 us | MW p99 us | MW rebases/s | MW page conflicts/s | reps |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | - | 64375 | 60451 | 0.94x | 20.0 | 27.0 | 0 | 0 | 2 |
| 10 | - | 60472 | 48032 | 0.79x | 25.0 | 2017.5 | 3824 | 8039 | 2 |
| 100 | - | 46942 | 34492 | 0.73x | 47.5 | 6048.0 | 2853 | 5039 | 2 |

### crdtinsert (synchronous=off)

| agents | stock tx/s | stock WAL tx/s | MultiWriter tx/s | MW vs WAL | WAL p99 us | MW p99 us | MW rebases/s | MW page conflicts/s | reps |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | - | 50801 | 44996 | 0.89x | 39.5 | 44.0 | 0 | 0 | 2 |
| 10 | - | 47751 | 31031 | 0.65x | 41.5 | 5246.0 | 2227 | 5981 | 2 |
| 100 | - | 34254 | 20681 | 0.60x | 57.0 | 11640.5 | 1882 | 2972 | 2 |

### hot (synchronous=off)

| agents | stock tx/s | stock WAL tx/s | MultiWriter tx/s | MW vs WAL | WAL p99 us | MW p99 us | MW rebases/s | MW page conflicts/s | reps |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | - | 212063 | 253507 | 1.20x | 6.0 | 10.0 | 0 | 0 | 2 |
| 10 | - | 196605 | 224479 | 1.14x | 7.5 | 802.5 | 0 | 16931 | 2 |
| 100 | - | 164453 | 139010 | 0.85x | 10.5 | 2351.0 | 0 | 9740 | 2 |
| 1000 | - | 134047 | 104084 | 0.78x | 15.0 | 2692.0 | 0 | 7257 | 2 |

### independent (synchronous=full)

| agents | stock tx/s | stock WAL tx/s | MultiWriter tx/s | MW vs WAL | WAL p99 us | MW p99 us | MW rebases/s | MW page conflicts/s | reps |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | - | 10869 | 38055 | 3.50x | 49.0 | 53.0 | 0 | 0 | 2 |
| 10 | - | 10936 | 64235 | 5.87x | 48.0 | 1097.5 | 0 | 0 | 2 |
| 100 | - | 11307 | 75492 | 6.68x | 48.5 | 2229.5 | 0 | 0 | 2 |
| 1000 | - | 11812 | 71322 | 6.04x | 52.0 | 2278.0 | 0 | 0 | 2 |

### independent (synchronous=off)

| agents | stock tx/s | stock WAL tx/s | MultiWriter tx/s | MW vs WAL | WAL p99 us | MW p99 us | MW rebases/s | MW page conflicts/s | reps |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 9941 | 213712 | 272131 | 1.27x | 6.0 | 9.0 | 0 | 0 | 3 |
| 10 | 9984 | 214118 | 218993 | 1.02x | 6.0 | 75.0 | 0 | 0 | 3 |
| 100 | 9516 | 167587 | 216218 | 1.29x | 9.0 | 870.0 | 0 | 0 | 3 |
| 1000 | 9305 | 139041 | 162198 | 1.17x | 13.0 | 1233.0 | 0 | 0 | 3 |

### insert-autoinc (synchronous=off)

| agents | stock tx/s | stock WAL tx/s | MultiWriter tx/s | MW vs WAL | WAL p99 us | MW p99 us | MW rebases/s | MW page conflicts/s | reps |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | - | 152897 | 155677 | 1.02x | 8.0 | 13.5 | 0 | 0 | 2 |
| 10 | - | 152953 | 128202 | 0.84x | 9.0 | 1040.5 | 0 | 26646 | 2 |
| 100 | - | 108609 | 88393 | 0.81x | 13.5 | 4431.0 | 0 | 9953 | 2 |

### insert-int (synchronous=off)

| agents | stock tx/s | stock WAL tx/s | MultiWriter tx/s | MW vs WAL | WAL p99 us | MW p99 us | MW rebases/s | MW page conflicts/s | reps |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | - | 232240 | 241995 | 1.04x | 6.0 | 9.5 | 0 | 0 | 2 |
| 10 | - | 238070 | 191093 | 0.80x | 6.5 | 822.0 | 0 | 35176 | 2 |
| 100 | - | 150499 | 129157 | 0.86x | 11.0 | 2949.0 | 0 | 13757 | 2 |

### insert-uuid (synchronous=off)

| agents | stock tx/s | stock WAL tx/s | MultiWriter tx/s | MW vs WAL | WAL p99 us | MW p99 us | MW rebases/s | MW page conflicts/s | reps |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | - | 128857 | 126015 | 0.98x | 18.5 | 19.0 | 0 | 0 | 2 |
| 10 | - | 131614 | 106240 | 0.81x | 18.0 | 1294.0 | 0 | 23733 | 2 |
| 100 | - | 83199 | 70206 | 0.84x | 25.5 | 5295.0 | 0 | 7243 | 2 |

### longreader (synchronous=off)

| agents | stock tx/s | stock WAL tx/s | MultiWriter tx/s | MW vs WAL | WAL p99 us | MW p99 us | MW rebases/s | MW page conflicts/s | reps |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 | - | 58044 | 54926 | 0.95x | 1392.5 | 831.5 | 0 | 0 | 2 |

### longtx (synchronous=off)

| agents | stock tx/s | stock WAL tx/s | MultiWriter tx/s | MW vs WAL | WAL p99 us | MW p99 us | MW rebases/s | MW page conflicts/s | reps |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | - | 127297 | 139207 | 1.09x | 11.0 | 14.0 | 0 | 0 | 2 |
| 10 | - | 15142 | 124495 | 8.22x | 336.0 | 1333.0 | 0 | 0 | 2 |
| 100 | - | 873 | 109254 | 125.12x | 3853.0 | 3344.5 | 0 | 0 | 2 |

### mixed (synchronous=off)

| agents | stock tx/s | stock WAL tx/s | MultiWriter tx/s | MW vs WAL | WAL p99 us | MW p99 us | MW rebases/s | MW page conflicts/s | reps |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | - | 513840 | 656898 | 1.28x | 5.0 | 6.0 | 0 | 0 | 2 |
| 10 | - | 400859 | 465598 | 1.16x | 7.0 | 78.0 | 0 | 0 | 4 |
| 100 | - | 303691 | 424400 | 1.40x | 13.5 | 321.5 | 0 | 0 | 4 |
| 1000 | - | 215694 | 311354 | 1.44x | 1269.0 | 605.5 | 0 | 0 | 2 |

### readonly (synchronous=off)

| agents | stock tx/s | stock WAL tx/s | MultiWriter tx/s | MW vs WAL | WAL p99 us | MW p99 us | MW rebases/s | MW page conflicts/s | reps |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | - | 719298 | 1192087 | 1.66x | 2.0 | 1.0 | 0 | 0 | 3 |
| 10 | - | 886674 | 781812 | 0.88x | 56.0 | 60.0 | 0 | 0 | 3 |
| 100 | - | 997843 | 695402 | 0.70x | 313.0 | 613.0 | 0 | 0 | 3 |
| 1000 | - | 599536 | 672976 | 1.12x | 348.0 | 461.0 | 0 | 0 | 3 |

### samecol (synchronous=off)

| agents | stock tx/s | stock WAL tx/s | MultiWriter tx/s | MW vs WAL | WAL p99 us | MW p99 us | MW rebases/s | MW page conflicts/s | reps |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | - | 61994 | 61437 | 0.99x | 21.5 | 27.0 | 0 | 0 | 2 |
| 10 | - | 63560 | 58013 | 0.91x | 24.5 | 1850.5 | 5211 | 5268 | 2 |
| 100 | - | 45633 | 38932 | 0.85x | 40.0 | 5557.5 | 2614 | 2658 | 2 |

### samepage (synchronous=off)

| agents | stock tx/s | stock WAL tx/s | MultiWriter tx/s | MW vs WAL | WAL p99 us | MW p99 us | MW rebases/s | MW page conflicts/s | reps |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | - | 62530 | 60508 | 0.97x | 23.5 | 26.5 | 0 | 0 | 2 |
| 10 | - | 63554 | 48262 | 0.76x | 21.5 | 2018.0 | 3849 | 7975 | 2 |
| 100 | - | 47654 | 34335 | 0.72x | 46.0 | 6053.5 | 2833 | 5032 | 2 |
