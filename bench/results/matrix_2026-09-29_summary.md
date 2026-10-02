
### cols (synchronous=off)

| agents | stock tx/s | stock WAL tx/s | MultiWriter tx/s | MW vs WAL | WAL p99 us | MW p99 us | MW rebases/s | MW page conflicts/s | reps |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | - | 66304 | 65144 | 0.98x | 22.5 | 26.0 | 0 | 0 | 2 |
| 10 | - | 61773 | 15751 | 0.25x | 24.5 | 2611.0 | 6826 | 20510 | 2 |
| 100 | - | 48315 | 13735 | 0.28x | 41.0 | 10983.5 | 6807 | 17383 | 2 |

### crdtinsert (synchronous=off)

| agents | stock tx/s | stock WAL tx/s | MultiWriter tx/s | MW vs WAL | WAL p99 us | MW p99 us | MW rebases/s | MW page conflicts/s | reps |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | - | 50982 | 46294 | 0.91x | 39.5 | 44.5 | 0 | 0 | 2 |
| 10 | - | 48488 | 14094 | 0.29x | 41.5 | 13224.5 | 2661 | 9380 | 2 |
| 100 | - | 34303 | 10153 | 0.30x | 58.5 | 28488.0 | 2868 | 5902 | 2 |

### hot (synchronous=off)

| agents | stock tx/s | stock WAL tx/s | MultiWriter tx/s | MW vs WAL | WAL p99 us | MW p99 us | MW rebases/s | MW page conflicts/s | reps |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | - | 212908 | 277518 | 1.30x | 6.0 | 9.0 | 0 | 0 | 2 |
| 10 | - | 203945 | 235506 | 1.15x | 6.5 | 863.5 | 0 | 17218 | 2 |
| 100 | - | 169030 | 172730 | 1.02x | 9.0 | 2310.0 | 0 | 11410 | 2 |
| 1000 | - | 138212 | 125557 | 0.91x | 12.0 | 2650.0 | 0 | 8317 | 2 |

### independent (synchronous=full)

| agents | stock tx/s | stock WAL tx/s | MultiWriter tx/s | MW vs WAL | WAL p99 us | MW p99 us | MW rebases/s | MW page conflicts/s | reps |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | - | 12121 | 17258 | 1.42x | 48.0 | 389.0 | 0 | 0 | 2 |
| 10 | - | 11688 | 36101 | 3.09x | 49.5 | 1221.5 | 0 | 0 | 2 |
| 100 | - | 11000 | 77084 | 7.01x | 50.0 | 1472.0 | 0 | 0 | 2 |
| 1000 | - | 11348 | 82278 | 7.25x | 54.0 | 1412.5 | 0 | 0 | 2 |

### independent (synchronous=off)

| agents | stock tx/s | stock WAL tx/s | MultiWriter tx/s | MW vs WAL | WAL p99 us | MW p99 us | MW rebases/s | MW page conflicts/s | reps |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 10209 | 214833 | 289095 | 1.35x | 6.0 | 9.0 | 0 | 0 | 3 |
| 10 | 10177 | 213515 | 227067 | 1.06x | 7.0 | 76.0 | 0 | 0 | 3 |
| 100 | 9692 | 168376 | 226827 | 1.35x | 9.0 | 330.0 | 0 | 0 | 3 |
| 1000 | 9595 | 140411 | 164545 | 1.17x | 13.0 | 1038.0 | 0 | 0 | 3 |

### insert-autoinc (synchronous=off)

| agents | stock tx/s | stock WAL tx/s | MultiWriter tx/s | MW vs WAL | WAL p99 us | MW p99 us | MW rebases/s | MW page conflicts/s | reps |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | - | 153963 | 164164 | 1.07x | 10.0 | 14.0 | 0 | 0 | 2 |
| 10 | - | 151852 | 131640 | 0.87x | 10.5 | 1054.5 | 0 | 26695 | 2 |
| 100 | - | 108724 | 105552 | 0.97x | 15.0 | 4036.0 | 0 | 11833 | 2 |

### insert-int (synchronous=off)

| agents | stock tx/s | stock WAL tx/s | MultiWriter tx/s | MW vs WAL | WAL p99 us | MW p99 us | MW rebases/s | MW page conflicts/s | reps |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | - | 236771 | 253036 | 1.07x | 6.0 | 10.0 | 0 | 0 | 2 |
| 10 | - | 237840 | 197764 | 0.83x | 6.0 | 769.5 | 0 | 36702 | 2 |
| 100 | - | 153238 | 154484 | 1.01x | 12.0 | 2895.0 | 0 | 16102 | 2 |

### insert-uuid (synchronous=off)

| agents | stock tx/s | stock WAL tx/s | MultiWriter tx/s | MW vs WAL | WAL p99 us | MW p99 us | MW rebases/s | MW page conflicts/s | reps |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | - | 129186 | 134326 | 1.04x | 18.5 | 19.0 | 0 | 0 | 2 |
| 10 | - | 134246 | 111768 | 0.83x | 18.0 | 1275.5 | 0 | 24707 | 2 |
| 100 | - | 89855 | 72724 | 0.81x | 23.0 | 4426.0 | 0 | 7614 | 2 |

### longreader (synchronous=off)

| agents | stock tx/s | stock WAL tx/s | MultiWriter tx/s | MW vs WAL | WAL p99 us | MW p99 us | MW rebases/s | MW page conflicts/s | reps |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 | - | 48245 | 45827 | 0.95x | 1537.0 | 1082.0 | 0 | 0 | 2 |

### longtx (synchronous=off)

| agents | stock tx/s | stock WAL tx/s | MultiWriter tx/s | MW vs WAL | WAL p99 us | MW p99 us | MW rebases/s | MW page conflicts/s | reps |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | - | 128560 | 144403 | 1.12x | 11.0 | 13.0 | 0 | 0 | 2 |
| 10 | - | 14837 | 127259 | 8.58x | 356.0 | 1328.5 | 0 | 0 | 2 |
| 100 | - | 830 | 109614 | 132.03x | 5861.5 | 3255.5 | 0 | 0 | 2 |

### mixed (synchronous=off)

| agents | stock tx/s | stock WAL tx/s | MultiWriter tx/s | MW vs WAL | WAL p99 us | MW p99 us | MW rebases/s | MW page conflicts/s | reps |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | - | 519658 | 697420 | 1.34x | 5.0 | 5.0 | 0 | 0 | 2 |
| 10 | - | 408398 | 463544 | 1.14x | 7.0 | 80.0 | 0 | 0 | 4 |
| 100 | - | 298902 | 431490 | 1.44x | 15.0 | 324.0 | 0 | 0 | 4 |
| 1000 | - | 216386 | 317429 | 1.47x | 1268.0 | 583.5 | 0 | 0 | 2 |

### readonly (synchronous=off)

| agents | stock tx/s | stock WAL tx/s | MultiWriter tx/s | MW vs WAL | WAL p99 us | MW p99 us | MW rebases/s | MW page conflicts/s | reps |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | - | 726029 | 1199293 | 1.65x | 2.0 | 1.0 | 0 | 0 | 3 |
| 10 | - | 891116 | 789790 | 0.89x | 56.0 | 60.0 | 0 | 0 | 3 |
| 100 | - | 1022994 | 676701 | 0.66x | 284.0 | 657.0 | 0 | 0 | 3 |
| 1000 | - | 652737 | 674737 | 1.03x | 283.0 | 585.0 | 0 | 0 | 3 |

### samecol (synchronous=off)

| agents | stock tx/s | stock WAL tx/s | MultiWriter tx/s | MW vs WAL | WAL p99 us | MW p99 us | MW rebases/s | MW page conflicts/s | reps |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | - | 63580 | 65513 | 1.03x | 23.5 | 26.0 | 0 | 0 | 2 |
| 10 | - | 66679 | 84893 | 1.27x | 23.0 | 375.0 | 56090 | 56886 | 2 |
| 100 | - | 48400 | 78287 | 1.62x | 39.5 | 2958.0 | 50623 | 51060 | 2 |

### samepage (synchronous=off)

| agents | stock tx/s | stock WAL tx/s | MultiWriter tx/s | MW vs WAL | WAL p99 us | MW p99 us | MW rebases/s | MW page conflicts/s | reps |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | - | 64731 | 63823 | 0.99x | 23.0 | 26.0 | 0 | 0 | 2 |
| 10 | - | 65190 | 16330 | 0.25x | 26.0 | 2586.5 | 7107 | 21214 | 2 |
| 100 | - | 47823 | 15470 | 0.32x | 46.0 | 9012.5 | 7610 | 19465 | 2 |
