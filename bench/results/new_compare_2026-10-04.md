# SQLite (WAL, synchronous=FULL) contro Multi-Writer con tutto tracciato, 2026-10-04

Macchina: 18 core, macOS. `mw` = Multi-Writer con cattura CRDT su ogni tabella (thread: un processo; processi: modalita condivisa). `sqlite` = WAL con busy_timeout (il lock lo aspetta SQLite). `sqlite0` = busy_timeout 0, l'applicazione ritenta con backoff (i retry sono contati; limite 1000 per transazione). Carico: transazioni da 100 inserimenti, 20 s (15 s per la transazione lenta). `retr/tx%` = tentativi rifiutati e ritentati su cento transazioni; `gave_up` = transazioni rinunciate dopo 1000 retry. Attesa = tempo tra il primo tentativo e il via della scrittura (lock di scrittura per SQLite, ammissione per Multi-Writer). RSS in MB (thread: del processo; processi: mediana per processo, somma, e crescita della memoria di sistema che non conta N volte le mappature condivise).


## Thread, inserimenti da 100 righe

| variante | n | tx/s | retry | retr/tx % | rinunce | RSS MB (mediana / somma / crescita sistema) | attesa p50 us | attesa p99 us | attesa max ms | lat p99 ms |
|---|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|
| mw | 1 | 9797 | 59 | 0.03 | 0 | 406 | 8 | 10 | 0.2 | 0.2 |
| sqlite | 1 | 12281 | 0 | 0.00 | 0 | 14 | 8 | 9 | 0.0 | 0.1 |
| sqlite0 | 1 | 12301 | 0 | 0.00 | 0 | 14 | 8 | 9 | 0.0 | 0.1 |
| mw | 2 | 13820 | 245 | 0.08 | 0 | 510 | 8 | 16 | 0.6 | 0.2 |
| sqlite | 2 | 10446 | 0 | 0.00 | 0 | 14 | 8 | 9 | 2676.6 | 0.1 |
| sqlite0 | 2 | 9058 | 206014 | 104.15 | 0 | 15 | 31 | 137 | 129.1 | 0.4 |
| mw | 4 | 19859 | 381 | 0.09 | 0 | 808 | 9 | 40 | 3.1 | 0.6 |
| sqlite | 4 | 9819 | 0 | 0.00 | 0 | 14 | 8 | 9 | 4433.2 | 0.1 |
| sqlite0 | 4 | 8502 | 227808 | 122.48 | 0 | 13 | 33 | 2687 | 169.6 | 2.9 |
| mw | 8 | 28799 | 946 | 0.15 | 0 | 926 | 12 | 69 | 2.0 | 1.0 |
| sqlite | 8 | 9689 | 0 | 0.00 | 0 | 14 | 8 | 9 | 3633.6 | 0.1 |
| sqlite0 | 8 | 8270 | 284713 | 156.73 | 0 | 14 | 36 | 25631 | 316.4 | 25.9 |
| mw | 16 | 33366 | 2148 | 0.29 | 0 | 1078 | 18 | 193 | 6.0 | 1.8 |
| sqlite | 16 | 9563 | 0 | 0.00 | 0 | 16 | 8 | 9 | 7800.2 | 0.4 |
| sqlite0 | 16 | 8127 | 336275 | 188.73 | 0 | 16 | 37 | 57710 | 388.2 | 57.9 |
| mw | 32 | 28371 | 2798 | 0.45 | 0 | 1458 | 37 | 721 | 72.6 | 11.2 |
| sqlite | 32 | 9387 | 0 | 0.00 | 0 | 19 | 8 | 9 | 9549.7 | 0.9 |
| sqlite0 | 32 | 7885 | 451007 | 260.27 | 0 | 18 | 47 | 95802 | 463.5 | 96.0 |
| mw | 64 | 24260 | 4260 | 0.79 | 0 | 1788 | 45 | 1726 | 25.7 | 25.5 |
| sqlite | 64 | 9287 | 0 | 0.00 | 0 | 26 | 8 | 10 | 16332.9 | 1.0 |
| sqlite0 | 64 | 7614 | 650478 | 389.28 | 0 | 25 | 64 | 149365 | 507.6 | 149.7 |

## Processi, inserimenti da 100 righe

| variante | n | tx/s | retry | retr/tx % | rinunce | RSS MB (mediana / somma / crescita sistema) | attesa p50 us | attesa p99 us | attesa max ms | lat p99 ms |
|---|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|
| mw | 1 | 9616 | 27 | 0.01 | 0 | 195 / 195 / 70 | 9 | 12 | 0.1 | 0.3 |
| sqlite | 1 | 12123 | 0 | 0.00 | 0 | 14 / 14 / -107 | 8 | 9 | 0.0 | 0.1 |
| sqlite0 | 1 | 12104 | 0 | 0.00 | 0 | 14 / 14 / 77 | 8 | 9 | 0.1 | 0.1 |
| mw | 2 | 13258 | 436 | 0.15 | 0 | 455 / 831 / 452 | 10 | 14 | 0.4 | 0.4 |
| sqlite | 2 | 10434 | 0 | 0.00 | 0 | 12 / 24 / 24 | 8 | 10 | 5495.4 | 0.1 |
| sqlite0 | 2 | 9182 | 191589 | 95.21 | 0 | 12 / 24 / 83 | 30 | 138 | 172.4 | 0.4 |
| mw | 4 | 16810 | 1331 | 0.36 | 0 | 1033 / 4128 / 1249 | 10 | 20 | 2.1 | 0.5 |
| sqlite | 4 | 9857 | 0 | 0.00 | 0 | 10 / 40 / 20 | 8 | 10 | 6297.4 | 0.1 |
| sqlite0 | 4 | 8363 | 228158 | 124.49 | 0 | 11 / 42 / 21 | 34 | 3131 | 223.2 | 3.3 |
| mw | 8 | 16292 | 4005 | 1.11 | 0 | 628 / 4834 / 1447 | 11 | 295 | 3.8 | 3.8 |
| sqlite | 8 | 9637 | 0 | 0.00 | 0 | 10 / 80 / 25 | 8 | 10 | 5070.4 | 0.5 |
| sqlite0 | 8 | 8121 | 269426 | 151.32 | 0 | 10 / 80 / 37 | 36 | 27943 | 281.2 | 28.1 |
| mw | 16 | 14862 | 11784 | 3.59 | 0 | 664 / 10779 / 2451 | 12 | 1196 | 7.5 | 7.4 |
| sqlite | 16 | 9416 | 0 | 0.00 | 0 | 10 / 152 / 77 | 8 | 12 | 6698.0 | 1.0 |
| sqlite0 | 16 | 7831 | 327123 | 190.02 | 0 | 9 / 150 / -36 | 37 | 74055 | 521.0 | 74.3 |
| mw | 32 | 13796 | 19064 | 6.26 | 0 | 587 / 18622 / 4700 | 14 | 2179 | 31.3 | 7.8 |
| sqlite | 32 | 9310 | 0 | 0.00 | 0 | 9 / 288 / 168 | 9 | 15 | 10672.9 | 1.1 |
| sqlite0 | 32 | 7643 | 412027 | 245.16 | 0 | 9 / 279 / 110 | 40 | 164899 | 755.8 | 165.3 |
| mw | 64 | 12629 | 21172 | 7.63 | 0 | 419 / 26850 / 6566 | 1995 | 8838 | 75.1 | 15.3 |
| sqlite | 64 | 9182 | 0 | 0.00 | 0 | 9 / 576 / 304 | 9 | 232239 | 10516.8 | 232.4 |
| sqlite0 | 64 | 7474 | 572238 | 349.38 | 0 | 8 / 515 / 302 | 49 | 273694 | 1104.4 | 274.1 |


Nella transazione lenta ogni scrittore tiene la transazione aperta 10 ms dopo la scrittura (un'applicazione che lavora a meta). Il campo di attesa del banco in questo carico non e valido (valori fuori scala): conta tx/s e la p99 della latenza.


## Thread, transazione lenta (10 ms)

| variante | n | tx/s | retry | retr/tx % | rinunce | RSS MB | lat p99 ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| mw | 4 | 292 | 0 | 0 | 0 | 20 | 15.2 |
| sqlite | 4 | 72 | 0 | 0 | 0 | 7 | 15.2 |
| sqlite0 | 4 | 76 | 1408 | 109 | 7 | 7 | 15.3 |
| mw | 16 | 1181 | 0 | 0 | 0 | 33 | 15.3 |
| sqlite | 16 | 73 | 0 | 0 | 0 | 10 | 15.3 |
| sqlite0 | 16 | 74 | 7237 | 577 | 35 | 10 | 1419.9 |
| mw | 64 | 4806 | 0 | 0 | 0 | 78 | 15.5 |
| sqlite | 64 | 74 | 0 | 0 | 0 | 19 | 15.3 |
| sqlite0 | 64 | 71 | 36396 | 2996 | 148 | 20 | 3917.6 |

## Processi, transazione lenta (10 ms)

| variante | n | tx/s | retry | retr/tx % | rinunce | RSS MB | lat p99 ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| mw | 4 | 293 | 0 | 0 | 0 | 32 / 121 / 22 | 15.3 |
| sqlite | 4 | 72 | 0 | 0 | 0 | 6 / 25 / 7 | 15.3 |
| sqlite0 | 4 | 75 | 2183 | 171 | 5 | 7 / 28 / -16 | 15.4 |
| mw | 16 | 1192 | 0 | 0 | 0 | 37 / 572 / 87 | 15.5 |
| sqlite | 16 | 74 | 0 | 0 | 0 | 6 / 100 / 110 | 15.3 |
| sqlite0 | 16 | 74 | 14035 | 1120 | 28 | 7 / 110 / 38 | 3434.5 |
| mw | 64 | 5010 | 0 | 0 | 0 | 47 / 2828 / 347 | 15.7 |
| sqlite | 64 | 73 | 0 | 0 | 0 | 6 / 406 / 154 | 15.3 |
| sqlite0 | 64 | 71 | 52097 | 4320 | 129 | 7 / 444 / 120 | 4115.9 |
