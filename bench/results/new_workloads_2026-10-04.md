# Altri carichi: SQLite contro Multi-Writer con tutto tracciato, 2026-10-04

16 thread (un processo) e 8 processi, 12 s a punto. `retry%` = tentativi rifiutati e ritentati ogni 100 transazioni; `rinunce` = transazioni rinunciate dopo 1000 retry. `sqlite` = busy_timeout, `sqlite0` = timeout 0 e retry dell'applicazione. RSS in MB (processi: mediana per processo). Il campo di attesa del banco non e valido in questi carichi (conta tx/s e la p99 della latenza). Nei processi, `valid=0` compare per tutte le varianti in alcuni carichi (cols, samecol, crdtinsert, insert-*): e il controllo finale del banco che non coincide con il suo conteggio, non un errore dei dati (anche SQLite).


## Aggiornamenti indipendenti (una riga a testa) (`independent`)

| | variante | tx/s | retry % | rinunce | RSS MB | lat p99 ms |
|---|---|---:|---:|---:|---:|---:|
| 16 thread | mw | 80672 | 0.0 | 0 | 55 | 0.36 |
| 16 thread | sqlite | 14568 | 0.0 | 0 | 13 | 0.05 |
| 16 thread | sqlite0 | 12688 | 21.9 | 0 | 12 | 0.06 |
| 8 processi | mw | 44853 | 0.0 | 0 | 262 | 0.50 |
| 8 processi | sqlite | 14509 | 0.0 | 0 | 7 | 0.05 |
| 8 processi | sqlite0 | 15572 | 8.5 | 0 | 7 | 0.05 |

## Solo letture (`readonly`)

| | variante | tx/s | retry % | rinunce | RSS MB | lat p99 ms |
|---|---|---:|---:|---:|---:|---:|
| 16 thread | mw | 861339 | 0.0 | 0 | 114 | 0.11 |
| 16 thread | sqlite | 883273 | 0.0 | 0 | 114 | 0.09 |
| 16 thread | sqlite0 | 884762 | 0.0 | 0 | 114 | 0.09 |
| 8 processi | mw | 559398 | 0.0 | 0 | 16 | 0.05 |
| 8 processi | sqlite | 282735 | 0.0 | 0 | 12 | 0.04 |
| 8 processi | sqlite0 | 284381 | 0.0 | 0 | 12 | 0.04 |

## Misto lettura/scrittura (`mixed`)

| | variante | tx/s | retry % | rinunce | RSS MB | lat p99 ms |
|---|---|---:|---:|---:|---:|---:|
| 16 thread | mw | 380028 | 0.0 | 0 | 120 | 0.28 |
| 16 thread | sqlite | 73842 | 0.0 | 0 | 21 | 0.03 |
| 16 thread | sqlite0 | 281987 | 592.1 | 0 | 50 | 0.32 |
| 8 processi | mw | 202416 | 0.0 | 0 | 374 | 0.28 |
| 8 processi | sqlite | 93540 | 0.0 | 0 | 8 | 0.03 |
| 8 processi | sqlite0 | 215152 | 268.2 | 0 | 12 | 0.20 |

## Transazioni lunghe (`longtx`)

| | variante | tx/s | retry % | rinunce | RSS MB | lat p99 ms |
|---|---|---:|---:|---:|---:|---:|
| 16 thread | mw | 33466 | 118.2 | 0 | 43 | 7.27 |
| 16 thread | sqlite | 10502 | 7.2 | 0 | 12 | 0.05 |
| 16 thread | sqlite0 | 37730 | 7.7 | 0 | 17 | 0.04 |
| 8 processi | mw | 40264 | 76.2 | 0 | 363 | 0.82 |
| 8 processi | sqlite | 11770 | 2.4 | 0 | 7 | 0.19 |
| 8 processi | sqlite0 | 37985 | 2.6 | 2 | 7 | 0.04 |

## Righe calde (4 righe condivise) (`hot`)

| | variante | tx/s | retry % | rinunce | RSS MB | lat p99 ms |
|---|---|---:|---:|---:|---:|---:|
| 16 thread | mw | 37659 | 10.9 | 0 | 37 | 0.08 |
| 16 thread | sqlite | 12308 | 0.0 | 0 | 12 | 0.04 |
| 16 thread | sqlite0 | 18747 | 16.0 | 0 | 13 | 0.05 |
| 8 processi | mw | 37895 | 83.4 | 0 | 228 | 0.81 |
| 8 processi | sqlite | 11903 | 0.0 | 0 | 7 | 0.04 |
| 8 processi | sqlite0 | 16316 | 8.1 | 0 | 7 | 0.05 |

## Righe diverse nella stessa pagina (`samepage`)

| | variante | tx/s | retry % | rinunce | RSS MB | lat p99 ms |
|---|---|---:|---:|---:|---:|---:|
| 16 thread | mw | 37296 | 10.5 | 0 | 41 | 0.07 |
| 16 thread | sqlite | 12387 | 0.0 | 0 | 12 | 0.04 |
| 16 thread | sqlite0 | 21377 | 14.0 | 0 | 14 | 0.04 |
| 8 processi | mw | 37894 | 78.0 | 0 | 215 | 0.53 |
| 8 processi | sqlite | 12608 | 0.0 | 0 | 7 | 0.05 |
| 8 processi | sqlite0 | 19430 | 6.3 | 0 | 7 | 0.05 |

## Colonne diverse della stessa riga (`cols`)

| | variante | tx/s | retry % | rinunce | RSS MB | lat p99 ms |
|---|---|---:|---:|---:|---:|---:|
| 16 thread | mw | 39329 | 10.6 | 0 | 37 | 0.07 |
| 16 thread | sqlite | 11174 | 0.0 | 0 | 12 | 0.04 |
| 16 thread | sqlite0 | 20776 | 13.9 | 0 | 14 | 0.05 |
| 8 processi | mw | 39270 | 85.6 | 0 | 200 | 0.76 |
| 8 processi | sqlite | 11047 | 0.0 | 0 | 7 | 0.05 |
| 8 processi | sqlite0 | 17793 | 7.6 | 0 | 7 | 0.05 |

## Stessa colonna della stessa riga (`samecol`)

| | variante | tx/s | retry % | rinunce | RSS MB | lat p99 ms |
|---|---|---:|---:|---:|---:|---:|
| 16 thread | mw | 38602 | 10.3 | 0 | 36 | 0.07 |
| 16 thread | sqlite | 12094 | 0.0 | 0 | 12 | 0.04 |
| 16 thread | sqlite0 | 24603 | 13.3 | 0 | 14 | 0.04 |
| 8 processi | mw | 38220 | 85.3 | 0 | 254 | 0.82 |
| 8 processi | sqlite | 12468 | 0.0 | 0 | 7 | 0.05 |
| 8 processi | sqlite0 | 20012 | 6.5 | 0 | 7 | 0.05 |

## Inserimenti CRDT (`crdtinsert`)

| | variante | tx/s | retry % | rinunce | RSS MB | lat p99 ms |
|---|---|---:|---:|---:|---:|---:|
| 16 thread | mw | 32073 | 11.2 | 0 | 130 | 0.06 |
| 16 thread | sqlite | 17115 | 0.0 | 0 | 15 | 0.05 |
| 16 thread | sqlite0 | 5448 | 78.4 | 0 | 11 | 16.76 |
| 8 processi | mw | 31396 | 80.5 | 0 | 180 | 0.94 |
| 8 processi | sqlite | 16673 | 0.0 | 0 | 8 | 0.05 |
| 8 processi | sqlite0 | 16750 | 55.7 | 0 | 8 | 1.59 |

## Inserimenti con chiave UUID (`insert-uuid`)

| | variante | tx/s | retry % | rinunce | RSS MB | lat p99 ms |
|---|---|---:|---:|---:|---:|---:|
| 16 thread | mw | 29436 | 10.8 | 0 | 145 | 0.23 |
| 16 thread | sqlite | 16773 | 0.0 | 0 | 14 | 0.05 |
| 16 thread | sqlite0 | 14752 | 72.6 | 0 | 13 | 13.92 |
| 8 processi | mw | 31549 | 79.5 | 0 | 177 | 0.94 |
| 8 processi | sqlite | 17410 | 0.0 | 0 | 7 | 0.05 |
| 8 processi | sqlite0 | 22144 | 40.0 | 0 | 8 | 0.37 |

## Inserimenti con chiave intera (`insert-int`)

| | variante | tx/s | retry % | rinunce | RSS MB | lat p99 ms |
|---|---|---:|---:|---:|---:|---:|
| 16 thread | mw | 29163 | 11.3 | 0 | 112 | 0.06 |
| 16 thread | sqlite | 11400 | 0.0 | 0 | 12 | 0.04 |
| 16 thread | sqlite0 | 21191 | 15.1 | 0 | 14 | 0.06 |
| 8 processi | mw | 33052 | 78.1 | 0 | 180 | 2.62 |
| 8 processi | sqlite | 11429 | 0.0 | 0 | 7 | 0.04 |
| 8 processi | sqlite0 | 19589 | 6.7 | 0 | 7 | 0.05 |

## Inserimenti con autoincrement (`insert-autoinc`)

| | variante | tx/s | retry % | rinunce | RSS MB | lat p99 ms |
|---|---|---:|---:|---:|---:|---:|
| 16 thread | mw | 27672 | 12.9 | 0 | 112 | 0.06 |
| 16 thread | sqlite | 17009 | 0.0 | 0 | 13 | 0.05 |
| 16 thread | sqlite0 | 15931 | 40.0 | 0 | 13 | 0.67 |
| 8 processi | mw | 29533 | 78.6 | 0 | 176 | 1.59 |
| 8 processi | sqlite | 16466 | 0.0 | 0 | 7 | 0.05 |
| 8 processi | sqlite0 | 16238 | 19.3 | 0 | 7 | 0.23 |

## Un lettore che tiene lo snapshot per tutta la prova (`longreader`)

| | variante | tx/s | retry % | rinunce | RSS MB | lat p99 ms |
|---|---|---:|---:|---:|---:|---:|
| 16 thread | mw | 4641 | 0.0 | 0 | 806 | 2.70 |
| 16 thread | sqlite | 12213 | 0.0 | 0 | 15 | 11.19 |
| 16 thread | sqlite0 | 36625 | 15.1 | 0 | 23 | 1.66 |
| 8 processi | mw | 4211 | 0.0 | 0 | 317 | 6.53 |
| 8 processi | sqlite | 11435 | 0.0 | 0 | 9 | 4.73 |
| 8 processi | sqlite0 | 37177 | 7.2 | 0 | 13 | 1.39 |

## Righe calde, transazione aperta 5 ms (`slowhot`)

| | variante | tx/s | retry % | rinunce | RSS MB | lat p99 ms |
|---|---|---:|---:|---:|---:|---:|
| 16 thread | mw | 194 | 0.0 | 0 | 24 | 86.20 |
| 16 thread | sqlite | 139 | 0.0 | 0 | 10 | 7.73 |
| 16 thread | sqlite0 | 150 | 637.2 | 18 | 10 | 1872.32 |
| 8 processi | mw | 192 | 0.9 | 0 | 20 | 44.29 |
| 8 processi | sqlite | 138 | 0.0 | 0 | 7 | 7.72 |
| 8 processi | sqlite0 | 146 | 276.9 | 9 | 7 | 452.95 |

## Stessa pagina, transazione aperta 5 ms (`slowpage`)

| | variante | tx/s | retry % | rinunce | RSS MB | lat p99 ms |
|---|---|---:|---:|---:|---:|---:|
| 16 thread | mw | 194 | 0.0 | 0 | 28 | 85.04 |
| 16 thread | sqlite | 138 | 0.0 | 0 | 10 | 7.72 |
| 16 thread | sqlite0 | 149 | 309.8 | 25 | 10 | 176.24 |
| 8 processi | mw | 192 | 1.8 | 0 | 21 | 44.65 |
| 8 processi | sqlite | 137 | 0.0 | 0 | 7 | 7.73 |
| 8 processi | sqlite0 | 146 | 489.1 | 4 | 7 | 2447.93 |

## Bulk da 100 righe larghe (5 colonne) (`MW_BENCH_BULK_WIDE`)

| | variante | tx/s | retry % | rinunce | RSS MB | lat p99 ms |
|---|---|---:|---:|---:|---:|---:|
| 16 thread | mw | 32058 | 0.2 | 0 | 660 | 1.89 |
| 16 thread | sqlite | 8070 | 0.0 | 0 | 14 | 0.16 |
| 16 thread | sqlite0 | 7284 | 170.7 | 0 | 14 | 64.47 |
| 8 processi | mw | 15124 | 0.8 | 0 | 400 | 3.96 |
| 8 processi | sqlite | 7912 | 0.0 | 0 | 10 | 0.20 |
| 8 processi | sqlite0 | 7347 | 117.3 | 0 | 10 | 37.04 |

## Bulk da 100 righe con chiavi casuali (`MW_BENCH_BULK_RANDOM`)

| | variante | tx/s | retry % | rinunce | RSS MB | lat p99 ms |
|---|---|---:|---:|---:|---:|---:|
| 16 thread | mw | 449 | 14.6 | 1 | 295 | 506.87 |
| 16 thread | sqlite | 454 | 0.0 | 0 | 26 | 574.21 |
| 16 thread | sqlite0 | 464 | 661.4 | 0 | 28 | 654.71 |
| 8 processi | mw | 362 | 53.9 | 0 | 268 | 110.03 |
| 8 processi | sqlite | 460 | 0.0 | 0 | 16 | 784.10 |
| 8 processi | sqlite0 | 466 | 354.8 | 0 | 18 | 600.26 |
