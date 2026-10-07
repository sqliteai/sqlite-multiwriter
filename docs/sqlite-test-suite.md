# SQLite's own test suite through the engine (2026-10-07, run again after the fixes below)

How: `test/tcl/README.md`. SQLite 3.53.4 source tree, `testfixture` built with the engine linked in, every database opened with `mw=2` (`MW_DEFAULT_MODE=2`), the 1190 `test/*.test` files run one by one (60 s each), compared with the stock `testfixture` on the same files.

| | stock | engine (`mw=2`), first run | engine, after the fixes |
|---|---|---|---|
| files with no error | 1151 | 872 | 876 |
| files with errors | 7 | 206 | 203 |
| no summary (aborted by an error at the top level) | 17 | 93 | 94 |
| timeout (60 s) | 15 | 35 | 17 |

876 files pass identically (the whole SQL layer: select, insert, indexes, joins, triggers, views, CTEs, window functions, JSON, FTS, R-tree... all the `select*`, `insert*`, `index*`, `join*`, `where*`, `trigger*`, `fts*` files that do not touch the file).
The other 314 differ. Almost all of them are what the engine is by design; this is the reading, from the first error of each file and the samples that were examined one by one. It is **not** a complete triage of the 318 files.

## Differences by design (not bugs)

- **Unsupported modes** (99 files fail on their first error): `journal_mode` other than WAL, `auto_vacuum`, `locking_mode=EXCLUSIVE`, `journal_mode=off`: the engine answers with an error (`multiwriter: ... is not supported`). The rest of those files then fails on what depended on it, and three of them end in a segmentation fault that is the *test's*: after a failed `prepare` it uses the invalid statement (`createtab`, `incrcorrupt`, `walvfs`).
- **Tests that look at the files**: `test.db-journal` exists or not (`trans`, `misc1`, `delete_db`...), size of the file (`sync`, `backup4`), number of open files (`stmt`, `tempdb`: the engine has the `-mw` sidecar), `hexio_write` into `test.db` (`alter2`, `alter3`, `corrupt*`: the data is in the log, not in the file), WAL file protocol (`wal*`, `walcrash*`, `e_wal*`), fault injection by VFS (`crash*`, `*fault*`, `*malloc*`: the engine allocates with `malloc`, so the counts of the injected failures do not match).
- **Locks that do not block**: tests that expect `SQLITE_BUSY` between two connections of one process (`lock*`, `busy`, `attach`, `capi3b`, `thread1`, `tkt2409`, `tclsqlite-10.18`) because the second writer is allowed in; a second *process* that opens the file without `mw_mp=1` (`tkt4018`).
- **`PRAGMA page_size` on a new database is ignored** (`pagesize`, `format4`, `tkt1512`...): the engine converts a new file to WAL when it opens it, with the default page size, before the application's pragma. To choose the page size, create the database with a stock connection first.
- **Slow**: `savepoint4`, `exprfault2`, `fts4merge2` run past 60 s (cold cache on every transaction), they do not fail.

## What the suite found, and was fixed

- **Deadlock at open when the application enabled the shared cache** (`sqlite3_enable_shared_cache(1)`, `shared*.test`, `thread002`, `vtab_shared`...): the engine's own private connection (conversion of a new file to WAL, and the rebase helper) opened in shared-cache mode inside the open of the application's connection and waited for a mutex that the outer open held. They now open with `SQLITE_OPEN_PRIVATECACHE`. With the shared cache the engine behaves like stock (`database table is locked`).
- **A VFS stacked over the engine failed every write** (`devsym`, the test VFS of SQLite; the same for any VFS that reports a device without power-safe overwrite or with a big sector, such as an encryption or multiplex layer): SQLite then pads the WAL at the commit to a sector boundary, with a frame header and part of a page, and the engine answered that write with `SQLITE_IOERR_WRITE`. The padding after the commit frame is accepted now. Test `mw_stacked` (sector 512/4096/8192, page 1024/4096).
- **Out of memory came out as `SQLITE_BUSY_SNAPSHOT`** (`mallocA`, `mallocD`, `mallocF`, `fkey_malloc`, `vtab_err`...: 40 files fewer with errors in the fault-injection group): when the allocation of the read set of a transaction failed, the engine refused the commit as a read conflict, and an application retries a conflict. The commit fails with `SQLITE_NOMEM` now. Test `mw_stacked` (every allocation of a transaction failing in turn: no `BUSY`, the connection works afterwards; it fails on the old code with 2 `BUSY`).
- **`sqlite3_backup_step(n)` with a small `n` never ended** (`sqlite3` shell `.backup` steps by 100 pages): every snapshot started with a cold page cache, SQLite resets the cache when it is told that the database changed, and the backup interface starts again when the cache of its source is reset. A connection that has not asked for the write lock for 64 snapshots now keeps its cache, and its read set, from one read-only snapshot to the next when they are at the same epoch (the header of the private wal-index stays valid, and the next `snapshot_begin` invalidates it when the epoch is another one). A connection that writes now and then keeps the cold cache: with the cache inherited the read set it brings is a superset of what the transaction reads, and in a mixed workload (90% reads) it made 150 thousand commits conflict for pages that were not read, 37% fewer writes. Test `mw_warm` (never stale, the read set is inherited, the incremental backup ends). Read-only workload: 795k -> 884k reads/s (8 threads); mixed: unchanged. A backup from a connection that just wrote needs 64 steps of reads before it ends (each of them starts again).

## Not understood yet

- A few files that were not looked at one by one (`vacuum*` with `database is locked`, `interrupt2`, `incrblob2/3`, `temptable-4.x`, `mmap3`, `e_walhook`, `sqllimits1-7.7.1`): they may be lock semantics or file-level expectations, or real defects.
- `fts4merge4-2.2.3.2` and `2.2.4.2` (the variants that close and reopen the database) fail now and then (6 failures in about 25 runs one afternoon, none in 100 runs later; the same before the changes of this day): the number of segments after the automatic merge differs from the expected one. Not reproduced under load, with a copy of the test, or in parallel. It looks timing dependent (background threads of the engine), and a different result of a deterministic merge would be a data problem, so it is worth finding.
- In some files an injected out-of-memory is absorbed (`mallocE`, `mallocG`, `malloc6`...: the statement succeeds where the test expects an error): SQLite retries internally after the cold-cache schema reload; not a defect that was seen.
- `main-2.0`: opening a file that is not a database fails at open (the engine reads the header to convert to WAL), stock fails at the first statement.
