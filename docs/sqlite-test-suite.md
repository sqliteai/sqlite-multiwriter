# SQLite's own test suite through the engine (2026-10-07)

How: `test/tcl/README.md`. SQLite 3.53.4 source tree, `testfixture` built with the engine linked in, every database opened with `mw=2` (`MW_DEFAULT_MODE=2`), the 1190 `test/*.test` files run one by one (60 s each), compared with the stock `testfixture` on the same files.

| | stock | engine (`mw=2`) |
|---|---|---|
| files with no error | 1151 | 872 |
| files with errors | 7 | 206 |
| no summary (aborted by an error at the top level) | 17 | 93 |
| timeout (60 s) | 15 | 19 |

872 files pass identically (the whole SQL layer: select, insert, indexes, joins, triggers, views, CTEs, window functions, JSON, FTS, R-tree... all the `select*`, `insert*`, `index*`, `join*`, `where*`, `trigger*`, `fts*` files that do not touch the file).
The other 318 differ. Almost all of them are what the engine is by design; this is the reading, from the first error of each file and the samples that were examined one by one. It is **not** a complete triage of the 318 files.

## Differences by design (not bugs)

- **Unsupported modes** (99 files fail on their first error): `journal_mode` other than WAL, `auto_vacuum`, `locking_mode=EXCLUSIVE`, `journal_mode=off`: the engine answers with an error (`multiwriter: ... is not supported`). The rest of those files then fails on what depended on it, and three of them end in a segmentation fault that is the *test's*: after a failed `prepare` it uses the invalid statement (`createtab`, `incrcorrupt`, `walvfs`).
- **Tests that look at the files**: `test.db-journal` exists or not (`trans`, `misc1`, `delete_db`...), size of the file (`sync`, `backup4`), number of open files (`stmt`, `tempdb`: the engine has the `-mw` sidecar), `hexio_write` into `test.db` (`alter2`, `alter3`, `corrupt*`: the data is in the log, not in the file), WAL file protocol (`wal*`, `walcrash*`, `e_wal*`), fault injection by VFS (`crash*`, `*fault*`, `*malloc*`: the engine allocates with `malloc`, so the counts of the injected failures do not match).
- **Locks that do not block**: tests that expect `SQLITE_BUSY` between two connections of one process (`lock*`, `busy`, `attach`, `capi3b`, `thread1`, `tkt2409`, `tclsqlite-10.18`) because the second writer is allowed in; a second *process* that opens the file without `mw_mp=1` (`tkt4018`).
- **`PRAGMA page_size` on a new database is ignored** (`pagesize`, `format4`, `tkt1512`...): the engine converts a new file to WAL when it opens it, with the default page size, before the application's pragma. To choose the page size, create the database with a stock connection first.
- **`sqlite3_backup_step(n)` with a small `n` never ends** (`backup`, `backup2`, `backup5`): every transaction starts with a cold page cache (it is how the read set is known), and the backup interface restarts when the cache is reset. `sqlite3_backup_step(-1)` and `VACUUM INTO` do not have it; the `sqlite3` shell's `.backup` steps by 100 pages, so it does not finish on an `mw=2` database. To be fixed or documented before a release.
- **Slow**: `savepoint4`, `exprfault2`, `fts4merge2` run past 60 s (cold cache on every transaction), they do not fail.

## What the suite found, and was fixed

- **Deadlock at open when the application enabled the shared cache** (`sqlite3_enable_shared_cache(1)`, `shared*.test`, `thread002`, `vtab_shared`...): the engine's own private connection (conversion of a new file to WAL, and the rebase helper) opened in shared-cache mode inside the open of the application's connection and waited for a mutex that the outer open held. They now open with `SQLITE_OPEN_PRIVATECACHE`. With the shared cache the engine behaves like stock (`database table is locked`). Timeouts went from 35 to 19 files (15 of which time out with stock SQLite too).

## Not understood yet

- OOM injection: in `mallocD`, `mallocA`, `mallocF` a failed allocation inside SQLite comes out as `database is locked` where the test expects `out of memory`: some path of the engine turns `SQLITE_NOMEM` into `SQLITE_BUSY`, and an application would retry an out-of-memory error. To find.
- A few files that were not looked at one by one (`vacuum*` with `database is locked`, `interrupt2`, `incrblob2/3`, `temptable-4.x`, `mmap3`, `e_walhook`, `sqllimits1-7.7.1`): they may be lock semantics or file-level expectations, or real defects.
- `main-2.0`: opening a file that is not a database fails at open (the engine reads the header to convert to WAL), stock fails at the first statement.
