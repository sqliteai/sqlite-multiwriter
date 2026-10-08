> **OBSOLETE — historical document (older than `history-crdt-design.md`).** It describes the first prototype, built inside sqlite-sync (`MULTIWRITER=1` builds, `cloudsync_changes`, the rebase of tracked tables). That engine is no longer this project: metadata is captured in the VFS and kept in the version store, and sqlite-sync is only used for the SQLite amalgamation, LZ4 and the oracle tests. For the current design read `design.md`; numbers and statements below are not current.

# Multi-Writer mode for SQLite (experimental)

> **Status: experimental research branch (`multiwriter`).** Everything here is opt-in and compiled only
> with `make MULTIWRITER=1 ...`; the normal SQLite and PostgreSQL builds are unchanged (their full test
> suites pass with the branch merged). It is a proof of concept whose purpose is to *measure* whether
> the sqlite-sync CRDT engine can turn stock SQLite into a genuinely concurrent multi-writer database.
> The honest short answer, with numbers, is in [§17 Results](#17-benchmark-results) and [§18 Where it wins and loses](#18-where-it-wins-loses-and-stops-scaling).

Contents: 1 Problem · 2 How the existing engine works · 3 Architecture · 4 The VFS and lanes ·
5 Snapshots and epochs · 6 Page-version store · 7 Commit protocol · 8 CRDT rebase · 9 Read dependencies
and consistency · 10 db_version, sync and speculative changes · 11 DDL, VACUUM, ROWID · 12 Durability and
recovery · 13 Compaction · 14 Backup and copying · 15 Configuration and unsupported features ·
16 Build, test, benchmark · 17 Results · 18 Wins/losses · 19 Multi-process · 20 Development notes

## 1. Problem

SQLite allows one writer at a time per database. sqlite-sync already reduces row/column writes to CRDT
changes that merge deterministically in any order. The experiment: reuse that machinery so that many
local connections execute write transactions **concurrently** — against stock SQLite (source unmodified),
with unchanged SQL and APIs — and let conflicts be resolved by the CRDT engine instead of a lock.

Core principle:

> Physical page changes are *speculative execution results*. Logical sqlite-sync changes are the
> authoritative representation of a transaction whenever it has to be rebased.

Every transaction therefore has two representations: the **physical** one (dirty pages produced by
stock SQLite; the fast path) and the **logical** one (sqlite-sync metadata rows / `cloudsync_changes`; the
conflict path). The CRDT rules (CLS/GOS/DWS/AWS, Block-LWW, tombstones, column winners) stay in
`src/cloudsync.c` (`merge_insert()`); nothing in `src/multiwriter/` decides a CRDT outcome.

## 2. How the existing engine works (repository analysis)

### 2.1 Local write path

```
SQL (INSERT/UPDATE/DELETE on a tracked table)
  -> SQLite executes it (B-tree change in the pager cache)
  -> AFTER INSERT/UPDATE/DELETE trigger  ("cloudsync_after_*", database_sqlite.c)
       SELECT cloudsync_insert/update/delete('tbl', pk cols...)              (cloudsync_sqlite.c)
  -> cloudsync_dbversion_next()  : db_version of this transaction              (cloudsync.c)
  -> local_mark_insert_or_update_meta / local_mark_delete_meta
       writes <tbl>_cloudsync: (pk, col_name, col_version, db_version, site_id, seq)
       (+ a (db_version) index <tbl>_cloudsync_db_idx)
  -> changes are *derived on demand* by the cloudsync_changes vtab from <tbl>_cloudsync JOIN the base table
       => (tbl, pk, col_name, col_value, col_version, db_version, site_id, cl, seq)
```

The `BEFORE UPDATE/DELETE` triggers only reject writes while a table is being altered. Column values are
not stored in the metadata: they are read from the base table when a change is emitted. The metadata
tables are ordinary tables, so they travel through the same speculative pages as user data.

### 2.2 Remote apply path

```
INSERT INTO cloudsync_changes / cloudsync_payload_apply()      (cloudsync_changes_sqlite.c, cloudsync.c)
  -> merge_insert()  : decides the winner by causal length (cl), col_version, value, site_id
       -> merge_delete / merge_sentinel_only_insert / merge_did_cid_win
       -> merge_insert_col() (or the pending batch), or block_store_value + block_materialize_column (Block-LWW)
  -> plain SQLite INSERT/UPDATE/DELETE on the base table + <tbl>_cloudsync update (triggers suppressed while `insync`)
```

### 2.3 What `db_version + seq` gives, and where it ends

* `db_version` = `MAX(db_version)` over all `*_cloudsync` tables (recomputed when `PRAGMA data_version`
  changes) `+1` for the first change of a transaction; `seq` restarts at 0 for each db_version. The commit
  hook promotes pending → committed, the rollback hook drops it. **All changes of one SQLite transaction
  share one db_version, ordered by seq.**
* Limits that matter here: (1) it is *per-connection* state — two concurrent lanes both compute `MAX+1`
  from the same snapshot and collide; (2) the send cursor is a db_version watermark, so a lower version
  committing after a higher one was sent would never be sent; (3) `site_id` identifies a *replica* and must
  not be reused as a lane id. Multi-Writer therefore adds an explicit `(tx_id, writer_id, snapshot_epoch,
  commit_epoch)` and unique db_version reservation (§10).
* **Finding (hot page):** every tracked table has a `(db_version)` index, so every tracked write appends
  to the right edge of that index. Concurrent tracked writers therefore *always* conflict physically; all
  of them go through the rebase path. This is the single most important performance fact about tracked
  workloads (§17).

## 3. Architecture

```
   connection A     connection B     connection C           (stock sqlite3 API, stock SQL, stock pager)
        |                |                |
   private lane     private lane     private lane           in-memory "-wal" + private wal-index per connection
        \                |                /                 (SQLite believes it is the only writer: no WAL write lock contention)
         +---- MultiWriter VFS (wraps the platform VFS) ----+
                       |  commit frame = physical write set
                       v
          validate ---- conflict? ---- no: FAST COMMIT ---------------+
                       | yes                                          |
                  CRDT rebase (replay the logical changes at the      |
                  latest snapshot through merge_insert(); stock       |
                  SQLite regenerates the pages) ----------------------+
                                                                      v
           versioned page store  +  durable commit log ("<db>-mw")  ->  visible at a new epoch
                       |
                  compaction: materialise versions into the real database file
```

Files (all under `src/multiwriter/`, none compiled into the normal builds):

| file | role |
|---|---|
| `multiwriter_vfs.c` | wrapper VFS (delegates to the default VFS), event tracing, per-file dispatch |
| `multiwriter_lane.c` | private lanes: in-memory WAL, private wal-index, versioned reads, commit-frame capture, PRAGMA guards |
| `multiwriter_tx.c` | transaction identity, snapshot registry, db_version reservation, DDL barrier |
| `multiwriter_pages.c` | versioned page store, striped locking, commit protocol (validate + install), GC |
| `multiwriter_log.c` | durable commit log, recovery, group commit, in-order visibility, fault injection |
| `multiwriter_compact.c` | compaction / checkpoint, background compactor |
| `multiwriter_rebase.c` | CRDT rebase |
| `multiwriter_db.c` | per-database shared state (registry) |

`SQLITE_EXTRA_INIT` (a *compile option* for `sqlite3.c`; the source is unmodified) registers the VFS at
the end of `sqlite3_initialize()`, i.e. before any `sqlite3_open()`. sqlite-sync itself is auto-loaded on
every new connection through `sqlite3_auto_extension()` when the application enables it
(`mw_vfs_set_autoload_cloudsync(1)`); applications that already load sqlite-sync explicitly need nothing.

Lane mode is selected **per database** through the URI: `file:app.db?mw=2` (`mw=1` = observation only on
the stock WAL, `mw=0` / absent = plain passthrough). `mw_vfs_set_enabled_default(2)` enables it globally.

## 4. The VFS and private lanes

A VFS is bound at `sqlite3_open()`; an extension loaded later cannot replace it. Hence the build-time
registration above. The wrapper delegates everything and intercepts only what Multi-Writer needs.

**Measured** mapping between SQLite's transaction lifecycle and VFS events (`test/multiwriter/mw_vfs_trace.c`,
WAL mode) — these facts, not assumptions, drive the design:

| SQLite event | VFS-visible signature |
|---|---|
| `BEGIN` (deferred) | nothing |
| first read in a txn = **snapshot start** | `xShmLock(SHARED)` on a read-mark slot (>= 3) |
| end of read txn / `COMMIT` | matching `xShmLock(UNLOCK\|SHARED)` = **snapshot end** |
| first write | `xShmLock(slot 0, EXCLUSIVE)`; **no** `xWrite` yet (dirty pages live in the pager cache) |
| `COMMIT` of a write txn | frames appended to `-wal` (`xWrite`), `xSync` (per `synchronous`), write lock released |
| `ROLLBACK` | write lock released, **zero** `xWrite`: speculative state never reaches the VFS |
| `SAVEPOINT` / `ROLLBACK TO` | invisible to the VFS (pager-internal) |
| first write on an empty WAL | read-mark 0 is swapped for a real read-mark *while the write lock is held* (not a snapshot end) |
| `PRAGMA mmap_size>0` | reads via `xFetch`, bypassing `xRead` |
| second concurrent writer | `SQLITE_BUSY` at the write lock — the limit being removed |

**Private lane** (`multiwriter_lane.c`). Each connection gets: (1) a `-wal` file that lives in memory only;
(2) its own wal-index (`xShmMap` memory) with no-op shm locks, so SQLite believes it is the sole writer;
(3) a main-database file whose writes/syncs/truncates are swallowed (only compaction writes the real file)
and whose reads are served from the versioned store at the transaction's snapshot; (4) `xFetch` disabled
(mmap is forced off; `PRAGMA mmap_size` reads back `0`); (5) locks above SHARED virtualised (a lane never
writes the file; real RESERVED/PENDING/EXCLUSIVE locks would also make unrelated connection opens fail
with `SQLITE_BUSY`).

When a snapshot ends, the lane's WAL is emptied and its wal-index header zeroed. SQLite's own recovery path
then rebuilds an empty WAL at the next transaction and reports "changed", which resets the page cache:
**every transaction starts with a cold page cache** (a cost, §18) — and that is what makes the set of pages
fetched through `xRead` a *complete* read set (§9).

The physical write set of a transaction is read back from the private WAL at the **commit frame** (the
frame with a non-zero db-size): the set of distinct page numbers and their final images.

## 5. Snapshots and epochs

* `commit epoch`: a monotonically increasing integer; epoch 1 = the state found on disk.
* Transaction identity (`mw_tx_info`): `tx_id`, `writer_id` (the connection's lane — **not** the
  sqlite-sync `site_id`), `snapshot_epoch`, `commit_epoch`, `schema_generation`, state
  `ACTIVE → PREPARED → COMMITTED | ABORTED`.
* A transaction's snapshot is taken at its first read (the read-mark lock): `snapshot_epoch =` the current
  *visible* epoch, read and registered under one mutex so GC sees either the registration or an older epoch.
* The **snapshot registry** is the list of lanes holding a snapshot; `oldest_active_snapshot` is its
  minimum (or the current epoch when empty). It bounds GC and compaction. It never decreases.

## 6. Page-version store

`(page number, commit epoch) → page image`, as a directory of per-page chains (ascending epoch), no custom
tree, no lock-free framework:

* lookup = binary search for the newest version with `epoch <= snapshot`; else the real file;
* locking is **striped**: the chains of pages with the same `pgno % 256` share one mutex; a reader locks
  one stripe, copies the bytes it needs, unlocks (so GC can never free memory in use);
* per-page bookkeeping is intrusive (`GC candidates`, `dirty since last compaction`): nothing on the hot
  path allocates in a way that can fail after an epoch has been assigned.

**Garbage collection.** A version is reclaimable when a newer version is visible to every snapshot that can
still exist (`epoch <= oldest_active`); if the newest version itself is already materialised in the real file
(`<= compacted_epoch`) and old enough, the whole chain goes. GC runs every N commits (`mw_gc=N`, default 64)
or on demand, works from a detached candidate list one stripe at a time, and reports versions reclaimed /
retained bytes / GC time. Tests pin a reader at epoch 100, advance the database by 1000 commits, run GC and
verify the reader still reads its snapshot; releasing the reader lets the versions go.

## 7. Commit protocol

At the commit frame (inside `xWrite` of the private WAL), `mw_db_publish()`:

1. copies the page images (outside any lock);
2. locks the **stripes of every page written or read** (ascending; overlapping commits share a stripe, so
   their validate+install serialise; disjoint commits run in parallel) — inside the *publication gate*
   (§8) which costs two atomics on the fast path;
3. **validates**: read-set pages, write-set pages and the schema cookie must have no version newer than the
   snapshot epoch;
4. assigns the epoch and the log offset under `seq_mu` (the only serialisation point: a handful of
   instructions), records the file size, installs the new versions **invisibly** (tagged with the epoch,
   `db->epoch` not yet advanced);
5. outside all locks: appends the record to the durable log (§12), fsyncs it if `synchronous>=FULL`
   (group commit), then becomes **visible in epoch order** (epoch E only after E-1);
6. if validation failed: *page conflict* → rebase (§8); *read conflict* / *schema change* / not rebasable →
   `SQLITE_BUSY_SNAPSHOT` (retryable), the transaction is rolled back.

There is no global mutex around transaction execution, page writes, CRDT replay or fsync.

## 8. CRDT rebase

A page-level conflict is not a logical conflict: two writers may modify different rows on one page. When
validation fails on the *write set* only, and the transaction is fully described by sqlite-sync changes:

1. the physical result is discarded;
2. the transaction's logical changes are read through an **overlay view** (its own pages over its snapshot)
   from `cloudsync_changes WHERE db_version IN (the versions it reserved)`;
3. a helper connection at the **latest snapshot** replays them with `INSERT INTO cloudsync_changes`
   (i.e. `merge_insert()` — the same path as a remote apply); stock SQLite regenerates every page (B-tree
   splits, allocation, overflow, indexes, freelist, record encoding);
4. the helper's commit is validated like any other; if it loses a race it retries against the newer
   snapshot. **Fairness:** rebases are serialised per database and, after 3 losses, the *publication gate*
   is closed so the next attempt runs against a frozen state (measured: max attempts fell from hundreds to 4);
5. the original SQL is never re-executed (no repeated side effects); the rebase reuses the logical changes.

A transaction is **rebasable** only if everything it did has a logical representation. It is *not*
rebasable (→ retryable `SQLITE_BUSY_SNAPSHOT`) when it: changed the schema (its page 1 carries a different
schema cookie); wrote a table sqlite-sync does not track, including `sqlite_sequence` (detected with a
`sqlite3_preupdate_hook`, which also sees WITHOUT ROWID tables); is a remote apply (`insync`); reserved no
db_version; or is itself a rebase helper. Row *creation* (sentinel or first-version change) is checked
against the latest snapshot: if a concurrently committed transaction already created the same primary key
the rebase refuses (stock SQLite would have raised a PK violation for the second insert; a silent merge would
lose an insert — this is the auto-assigned-rowid hazard, §11).

**Write-write conflict on one cell (first committer wins).** A column change is computed from the cell's value at the
transaction's snapshot (its new `col_version` is the old one + 1). Before replaying, the rebase looks at the *latest* snapshot: if a concurrently committed
transaction has already advanced the same cell to that version or beyond, the transaction is refused with `SQLITE_BUSY_SNAPSHOT` (retryable)
instead of being replayed. Reason: a blind `UPDATE t SET a='x'` and `UPDATE t SET a=a+1` cannot be told apart at this level, and replaying the second
would silently lose an increment. This was found by the tracked hot-row benchmark: an earlier version replayed such changes (last-writer-wins) and lost ~5% of the
increments while `integrity_check` still passed. Now the benchmark's invariant (`sum(a) == committed transactions`) holds. Consequences: two local
transactions writing *different* cells of a row (or different rows of a page) still merge without an error; two local transactions writing the *same* cell serialise
through a retry. Last-writer-wins on the same cell still applies to changes that arrive from *other replicas* (sync), and Block-LWW cells (per-block) are unaffected.
(Test: `mw_crdt.c`, "same cell", plus a 100-increment counter that must not lose any.)

## 9. Read dependencies and consistency model

**Isolation level: snapshot isolation plus deterministic CRDT merge for write/write conflicts on tracked
tables. It is NOT serializable and must not be described as such.**

* Readers see a stable committed snapshot; a writer sees its own speculative changes.
* Write/write page conflicts are merged by the CRDT (tracked tables) or refused (everything else).
* **Page-level read validation** (default on, `mw_readcheck=0` turns it off): every page fetched through
  `xRead` during the transaction (except page 1) is recorded; a page that was *only read* and has a newer
  version at commit → `SQLITE_BUSY_SNAPSHOT`, never rebased (a replay would not redo the reads). This
  prevents, and the tests demonstrate: stale reads across tables, write skew between different pages, phantoms
  for scanned ranges. It is **conservative**: index/interior pages that others modify cause false-positive
  retries (e.g. a concurrent delete makes a concurrent update-by-key retry instead of merging). Applications
  should retry `SQLITE_BUSY_SNAPSHOT` like any optimistic-concurrency client.
* With `mw_readcheck=0`: plain snapshot isolation + CRDT merge. Write skew and stale-read anomalies are then
  possible (tested and documented in `mw_readdeps.c`).
* Write skew *within one tracked table* (two rows, both transactions write the same table) is merged by the
  CRDT and is not caught by page-level validation when the writes rebase; CRDT convergence does not preserve
  cross-row invariants.
* **Application invariants** (`balance >= 0`, inventory, "exactly one leader", cross-row uniqueness) are
  *not* solved by CRDT merging. SQLite-enforced `UNIQUE`/`CHECK`/`FOREIGN KEY` constraints are enforced
  during execution (against the snapshot) and again during rebase replay; anything else needs application
  logic, retries or future semantic CRDTs.
* Savepoints: `ROLLBACK TO` inside a transaction undoes physical pages and the metadata rows together (same
  transaction), so both representations describe the same effective transaction; the test runs the spec's
  nested-savepoint script and checks that fast-commit and rebase produce identical results.
* Triggers/UDF side effects: executed once, during the original speculative run; a rebase replays logical
  changes and does not re-run SQL.

## 10. db_version, sync, and speculative changes

* **Unique versions.** The first change of a transaction reserves a unique db_version from a per-database
  counter (`cloudsync_dbversion_next`, under `#ifdef CLOUDSYNC_MULTIWRITER`), so concurrent lanes never share
  one and `(db_version, seq)` stays unique.
* **Send ceiling.** Because the send cursor is a watermark, a change may be exported only once no unresolved
  transaction can still publish a lower version: `ceiling = (lowest reservation of any unresolved
  transaction) - 1`. The (few) queries that compute the export window append
  `AND db_version <= cloudsync_send_ceiling()` (a macro that is empty in normal builds).
  Rebased transactions keep the versions they reserved, and hold the ceiling until resolved.
* **Speculative changes never sync.** A lane's uncommitted state is physically invisible to every other
  connection (including the sync code); aborted/rolled-back reservations leave gaps but no rows; committed
  rows above the ceiling are held back. Tests: `mw_capture.c`.
* A long transaction that holds a reservation delays the export of *later* commits (head-of-line blocking of
  sync exports); it does not delay their visibility to readers.
* Remote apply (`cloudsync_payload_apply`) on a lane that conflicts is refused with a retryable error
  (its logical changes carry foreign db_versions; automatic retry inside the network layer is future work).

## 11. DDL, VACUUM, ROWID, AUTOINCREMENT

* **DDL barrier.** A statement-start trace hook recognises `CREATE/DROP/ALTER/REINDEX/VACUUM`, takes the
  per-database schema mutex (one DDL at a time), waits (bounded) for other lanes' in-flight write
  transactions, and refuses *new* write transactions with `SQLITE_BUSY` (so `busy_timeout` waits) until the
  DDL transaction ends. Readers are never blocked. **Correctness never depends on the barrier**: a
  transaction whose snapshot predates a schema change can never publish — the schema cookie (page 1, bytes
  40-43) is validated at commit (and checked fail-fast at the first write). DDL is never merged or rebased;
  a DDL that loses a race fails retryably. `schema_generation` and `last_schema_epoch` are exposed.
* **VACUUM / VACUUM INTO** work through the lane (tested); the rebuild is one big commit at a new epoch,
  concurrent snapshots keep reading the old pages. `auto_vacuum`/`incremental_vacuum` databases are
  **rejected at open** (page relocation is unsupported).
* **INTEGER PRIMARY KEY / AUTOINCREMENT.** Concurrent transactions can allocate the same next rowid from
  their snapshots. Guarantee (test `mw_rowid.c`, 12 threads × 10 inserts, tracked and untracked tables,
  `RETURNING`, `last_insert_rowid()`, rollback): an id returned for a *successfully committed* insert is never
  reused and never merged with another insert; the loser gets a retryable error and its retry gets a fresh
  id. `sqlite_sequence` is a hot untracked page, so AUTOINCREMENT tables serialise through retries and are
  never rebased. This does **not** change SQLite's semantics (no ID ranges); for high concurrency prefer
  explicit UUID/TEXT keys (§17).

## 12. Durability and recovery

Committed state = real database file + committed page versions, so every commit is first appended to the
**commit log** `<db>-mw` (checksummed records, contiguous epochs, salted header with the compacted
`base_epoch`, the file is `flock()`ed so a second Multi-Writer process is refused).

Ordering: the commit is validated and installed *invisibly*; its record is written; if `synchronous>=FULL`
(observed from the connection's `PRAGMA synchronous`; default FULL) it is fsynced (group commit: an fsync
that starts after a record was written covers it); only then does it become visible, and only after the
previous epoch did. **A commit is never visible before its record is persisted per the requested level.**
An I/O error before visibility aborts that commit cleanly when nothing was assigned after it; otherwise (and
after an fsync failure, whose outcome is uncertain) the database is *failed* (sticky) and recovers by
reopening. `synchronous=NORMAL/OFF` behave like SQLite's: the record is written to the OS, not fsynced.
(macOS `fsync` does not flush the drive cache; benchmarks note this.)

**Recovery** (at first open): replay the longest valid, epoch-contiguous prefix of records past
`base_epoch`, ignore and truncate a torn tail. Tested with deterministic crash points (`_exit(9)` at the
instant) — *before the log write, mid-write (torn record), after the durable write but before publication,
right after publication, in compaction after the pages, in compaction after the base* — and with `SIGKILL`
under load with a concurrent compactor. Invariants checked after each: every acknowledged commit present, no
transaction half applied, `integrity_check` ok, usable afterwards, ordinary SQLite file after a clean close.
Injectable faults: log write error, log fsync error, allocation failure.

## 13. Compaction (checkpoint)

Materialises versions into the real file so it becomes an ordinary database again. Target
`T = min(visible epoch, oldest active snapshot)` (no reader can need an overwritten page). Crash-safe
order: (1) write the newest version ≤ T of every dirty page, resize the file, fsync; (2) durably record
`base_epoch = T` in the log header; (3) publish `compacted_epoch = T` (lets GC free versions), and reset or
**rewrite the log tail** so it stays bounded under continuous load. A background thread per database runs
it when the log outgrows `mw_log_max_mb` (default 32 MB) and, if `mw_compact_ms>0`, periodically;
`MW_FCNTL_COMPACT` runs it on demand; the last connection to close compacts everything and removes the log.
It never takes part in publication. Writers are throttled (0.5 ms sleeps, counted) only if the log exceeds
16× the threshold (a long-lived reader pins the target). Metrics: backlog (commits not yet materialised),
pages written, duration.

## 14. Backup, copying, unaware processes

* `sqlite3_backup_*` and `VACUUM INTO` read through the lane and therefore produce the **logical committed
  database** (tested).
* A **raw copy of `app.db` alone is not the committed database** until compaction: the files that define the
  active database are `app.db` and `app.db-mw` (the log). Safe procedure: `MW_FCNTL_COMPACT` (or close all
  connections), then copy `app.db`. Or use the backup API / `VACUUM INTO`.
* A process that opens `app.db` with an unaware stock SQLite runtime sees the state as of the last
  compaction and must not write it. Not detected (a stock connection ignores the log); documented as
  unsupported. The `-mw` file lock only protects against a second Multi-Writer process.

## 15. Configuration and unsupported features

| | |
|---|---|
| URI `mw=2` | private lanes (`mw=1` observe only, `mw=0` off) |
| `mw_gc=N` | GC every N commits (default 64, 0 = manual) |
| `mw_readcheck=0` | disable page-level read validation |
| `mw_compact_ms=N` | periodic compaction (default: only when the log outgrows `mw_log_max_mb`) |
| `mw_log_max_mb=N` | log size that triggers compaction (default 32) |
| `PRAGMA synchronous` | observed per connection; FULL (default) fsyncs each commit batch |

**Journal modes:** WAL only. `journal_mode=DELETE/TRUNCATE/PERSIST/MEMORY/OFF` return an error (never silently
run); new/rollback-mode files are converted to WAL at open. **Rejected:** `locking_mode=EXCLUSIVE`,
`auto_vacuum≠NONE` (pragma and existing databases), shared-cache URIs. **mmap:** forced off.
**Not supported / not tested:** in-memory databases (they simply bypass the VFS), network filesystems
(SQLite's own locking caveats; nothing here was tested off local disks), WASM, SQLCipher stacking, ATTACH
across lane databases, concurrent schema changes, multiple Multi-Writer processes (§19).
**Extensions:** FTS5 and RTree (ordinary shadow tables) work and are tested; concurrent writers to one FTS5
index conflict physically and are refused (retryable), never merged. `sqlite-vector`, `sqlite-ai`,
`sqlite-memory` are outside this repository and were **not** tested; any virtual table with external side
effects is unsupported by rebase (its writes have no logical representation).

## 16. Build, test, benchmark

```
make mw-unittest     # the existing SQLite unit + regression suites with the Multi-Writer VFS as default (passthrough)
make mw-test         # 23 Multi-Writer test programs (test/multiwriter/mw_*.c): lifecycle, identity, lanes, store, GC,
                     # fast commit, capture, rebase, CRDT semantics, read dependencies, structural, ROWID, savepoints,
                     # DDL, durability/crash recovery, compaction, compatibility, offline sync interoperability, multi-process
make mw-bench        # builds dist/mw/mw_bench
sh test/multiwriter/bench/run_matrix.sh out.csv 3 1 && python3 test/multiwriter/bench/summarize.py out.csv
make unittest        # normal build: unchanged and green
```
Objects go to `build/mw`, binaries to `dist/mw`. Fault injection: `mw_fault_arm()`.

**Benchmark methodology.** `mw_bench` compares (A) stock SQLite (rollback journal), (B) stock WAL and (C)
Multi-Writer, with the same PRAGMAs (printed, read back from a connection, for every run), warm-up, fixed
measurement window, fixed seeds, ≥ 2-3 repetitions (medians reported). *Agents* are independent connections;
they are multiplexed over `min(agents, 2×cores)` threads (one OS thread per agent would measure the
scheduler, not SQLite) while all 1000 connection states stay alive. Each run validates its data (workload
invariant + `PRAGMA integrity_check`); an INVALID run is discarded. Reported: tx/s, reads/s, writes/s,
p50/p95/p99/max latency, busy/retries, fast commits, page/read conflicts, rebases and retries, pages/commit,
fsync time, rebase time, versions/retained bytes/reclaimed/GC time, compaction backlog, CPU, peak RSS,
database/WAL/log size.


## 17. Benchmark results

Machine: Apple silicon, 18 cores, macOS, SQLite 3.45.3, local SSD, 3 s measurement after 1 s warm-up, median of 2-3 repetitions
(fixed seeds), page size 4096, `mmap_size=0`, `journal_mode=WAL` (stock: DELETE for the "stock" column). *Every* reported run passed its
data validation and `PRAGMA integrity_check` (invalid runs are discarded by `summarize.py`). Raw data:
`test/multiwriter/bench/results/` (CSV + summaries). **Stock WAL rows use `busy_timeout=60000` (writers queue on the single write lock); the failure
table uses the SQLite default (`busy_timeout=0`).** `synchronous=off` is a CPU-bound comparison (MultiWriter still writes its durable log, without fsync).

| workload | stock WAL tx/s at 1 / 10 / 100 / 1000 agents | MultiWriter tx/s | MW / WAL |
|---|---|---|---|
| independent writes (own row/page) | 214k / 214k / 168k / 139k | 272k / 219k / 216k / 162k | 1.27x / 1.02x / 1.29x / 1.17x |
| read-only (random PK) | 719k / 887k / 998k / 600k | 1,192k / 782k / 695k / 673k | 1.66x / **0.88x / 0.70x** / 1.12x |
| mixed 80% reads / 20% writes | 514k / 401k / 304k / 216k | 657k / 466k / 424k / 311k | 1.28x / 1.16x / 1.40x / 1.44x |
| long transactions (5 reads + 1 write) | - / 15k / 1k / - | 139k / 124k / 109k | 1.09x / 8.2x / 125x |
| unique TEXT-key insert (untracked) | 129k / 132k / 83k | 126k / 106k / 70k | 0.98x / **0.81x / 0.84x** |
| INTEGER PRIMARY KEY insert | 232k / 238k / 150k | 242k / 191k / 129k | 1.04x / **0.80x / 0.86x** |
| AUTOINCREMENT insert | 153k / 153k / 109k | 156k / 128k / 88k | 1.02x / **0.84x / 0.81x** |
| **sqlite-sync, same page** (rows of one table) | 63k / 64k / 48k | 61k / 48k / 34k | 0.97x / **0.76x / 0.72x** |
| **sqlite-sync, different columns of shared rows** | 64k / 60k / 47k | 60k / 48k / 34k | 0.94x / **0.79x / 0.73x** |
| **sqlite-sync, same column of one row** (blind writes; same-cell conflicts refused and retried) | 66k / 67k / 49k / 43k | 64k / 52k / 40k / 35k | 0.96x / **0.77x / 0.83x / 0.81x** |
| **sqlite-sync, 4 hot rows, `UPDATE ct SET a=a+1` (true conflicts)** | 67k / 65k / 48k / 42k | 63k / 49k / 38k / 34k | 0.94x / **0.76x / 0.79x / 0.79x** |
| **sqlite-sync, unique inserts** | 51k / 48k / 34k | 45k / 31k / 21k | 0.89x / **0.65x / 0.60x** |
| independent, long-lived reader pinned (100) | 58k | 55k | 0.95x |

`synchronous=full` (independent writes; **macOS `fsync` does not flush the drive cache, so absolute values are optimistic for both; the ratio is what
matters**): stock WAL 10.9k / 10.9k / 11.3k / 11.8k tx/s at 1/10/100/1000 agents; MultiWriter 38k / 64k / 76k / 71k = **3.5x / 5.9x / 6.7x / 6.0x**,
because concurrent commits share a group fsync of the log.

### Concurrent writes that fail with the SQLite default (`busy_timeout=0`, no retry)

Attempts that surface an error to the application (`SQLITE_BUSY`, `SQLITE_LOCKED`, `SQLITE_BUSY_SNAPSHOT`) out of all write attempts in 3 s,
`synchronous=off`. Rates are per *attempt* and failing attempts are cheap, so compare the successful commits too.

| workload | agents | stock (rollback) failed | stock WAL failed | MultiWriter failed | stock WAL commits | MultiWriter commits |
|---|---:|---:|---:|---:|---:|---:|
| independent | 10 | 100% | 91.2% | **0%** | 64.9k | 653.3k |
| independent | 100 | 100% | 99.4% | **0%** | 12.0k | 649.7k |
| independent | 1000 | 100% | 98.3% | **0%** | 1.8k | 486.3k |
| tracked, same page | 10 | 99.99% | 92.6% | **0%** | 47.1k | 144.6k |
| tracked, same page | 100 | 100% | 99.3% | **0%** | 9.7k | 100.6k |
| tracked, 4 hot rows (true conflict) | 10 | 100% | 92.7% | 5.6% | 61.1k | 197.7k |
| tracked, 4 hot rows | 100 | 100% | 99.4% | 4.0% | 10.5k | 154.0k |
| tracked, 4 hot rows | 1000 | 100% | 97.8% | 4.0% | 5.2k | 139.2k |

Non-conflicting writers never fail; same-page conflicts on sqlite-sync tables are resolved by the rebase (0 refused). **True conflicts are not eliminated:**
optimistic execution cannot re-run the application's SQL, so the *first* attempt of a transaction on a hot row can still be refused
(`SQLITE_BUSY_SNAPSHOT`, retryable). Mitigation shipped: a lane that conflicted serialises its next 16 transactions behind a per-database turn (bounded 20 ms
wait; waiting by *sleeping* was the best of three variants measured: spin and condition-variable turns were 40-45% slower on appends and hot rows). It turned
60-75% refused attempts into ~4-6% and raised committed hot-row throughput 3-30x over stock WAL without retries (10 agents: 3.2x, 100: 14.7x, 1000: 27x); the rest is retried by the application. The hot-row workload is on a **tracked** table (an untracked hot-row workload was removed: it exercised only the raw page-conflict path and is not the use case; its old numbers, ~7% refused, remain in `matrix_final.csv`/`failure_table_final.txt` as history). Raw data: `matrix_hot_tracked.csv`, `failure_table_hot_tracked.txt`.

### With application-level retry (no transaction may fail)

`mw_bench --busy-ms 0 --retry 1000000`: every refused transaction (`SQLITE_BUSY`/`LOCKED`/`BUSY_SNAPSHOT`, or a PK collision on a retried insert) is re-run by the
"application" after a jittered exponential backoff (5 us doubling to 2 ms) until it commits; nothing else is different. Both engines use `busy_timeout=0`, so every
conflict shows up as a retry. `synchronous=off`, 3 s, all 46 runs valid (`integrity_check` + workload invariant) and **0 transactions gave up in any run**.
Latency includes the retries. Raw: `retry_matrix.csv/.txt`.

| workload | agents | WAL tx/s | MW tx/s | WAL p99 us | MW p99 us | WAL retries (max for one tx) | MW retries (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| independent | 10 | 201k | 217k | 45 | 80 | 35k (37) | **0** (0) |
| independent | 100 | 92k | 212k | 10,930 | 897 | 154k (40) | **0** (0) |
| independent | 1000 | 70k | 166k | 16,028 | 1,183 | 164k (54) | **0** (0) |
| tracked, 4 hot rows (`a=a+1`) | 10 | 50k | 51k | 1,465 | 1,957 | 69k (46) | 7.9k (5) |
| tracked, 4 hot rows | 100 | 38k | 36k | 31,789 | 5,715 | 146k (77) | 4.4k (2) |
| tracked, 4 hot rows | 1000 | 32k | 31k | 35,612 | 6,038 | 157k (77) | 3.8k (2) |
| tracked, one shared cell | 10 / 100 / 1000 | 53k / 39k / 33k | 53k / 37k / 32k | 1.3k / 31k / 36k | 2.2k / 5.6k / 5.8k | 75k / 146k / 152k | 14k / 8k / 6.5k (7 / 4 / 2) |
| tracked, rows of one page | 10 / 100 | 53k / 39k | 48k / 34k | 1.2k / 31k | 2.0k / 6.1k | 79k / 149k (86) | **0** / **0** |
| tracked, different columns | 10 / 100 | 51k / 39k | 48k / 34k | 1.4k / 30k | 2.0k / 6.0k | 67k / 143k (99) | **0** / **0** |
| tracked, unique inserts | 10 / 100 | 42k / 25k | 31k / 21k | 0.1k / 50k | 5.5k / 11.6k | 13k / 61k (163) | 0.5k / 0.4k (5) |
| untracked TEXT-key insert | 10 / 100 | 130k / 61k | 118k / 67k | 47 / 16.9k | 31 / 5.8k | 18k / 88k | 13k / 23k (112 / 7) |
| INTEGER PRIMARY KEY insert | 10 / 100 | 205k / 107k | 222k / 122k | 12 / 8.7k | 22 / 3.5k | 19k / 170k | 27k / 43k (42 / 10) |
| AUTOINCREMENT insert | 10 / 100 | 150k / 88k | 138k / 83k | 16 / 9.1k | 45 / 5.2k | 15k / 101k | 20k / 31k (51 / 9) |
| long transactions (5 reads + 1 write) | 10 / 100 | 127k / 71k | 124k / 107k | 16 / 13.6k | 1.4k / 3.5k | 10k / 73k (102 / 75) | 33k / 14k (6 / 3) |

Reading it: with retry both engines complete every transaction, so the difference is the *cost of getting there*. Where writers touch different pages or different cells
MultiWriter needs no retries at all and keeps the tail latency 5-12x lower at 100-1000 agents (independent p99 0.9-1.2 ms vs 11-16 ms). On true conflicts (hot rows, one cell) it
needs 10-40x fewer retries and has 5-6x lower p99, but its throughput is at best equal to and up to ~8% below stock WAL's (which serialises writers and lets the
queue absorb the conflict). Not shown as a win: the append workloads at 10 agents (a few percent slower, p99 comparable), and the tracked unique inserts (0.7-0.83x).
Bounded retry counts are small (max 2-112 per transaction; stock WAL: up to 163), so a retry limit of a few hundred never triggers on these workloads, but
**the guarantee is "no failure with retry", not "no retry"**: an application must still have the retry loop, and an unbounded loop can starve under pathological contention.

### Multi-process (`mw_mp=1`), P processes x A agents on one database, `synchronous=off`

| workload | processes x agents | stock WAL tx/s | MultiWriter tx/s | MW / WAL |
|---|---|---:|---:|---:|
| independent | 1 x 4 / 2 x 4 / 4 x 4 / 8 x 4 / 8 x 16 | 209k / 210k / 208k / 207k / 206k | 204k / 202k / 155k / 85k / 80k | 0.98x / 0.96x / 0.75x / **0.41x / 0.39x** |

`synchronous=full`, independent writes (one run each): 1 x 4: 4.1x, 4 x 4: 1.7x, 8 x 4: 1.9x over stock WAL. Multi-process throughput *falls* beyond ~4 processes
(see §19: publication is one short critical section shared by all processes and every process applies every commit).

### What the profiling found and what was done (in order)

Measured with `sample` on `mw_bench`; every step kept all tests green.

1. Single store rwlock: >90% of blocked time. → striped mutexes, validate+install atomic per stripe set (10 agents: 75k → 134k tx/s).
2. Log checksum (byte-wise FNV over 4 KB) was the largest CPU cost of a small commit. → 64-bit word mixer.
3. `pread` of clean pages of the real file on every (cold-cache) transaction. → bounded epoch-0 base-page cache.
4. `pwrite` of the commit record (a syscall per commit, serialising on the inode). → the log is a mapped file; the record is a `memcpy` (header last).
5. The global SQLite allocator mutex (memory accounting) and the store's use of it. → store uses `malloc`; the benchmark disables `SQLITE_CONFIG_MEMSTATUS`.
6. Visibility hand-off (epoch E visible only after E-1; futex wake-ups cost 10-20 us). → spin briefly, wake the successor only if it parked. A "combining"
   variant was tried and **rejected** (spinners oversubscribed the cores: 18.8k tx/s at 100 agents).
7. **Rebase helpers committed at `synchronous=FULL` even when the application set `synchronous=off`** (a bug: each rebased commit did an msync/fsync). Fixed: the helper
   inherits the origin's level. Tracked tables at 10 agents: same page 16k → 41k, unique inserts 14k → 31k tx/s. *(This also means the tracked-table numbers in
   the first published matrix were pessimistic.)*
8. Hot-spot serialisation after a conflict (also after a *successful rebase*, so the next transactions run on the fast path instead of being replayed).
Net effect on independent writes: 94k/125k/105k/100k → 272k/219k/216k/162k tx/s (1/10/100/1000 agents).

Bugs found by the tests along the way (all fixed): log-header magic mismatch after a tail rewrite (lost commits after `SIGKILL`); compaction truncating the real file from a
stale size record (corrupted a multi-process database); tail rewrite starting after the wrong record; a deadlock between the fairness gate and the publication lock;
`flock`/`fcntl` interaction on macOS; a process-global checksum salt shared between databases.

## 18. Where it wins, loses, and stops scaling

**Wins**
* Writers that touch different pages: no lock, no `SQLITE_BUSY`; above stock WAL at every agent count at `synchronous=off` (1.0-1.3x) and 3.5-6.7x at
  `synchronous=full` (group fsync).
* Long transactions (8x-125x over stock WAL, whose lock upgrades turn into `SQLITE_BUSY_SNAPSHOT` storms), mixed read/write (1.2-1.4x), reads at 1 and 1000 agents.
* Same-page conflicts on sqlite-sync tables are resolved logically (0 refused writes) with column-level merge.

**Loses**
* **Tracked tables under concurrency are 25-40% slower than stock WAL** (about 34-48k vs 47-64k tx/s at 10-100 agents; unique inserts 0.6-0.65x) and have
  millisecond tail latency. Every tracked write appends to the `(db_version)` index of its metadata table, so concurrent tracked writers *always* conflict
  physically and pay a rebase or a serialised turn. Dropping that index was tried: it did not help (and hurt inserts), so the hot index is not the whole story.
* Untracked appends with concurrent writers (0.80-0.86x): the same pages are refused/retried or serialised. Hot rows of a tracked table (0.76-0.79x at 10-1000 agents) and one shared cell (0.77-0.83x): every conflict costs a rebase attempt, a refusal and a retry.
* Read-only at 10-100 agents (0.70-0.88x): every transaction starts with a cold page cache and copies pages through the store.
* Fixed per-transaction overhead (cold cache, wal-index recovery, page copies, log record) is larger than stock's for tiny transactions.
* True conflicts on hot rows still refuse ~4-6% of first attempts (retryable), and page-level read validation produces false-positive `SQLITE_BUSY_SNAPSHOT` (§9).

**Stops scaling** where the serial parts dominate: `seq_mu` (epoch assignment, tiny), in-order visibility (a hand-off chain per commit), the log, the rebase itself,
and in multi-process mode the shared publication critical section. Throughput is roughly flat between 10 and 1000 agents on this 18-core machine: CPU/serialisation
bound, not lock bound.

## 19. Multi-process mode (`mw_mp=1`)

Several processes, each with several connections, on one database file. Design: the shared commit log is the sequencer; `<db>-mwlock` is an mmapped shared
header (committed epoch, log end, compaction target, db_version counter, schema epoch, DDL owner, publication lock) plus a registry of *slots* (one per lane: pid,
snapshot epoch, lowest unresolved db_version, "writing") and *processes*. Each process keeps its own in-memory page store and brings it up to date by tailing the log.

* **Commit:** apply others' commits (outside the lock, then the last few inside) → validate against the complete local store → assign the next epoch → install and
  append the record → publish `committed_epoch/log_end` → release the lock → `fsync` (if `synchronous>=FULL`) → return. Publication is serialised across *all* processes;
  execution, reads, validation prep, rebases are parallel. The publication lock is an atomic flag in the shared header with the holder's pid (dead holders are stolen).
* **Snapshots** catch up first, then register their epoch in a slot; a compactor announces its target, fences and re-scans, so a snapshot can never fall below it.
* **Visibility vs durability (deliberate, documented):** a record becomes visible to other processes when appended and before its fsync; the committing call returns
  only after an fsync that started after the write, which also covers every earlier record of the file. A power failure can lose a commit another process has *read*,
  never one that was acknowledged.
* **db_version:** unique across processes (shared counter, lower bound published in the slot *before* taking the number); the send ceiling is the minimum over all live
  slots, so exports stay correct with several writer processes.
* **Compaction:** one compactor at a time (fcntl lock); target = min over every live process's snapshots; the real file size comes from page 1's header at the target epoch;
  the log is bounded by resetting it when everybody is caught up or by **rewriting its tail** into a new file (readers notice `log_gen`, reopen and reposition; a process that
  lagged behind the new base flushes its store and resyncs from the real file).
* **DDL:** an exclusive schema barrier across processes (one DDL owner, other processes' new writers get `SQLITE_BUSY`, in-flight writers are drained); correctness still rests on the schema cookie.
* **Crashes:** a killed process loses only its uncommitted work; its slots are recognised with `kill(pid,0)` and ignored; its lock is stolen. If every process dies, the next
  opener recovers from the log like a single process. `flock` on the log keeps a non-multi-process opener out.
* **Tested** (`mw_multiproc.c`, `mw_multiproc_crash.c`): 4 processes x 4 threads on disjoint rows (32,000 commits), a shared counter with no lost update, tracked inserts
  with unique db_versions across processes, `SIGKILL` of one process while the others continue, snapshot isolation across processes, log/lock files removed and a stock-readable file
  after the last close, *all* processes killed at once under load four times in a row (every acknowledged commit survives), and `CREATE INDEX` from one process while three others insert.
* **Limits:** throughput falls beyond ~4 processes (8 x 4: 0.41x of stock WAL at `synchronous=off`) because each process applies every commit and publication is one critical
  section: prefer fewer processes with more threads. All processes must open the database with `mw_mp=1` (a mismatch is refused). macOS `flock`/`fcntl` semantics were
  observed and worked around; **Linux was not tested**. `mw_mp` uses more memory (each process holds its own versions).

## 20. Development notes, assumptions and open risks

* Branch `multiwriter`, one commit per phase (0-21 done). SQLite source untouched: all interception is via the VFS, compile options (`SQLITE_EXTRA_INIT`, FTS5/RTree/
  preupdate hook for the test build) and public APIs. The normal builds gain only `#ifdef CLOUDSYNC_MULTIWRITER` hooks and one macro that is empty
  (`CLOUDSYNC_SEND_CEILING_SQL`); the normal SQLite suite passes unchanged.
* Assumptions: POSIX (pthreads, `flock`, `fcntl`, `mmap`); local filesystem; WAL; mmap off; cold page cache per transaction.
* **Not tested:** Linux, Windows, the PostgreSQL build and sync against PostgreSQL / SQLite Cloud (offline payload exchange between a stock replica and lane databases *is* tested),
  the network layer (`cloudsync_network_*`) on lane databases (tests build with `CLOUDSYNC_OMIT_NETWORK`), sqlite-vector/ai/memory, network filesystems, databases larger than RAM (the store
  and base-page cache are in memory, bounded but not tuned), `synchronous=full` on a drive that honours flushes.
* **Known gaps:** remote `cloudsync_payload_apply` that conflicts with local writers is refused (retryable) instead of retried internally; several processes syncing the same
  database would contend on the sync cursor rows (one syncing process per database is the safe setup); `SQLITE_BUSY_SNAPSHOT` must be retried by applications; a sticky "failed" state
  after an fsync error until reopen; rebase of very large transactions is memory-bound; long-held reservations delay sync export; read validation is page-granular; tracked-table concurrency is slower
  than stock (§18); `mw_multiproc_crash` failed rarely and unreproducibly (a `CREATE INDEX` under three inserting processes returned `SQLITE_IOERR`; seen twice, never in 12 consecutive reruns with or without the latest changes): the cross-process DDL path has an unexplained race.
* Next steps: internal retry for remote apply and single-syncer coordination (then a roundtrip against a real backend), cut the per-rebase cost, a warm-cache mode, zero-copy log
  application for multi-process, Linux and PostgreSQL runs.

## 21. Code review pass (four independent reviewers, every finding re-verified)

Four read-only reviewers went through the store/log/compaction, lane/transaction/VFS, rebase/core hooks and multi-process code. Each finding was checked
against the code (and where possible reproduced with a test) before any change; findings that were speculative or wrong were dropped.

**Fixed (each with a test run; new tests marked *):**
* *Lost update through the cell check* (`multiwriter_rebase.c`): a transaction that updated one cell twice raised its `col_version` by 4 while a concurrent single increment raised it by 2,
  so the "first committer wins" comparison passed and an increment was lost (`c=2` instead of 3, reproduced). The check now compares the cell's current version with its version **at the
  transaction's snapshot**, read through a view without the transaction's pages. * `mw_crdt` (double update). Delete-wins is kept (a cell that vanished because its row was deleted is not a conflict).
* *Several commits inside one read snapshot* (a `SELECT` still stepping while the same connection updates): the second commit always failed with `SQLITE_BUSY_SNAPSHOT` because its write set
  contained the first commit's pages (1 of 5 updates succeeded, reproduced). The write set now starts after the previous commit's frames and pages this snapshot committed itself are validated
  against its own commit epoch. Such later commits are not rebased. * `mw_selfcommit`.
* *Stale real `-wal`*: frames left by stock use were invisible to the lane store, and the compactor could later overwrite the main file under them (a stock connection replaying that WAL would corrupt
  newer pages). The WAL is now checkpointed at open. * `mw_stalewal` (note: the test passes with the fix; I did not run it against the unfixed code).
* *Log `msync` watermark*: a record reserved before but completed after the watermark advanced was never `msync`'d (matters on Darwin). Late-completed records now lower the watermark;
  the range, mapping and fd are captured once per sync.
* *Log file/mapping replaced under a sync leader* (`rewrite_tail`, `reopen`): the leader could `msync` an unmapped range or `fsync` a closed fd and mark a healthy database failed. The swap now excludes the leader.
  `remap_ro` no longer unmaps (the mapping always spans 1 GiB), `reserve_space` no longer unmaps under in-flight appenders (records past the mapping are `pwrite`n).
* *Runtime install without the stripe lock* (multi-process catch-up reallocating a chain a reader walks); *NOMEM after the record was written* (the "failed" commit would have been resurrected by recovery: the
  header is now destroyed); *recovery sizing an allocation from an unverified header*.
* *Multi-process*: dead-slot reclaim could run twice and wipe a live slot's snapshot; opening after the last closer's `unlink` could attach to a deleted lock file (two "first" processes on one database);
  an initialiser that failed left joiners with `SQLITE_CORRUPT` (they now retry); the joining process now registers and remembers the log generation **before** opening the log.
* *Others*: two first opens could start two compactor threads; `PRAGMA journal_mode=WAL` on a new database had no busy timeout; read-set tracking that ran out of memory silently disabled validation (now refuses the commit);
  rebase helpers are reopened after any schema change; any rebase failure now marks the transaction aborted; sentinel detection compared with the wrong value (the `"__[RIP]__"` col_name, never NULL); unchecked
  `sqlite3_value_dup`, failed `helper_open` left a handle, ignored `step` errors in the collision checks, PK-collision counter over-counted.
* Write-set construction was **quadratic** (one 50,000-page transaction: measured 8.5 s at 256k rows before, 136-145 ms for 50k rows after; hash lookup instead of a linear scan).

**Performance changes that did not move the numbers (kept, measured ±2%):** per-thread event counters instead of shared atomics, ctz walk of the stripe bitmap, stack arrays for small commits,
cached prepared statements in the rebase, a compiler-only barrier in `xShmBarrier`, read-only transactions spending hot-spot credit.
**Tried and reverted:** advancing visibility over all completed epochs at once (+3% at 10 agents, -23% at 100, -25% at 1000: the same result as the earlier "combining" experiment).

**What the profile says.** Independent writes at 100 agents: 21.4k of 23.9k samples are threads parked in `mw_db_make_visible` (each epoch is made visible only after the previous one, one thread wake-up per hop, ~4.5 us): that
chain is the ceiling (~220k tx/s here). Tracked tables: 15.4k of 17.7k samples are in `nanosleep`, i.e. the per-database hot-spot turn; every tracked write touches the same `db_version` index page, so tracked
writes are effectively serialised (48k tx/s vs 64k for stock WAL's single writer). Neither is a micro-optimisation problem.

**Fixed in a second pass (test `mw_multiproc_liveness`):**
* liveness is now a kernel-released lock, not a `kill(pid, 0)` probe: every process holds an fcntl lock on its own byte of `<db>-mwlk`; zombies, reused pids and other pid namespaces are handled
  (a zombie that pinned a snapshot no longer holds compaction back: measured `backlog=0`);
* the log generation and end offset are one atomic word (`log_pos`), so a reset/rewrite is a single store; a process that steals the publication lock re-checks that the log file is still the one the header
  describes (a rewriter killed right after `rename` is recovered: crash point `MW_CRASH_LOG_RENAME`);
* fork-safe registry: databases inherited through `fork()` are marked orphaned (a child never reuses or tears down the parent's state; its own connections get fresh state). A child must not use connections
  inherited from the parent. (The hang this fixed: the child inherited "compactor running" without the thread, the log never shrank and every commit was throttled.)

**Known and not fixed (verified, deferred):**
* the `ftruncate`-grown log can `SIGBUS` on ENOSPC instead of returning `SQLITE_FULL`;
* `mw_log_rewrite_tail` and the header scan run under `seq_mu`; compaction writes pages one `pwrite` at a time; GC re-walks pinned candidates; `list_mu` is one lock for every dirty/candidate list push;
  the multi-process group commit is per process (no shared `durable_epoch`); false sharing in the shared header; unbounded `sched_yield` spin in the publication lock;
* memory: version chains never shrink, the read-set bitmap costs one byte per page per lane, the WAL buffer and wal-index regions of a lane keep the size of its largest transaction; under a sustained
  bulk-insert load the versions retained by the store grew to the size of the database (observed once: 262 MB retained, 316 MB RSS on a 260 MB database, `synchronous=full`, 8 writers);
* rebase memory: 9 heap values per change; an application that installs its own `sqlite3_preupdate_hook` replaces sqlite-sync's, so untracked writes in a transaction would no longer be detected (documented, not enforced);
* `mw_db_release` runs the last-close work under the process-wide registry mutex; an unchecked `malloc` in `mw_db_acquire`; a mode mismatch is reported as `SQLITE_NOMEM`;
* the helper-reopen-after-schema-change fix has no dedicated test.

## 22. Comparison with Turso 0.8 (BEGIN CONCURRENT)

> **Correction (see §25):** every open-loop *latency* number in §22-§24, for all three engines, had a floor of about 10 ms at p99 caused by the benchmark harness (macOS timer coalescing of sleeping threads), not by the engines. Corrected latency numbers are in §25. The throughput numbers are not affected.

> The Multi-Writer columns and the statements about Multi-Writer's losses in this section are the *first* measurement, before the work described in §23; see §23 for the current numbers. The Turso and SQLite columns are unchanged.

Turso 0.8 ([announcement](https://turso.tech/blog/turso-0.8.0)) targets the same problem, concurrent writers on an SQLite-compatible database, with an MVCC engine written in Rust. This section runs the two workload shapes that
post describes on the same machine as everything else in this document. **The benchmark code is my own, written from the description in the post; Turso's own benchmark suite was not used.**

*Workloads* (shapes taken from the post): every transaction inserts 100 rows on disjoint keys (one `INSERT` of 100 tuples, 64-byte text payload); `synchronous=FULL` for all engines.
Throughput: N connections, closed loop, 1-64 connections. Latency: Poisson arrivals at 1000 tx/s in total over N connections, latency measured from the *scheduled* arrival to the commit (so waiting for a lock counts).
*Engines:* stock SQLite 3.45.3 (rollback journal and WAL, `busy_timeout` 60 s), Multi-Writer (private lanes, retry until commit), Turso 0.8.1 through its native Rust crate (`turso` 0.8.1, `journal_mode=mvcc`, one connection per task,
retry on conflict; `test/multiwriter/bench/turso_rs`). Mean of 3 runs (3 s after 1 s warm-up for throughput; 10 s for latency); every run passed its data check and `integrity_check`.
Raw data: `results/turso_compare.jsonl`; driver `run_turso_compare.py`, `run_turso_latency_variants.py`.

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

**What this shows.**
* **Throughput (this workload): Turso scales, Multi-Writer does not.** From 4 connections up Turso commits 1.7x-4.7x the transactions of stock WAL (43k tx/s = 4.3 million rows/s at 32-64 connections); stock SQLite stays flat around 9-12k tx/s
  and Multi-Writer at **0.72-0.91x of stock WAL** (about 8% of its commits are refused by a page conflict on the shared b-tree and retried). Turso is *slower* with a single connection (0.45x of WAL). Multi-Writer's private-lane design does
  not help when every writer appends to the same table: they all touch the same interior pages, so the commits conflict physically, and each commit also copies and logs its pages. That is the opposite of the independent-rows workloads earlier in this document.
* **Closed-loop tail latency:** Turso has the best p99/p99.9 from 8 connections up (1-7.7 ms); stock WAL has p99.9 of 0.16-1 s and Multi-Writer 11-62 ms.
* **Latency at 1000 tx/s:** Multi-Writer is best at 8-32 connections (p99 5.6-11 ms, p99.9 7.6-14 ms; stock WAL 39-351 ms / 92-878 ms). **Turso's p99 is 45-71 ms** in this setup, higher than the post reports (p99 1.7 ms, p99.9 5.9 ms on their Linux/NVMe
  machine). The cause is a periodic stall that scales with the volume written: with 1 row per transaction the same test gives p99 2.3 ms, with 10 rows p99 3.2 ms (p99.9 38 ms), with 100 rows p99 46 ms. I did not investigate further; it looks like the
  MVCC checkpoint, and it may depend on the filesystem and on Turso's checkpoint thresholds. The median waits differ by how the harness sleeps between arrivals (tokio timer 1.5 ms, blocking sleep 0.8 ms, busy-wait 0.13 ms at 1 connection);
  the blocking sleep is used as the reference because it matches `mw_bench`'s `nanosleep`; busy-waiting is unusable with 8 connections (the spinning threads starve Turso's own workers: p99 0.9 s).
* Turso's own claim (p99.9 500x lower than SQLite, 7x throughput) is consistent in direction for *throughput and closed-loop tails*; the absolute numbers here differ because the ratios depend on the cost of `fsync` (see below).

**Caveats, so these numbers are not over-read.**
* **Different machine and versions from the post:** Apple silicon / macOS here, Ryzen / NVMe / XFS / Linux there; Turso 0.8.1 here (0.8.0 there); SQLite 3.45.3 here (3.50.2 there). Absolute numbers are not comparable with the post's.
* **`fsync` on macOS does not flush the drive cache**, so `synchronous=FULL` is cheap for stock SQLite and Multi-Writer (70-100 us per commit; stock WAL reaches 10k+ tx/s where the post's SQLite reaches 1.4k). That makes stock SQLite look better than on Linux and shrinks
  the gap Turso shows in the post (4.7x here, 7x there). Whether Turso issues a full flush on macOS I did not verify (its single-connection commit of 0.17 ms suggests it does not).
* The Python binding of Turso (`pyturso`) does not scale with threads even for `SELECT 1` (331k -> 79k ops/s from 1 to 8 threads), so it was **not** used; the Rust crate was. The Python driver (`bench_turso.py`) remains for calibration only.
* Multi-Writer ran untracked tables here (no sqlite-sync CRDT): the point of this section is the engine, not the CRDT layer. Multi-Writer retried refused transactions until they committed; none failed.
* The workload is Turso's best case (disjoint keys, all inserts). It says nothing about hot rows, tracked tables or long transactions, where the results earlier in this document apply.

## 23. Why Multi-Writer lost the bulk-insert workload, and what was done

**Hypothesis (wrong):** concurrent inserts conflict on interior b-tree pages. **Measured** (`MW_DEBUG`, 8 writers, `synchronous=full`): of ~9,000 write conflicts, only ~90 were on an interior page. The conflicts were
* **page 1** (the database header: file change counter, in-header size, version-valid-for): every commit that extends the file rewrites it; and
* **the new pages themselves**: two transactions that start from the same snapshot both extend the file and allocate the same page numbers (`dbsize + 1, ...`), although what they wrote is disjoint.

Stock SQLite avoids this only by running one writer at a time. Also found while profiling (each one measured before and after):
1. **Memory leak on appends** (`multiwriter_pages.c`, `multiwriter_compact.c`): a page written exactly once has one version and never became a GC candidate (candidates were queued when a *second* version arrived), so
   every appended page stayed in memory forever. Retained versions: 586 MB on a 260 MB database, RSS 316 MB -> 2.2 MB retained, RSS 66 MB; single-writer throughput +14% (fewer allocations).
2. **The hot-spot turn hurt untracked writers.** After a refusal a lane ran its next 16 transactions serialised behind a per-database turn (a sleeping poll). That pays where a conflict is expensive (a tracked transaction that loses
   costs a whole rebase) and costs more than it saves for untracked ones (a refusal is just a retry): 32 writers spent 19.4k of 21.2k profile samples asleep in `mw_lane_snapshot_begin`. Now only tracked (rebasable) lanes, and lanes
   that were refused 16 times in a row, get the turn; read conflicts (an interior page read on the way down changed) do not grant it. Effect (sync off, 100 writers): `insert-int` 143k -> 205k tx/s, `insert-uuid` 76k -> 113k, `insert-autoinc` 95k -> 135k;
   the tracked hot-row / same-page / CRDT-insert workloads are unchanged (49-51k / 37k / 22-30k tx/s). Without the 16-refusals rule the worst transaction retried 505 times; with it 125.
3. **Page relocation** (new `multiwriter_reloc.c`): a commit that conflicts *only* because other commits extended the file is saved instead of refused. Its new pages are renumbered above the current end of the file, every reference
   to them is rewritten (child and right-most pointers of interior pages, overflow pointers in cells, the `next` pointer of an overflow page), and page 1 is merged (latest page 1, change counter + 1, size extended); then it is
   published through the normal validation. It refuses, as before, unless: page 1 changed only in those three fields (no freelist or schema change); the new pages are exactly the contiguous range past the snapshot's size and avoid the lock-byte page;
   no page of the snapshot conflicts; every new page is referenced exactly once (a reference the parser missed would show up as a page with no reference, so an incomplete parse cannot go unnoticed); the schema cookie is unchanged.
   Relocations are serialised by a mutex that is released right after the pages are installed (before the log fsync, so group commit is intact). Tests (`mw_reloc.c`): plain leaf splits, multi-page rows, overflow chains of 30 KB rows,
   a secondary index, real conflicts still refused, and an 8-thread random workload (row sizes from 0 to 25 KB, an index, deletes that change the freelist) ending in `integrity_check` and row counts, also on the compacted file read by a stock connection.

**Result** (this machine, 3 runs, every run validated; Multi-Writer retries refused transactions until they commit):

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

Multi-Writer went from 0.72-0.91x of stock WAL to 0.93x with one writer and **1.16-1.63x from 8 to 64 writers**; it is still **2.9-3.7x behind Turso** at 8-64 connections (Turso 4.3M rows/s at 32-64 connections).

**What is not fixed / limits.**
* Two writers: 0.80x of stock WAL (not investigated). The open-loop p99 at 8-16 connections got *worse* (5.6 -> 10.3 ms and 6.2 -> 10.8 ms; better at 32) and I did not find why; both are still 6-12x better than stock WAL and 6x better than Turso's 45-70 ms stalls.
* Relocation is physical and deliberately narrow: it does not apply when the transaction freed pages (freelist change), changed the schema, wrote pages that existed in the snapshot and were changed by somebody else, or when this connection already committed inside the same read snapshot.
  Refusals that remain: real write-write conflicts and **read conflicts** (an interior page read on the way down changed: ~7% of commits with 8 writers). §24 shows that almost all of those were avoidable (the change did not affect the path the transaction took) and removes them.
* The relocation code parses SQLite's b-tree page format (types 0x02/0x05/0x0a/0x0d, cell layout, overflow-pointer rule). A format assumption that is wrong would corrupt a database; the safeguards above bound the risk but it is new physical-format code, and the fuzz test covers table and index b-trees, overflow chains and the freelist, not every feature (`WITHOUT ROWID`, `auto_vacuum` is unsupported anyway, encrypted pages, non-default reserved bytes were not exercised).
* `mw_reloc` failed twice (out of about 30 runs) right after a rebuild, once with 6 failures, at a moment when the second commit of the first sub-test was refused; it did not fail in 550 further runs (one process, repeated in-process, under load). I could not reproduce it and the cause is unknown.
* A measurement warning: for about an hour of this session four orphaned test processes (a killed `mw_multiproc_crash` whose children survived) consumed most of the machine; everything measured in that window was discarded and repeated.

## 24. Investigation: why Multi-Writer is at 0.80x with 2 connections and behind Turso from 8 connections up

Method: the same bulk-insert workload as §22/§23 (100 rows per transaction, disjoint keys, `synchronous=full`), on a machine with nothing else running (checked with `ps`), instrumented with a new per-stage timer (`MW_TIMING=1`: time, calls, calls slower than 0.5 ms, worst call, for the publish path, log append, log sync, visibility, relocation, the hot-spot turn and its waits), `sample` profiles, and A/B switches (`mw_noroute`, `mw_hot_credit`, `mw_noreloc`, `MW_BENCH_URI_EXTRA`).

### Two connections (0.80x of stock WAL)

* The number is not stable: the same run gives 5.8k-13.6k tx/s (mean ~9-10k) while stock WAL gives 10.7-11.5k every time. Two states alternate: p95 latency 0.23 ms (fast runs) or 1.1 ms (slow runs).
* **Not** the cause (measured): the retry backoff (`nanosleep(5 us)` takes 11 us here), the stripe locks (0.1 us per commit), the relocation mutex, the visibility chain (0.1 us), or a wrong conflict policy (only ~4% of attempts are refused).
* **The log append.** Appending a ~25 KB record through the `mmap`ped log costs 19-41 us in fast runs and 78-111 us in slow ones, and 1-2% (10% in the slowest run) of the appends exceed 0.5 ms (worst 6 ms): first touches of fresh pages of a sparse file, competing with another writer's `msync` of the same mapping.
  Together with the log sync (52 us) this is most of a 2-writer commit, and two writers cannot overlap it as well as stock SQLite's single writer overlaps its own.
* **The compactor.** Disabling compaction (`mw_log_max_mb=3000`) lifts the 2-writer mean from ~10.8k to ~12.8k tx/s: its `fsync` of the real file competes with the log's `fsync`. It does not remove the slow state.
* Current number (3-run mean): 9.5k vs 11.0k = 0.87x. With one writer the same measurement gives 9.7k vs 12.6k (0.77x) although an earlier 3-run mean was 11.8k: single-writer numbers on this machine move by +-20% between runs of identical code.

### From 8 connections up

`synchronous=off` separates the durable-write path from everything else (tx/s; Turso with `PRAGMA synchronous=OFF` for the same test):

| | 8 connections | 32 connections |
|---|---:|---:|
| Multi-Writer, `synchronous=off` | **40.8k** | 27.9k |
| Turso, `synchronous=off` | 31.7k | 45.3k |
| SQLite WAL, `synchronous=off` | 14.3k | 12.6k |
| Multi-Writer, `synchronous=full` | 19.1k | 17.7k |
| Turso, `synchronous=full` | 28.5k | 43.3k |
| SQLite WAL, `synchronous=full` | 9.9k | 9.5k |

At 8 connections Multi-Writer is *faster* than Turso when nobody waits for the disk, and halves under `FULL`, where Turso barely moves: the gap there is the durable-write path. At 32 connections there is a second, CPU-side gap (27.9k vs 45.3k without fsync).
Three causes were found and fixed, one was characterised and not fixed:

1. **Wasted work: one hot page.** With 8 writers 31% of the attempts were refused, with 32 writers 59%; **98.5% of the read conflicts were on page 12, the root of the table's b-tree** (every insert reads it; ~17% of publish attempts rewrite it). Page-level read
   validation refused every transaction in flight each time the root gained a divider, although none of them depended on it. New `multiwriter_btree.c`: a *read* of an interior page that has a newer version is still valid if the newer version lists every child the transaction went through
   (pages it read or wrote) with the same divider keys on both sides (the cells are sorted, so the neighbours bound the key interval, and the child pages are validated as pages of their own). If the transaction used no child, the page changed type, or a child moved, it is a conflict as before.
   Effect (8 writers): read conflicts 28,219 -> 59; refused attempts 31% -> 9%. (32 writers: refused 59% -> 14%.) Correctness evidence: the bulk runs validate every row and `integrity_check`; `mw_reloc` test 5 (8 threads inserting and deleting blocks of rows in their own ranges, so pages merge and redistribute under concurrent descents) checks
   exact counts per thread and `integrity_check`, 26 consecutive runs, 143 reads saved per run.
2. **A bug in the hot-spot turn.** A lane granted the turn spends one credit per successful commit, but only the direct commit path did; commits saved by relocation (63% of all commits) or rebase never did, so a lane that was granted the turn once stayed serialised for ever: with 32 writers 87% of all thread time was spent waiting for the turn (110 s of 128 thread-seconds, 8.5 ms per wait).
   Fixed (all success paths spend credit). Together with the routing validation, 32 writers went from 12-14k to 19.1k tx/s.
3. **The durable-write path (characterised, not fixed).** At `FULL` a commit writes ~7 pages (~28 KB: physical page images) into the log, then a group `msync` + `fsync`. Per stage, 8 writers: log append 66 us, log sync 146 us average (2% over 0.5 ms), against a logical log of ~10 KB per commit in Turso.
   Experiments (3 s, `FULL`): appending with `pwrite` and syncing with `fsync` only (no `mmap` memcpy, no `msync`): 8 writers 24.5k vs 14.7k, 32 writers 21.2k vs 18.5k, **but 2 writers 11.0k vs 13.3k** and an append of 74-196 us; preallocating the log's blocks (`F_PREALLOCATE`) made every variant 2-10x slower. Neither was adopted.
   The design that could give both is a leader-batched write: committers copy their record into a warm in-memory staging buffer at their offset and the group-commit leader writes the whole group with one `pwrite` and one `fsync` (single-process, `FULL` only; `NORMAL`/`OFF` keep the mapping so a killed process still leaves its commits in the file). Not implemented.
4. **CPU side beyond 8 writers (not investigated further).** 32 threads on 18 cores use 500-860% of a core; per commit: a cold page cache (the b-tree path is re-read through the store), page copies (WAL frame -> store copy -> log), the relocation mutex (16 us average wait at 32), visibility hand-off (31 us). Turso does row-level MVCC with a logical log and a warm cache.

### Result (closed loop, tx/s, mean of 3 runs, every run valid)

| connections | SQLite WAL | Multi-Writer (first measurement) | + relocation, turn policy (§23) | + interior-page routing, credit fix (this section) | Turso 0.8.1 | MW now / WAL | Turso / MW now |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 12598 | 9398 | 11832 | 9664 | 5779 | 0.77x | 0.60x |
| 2 | 10954 | 9020 | 8595 | 9485 | 8559 | 0.87x | 0.90x |
| 4 | 10246 | 8893 | 10177 | 14822 | 17434 | 1.45x | 1.18x |
| 8 | 10082 | 8535 | 16399 | 16194 | 28756 | 1.61x | 1.78x |
| 16 | 9531 | 8917 | 13509 | 19804 | 31883 | 2.08x | 1.61x |
| 32 | 9684 | 7592 | 11627 | 19376 | 43362 | 2.00x | 2.24x |
| 64 | 9255 | 6645 | 10770 | 17958 | 43446 | 1.94x | 2.42x |

Multi-Writer went from 0.72-0.91x (first measurement) to **1.45-2.08x of stock WAL from 4 to 64 connections**, and the gap to Turso at 32-64 connections from 4.5x to **2.2-2.4x** (at 8 connections 1.8x, at 16 connections 1.6x); Turso is still 1.6-2.4x ahead from 8 connections up. With 1-2 connections Multi-Writer remains 0.77-0.87x of stock WAL.
Latency at 1000 tx/s (Poisson arrivals): p99 10.3 / 10.6 / 11.0 ms at 8 / 16 / 32 connections (stock WAL 56 / 164 / 368 ms; Turso 45-70 ms, see §22); the flat ~10 ms is not explained (the compactor's `fsync` is the candidate).
The `MW_TIMING` output is the tool to continue with: `MW_TIMING=1 mw_bench ...` prints the per-stage table at exit.

## 25. Leader-batched log writes with a staging buffer, and a correction of the latency benchmark

### What was built (`multiwriter_log.c`)

Single process, `synchronous >= FULL` (the mapped log stays for `OFF`/`NORMAL`, multi-process mode and every other case, where it measured better). Every commit owns a byte range of the log file (assigned under `seq_mu`). Instead of copying its record into the `mmap`ped file and
letting the group leader `msync` + `fsync` it, the committer
* waits only if the 8 MB **staging ring** is full (a ring slot is reusable once its bytes are in the file); a record larger than a quarter of the ring is written to the file directly, in order;
* copies its record into the ring at `(file offset mod ring size)` and computes the checksum from the sources with a streaming version of the same mixer, so **the on-disk format is unchanged** and recovery is untouched;
* joins the **contiguous prefix** of finished records (`written_end`); the first committer whose record is inside the prefix and finds no sync running becomes the leader: one `pwritev` of the prefix (two iovecs when the ring wraps) and one `fsync`, then everybody whose record is inside is released.
A commit from a connection with `synchronous=OFF` in a staged database is staged too and flushed to the file (without fsync) immediately, so a killed process still leaves it in the file. The log is no longer extended ahead of time with `ftruncate` in staged mode (see below). Compaction and tail rewrite first make every assigned record reach the file; every place that moves the log offset
(open, recovery, reset, rewrite) resets the staging counters. `MW_LOG_STAGING_OFF=1` restores the mapped log (A/B switch).

**Two things I measured on the way (3 s, `FULL`):** extending the log with `ftruncate` and then writing into the hole makes the file system allocate blocks inside every write: group sync 70-290 us and 2 writers 5-11k tx/s, against 40-50 us and 17k when the file simply grows. `F_PREALLOCATE` made every variant 2-10x slower.

**Tests** (`mw_stagedlog.c`; the whole suite, 25 programs, passes; `mw_durability` and `mw_stagedlog` 8 consecutive runs each): records of 6 and 9 MB (direct path) interleaved with staged ones, and reopened; 8 committers producing 61 MB of log through the 8 MB ring (it wraps 7 times; 1,908 syncs for 2,000 commits) with exact row counts and lengths;
`SIGKILL` under load with a `FULL` and an `OFF` connection committing at the same time, four rounds: every commit acknowledged by the `FULL` connection was recovered, `integrity_check` ok. The existing crash points (torn record, before/after the durable write, mid-compaction) and the injected write/sync errors run through the staged path too.
Not tested: power loss (nothing here can), Linux, Windows, a staged database that is also opened by a multi-process participant (staging is off when `mw_mp=1`).

### Result (bulk INSERT, 100 rows per transaction, `synchronous=full`, mean of 3 runs, every run valid)

### Throughput

| connections | SQLite WAL | Multi-Writer (§24) | **Multi-Writer, staged log** | Turso 0.8.1 | MW now / WAL | MW now / Turso |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 12598 | 9664 | 11861 | 5779 | 0.94x | 2.05x |
| 2 | 10954 | 9485 | 14244 | 8559 | 1.30x | 1.66x |
| 4 | 10246 | 14822 | 22049 | 17434 | 2.15x | 1.26x |
| 8 | 10082 | 16194 | 29427 | 28756 | 2.92x | 1.02x |
| 16 | 9531 | 19804 | 28127 | 31883 | 2.95x | 0.88x |
| 32 | 9684 | 19376 | 23978 | 43362 | 2.48x | 0.55x |
| 64 | 9255 | 17958 | 22012 | 43446 | 2.38x | 0.51x |

### Latency at 1000 tx/s (p99 / p99.9, ms; real-time wake-up)

| connections | SQLite WAL | Multi-Writer | Turso 0.8.1 |
|---:|---:|---:|---:|
| 1 | 3.41 / 4.50 | 0.83 / 1.74 | 47.58 / 51.11 |
| 8 | 8.55 / 29.74 | 0.73 / 2.21 | 60.31 / 64.70 |
| 16 | 8.72 / 33.45 | 0.68 / 1.41 | 63.17 / 68.84 |
| 32 | 8.47 / 31.80 | 0.67 / 1.75 | 64.87 / 71.40 |

Multi-Writer goes from 0.77-0.87x of stock WAL with 1-2 connections to **0.94x and 1.30x**, from 1.6-2.1x to **2.4-2.9x** from 4 connections up, and it is now **ahead of Turso from 1 to 8 connections (1.02x at 8) and behind from 16 up (0.88x at 16, 0.51-0.55x at 32-64)**. The append stage went from 34-120 us to 4-9 us per commit; group sync 40-50 us with 2 writers.
What limits it beyond 16 connections was not investigated again (§24 item 4: cold page cache per transaction, page copies, the relocation mutex, the visibility hand-off).

### Correction: the latency benchmark was measuring the harness

In the open-loop test every agent sleeps until its next scheduled arrival. On macOS the timers of idle threads are coalesced: an isolated test of nothing but `nanosleep` gives a wake-up lateness of p50 1.9 ms, p95 7.8 ms, **p99 10.0 ms**, which is exactly the "flat ~10 ms" I could not explain in §24 (it was reported as transaction latency, for all engines).
A real-time time-constraint thread policy removes it (p99 35 us in the isolated test; used for the agent threads of the latency test only, in `mw_bench` and in the Turso harness). Corrected results (p99 / p99.9 in ms, mean of 3 runs of 10 s, Poisson arrivals at 1000 tx/s in total):

| connections | SQLite WAL | Multi-Writer | Turso 0.8.1 |
|---:|---:|---:|---:|
| 1 | 3.41 / 4.50 | 0.83 / 1.74 | 47.58 / 51.11 |
| 8 | 8.55 / 29.74 | 0.73 / 2.21 | 60.31 / 64.70 |
| 16 | 8.72 / 33.45 | 0.68 / 1.41 | 63.17 / 68.84 |
| 32 | 8.47 / 31.80 | 0.67 / 1.75 | 64.87 / 71.40 |

Multi-Writer has the lowest tail (p99 0.7-0.8 ms). Stock WAL is 4-10x worse (p99.9 up to 33 ms: the queue behind the single writer). **Turso's p99 of 47-65 ms is real**: it does not change with the wake-up policy and it grows with the volume written per transaction (§22: 1 row per transaction p99 2.3 ms, 100 rows p99 46 ms); I did not find its cause. The p99 figures of §22-§24 for Multi-Writer (10-11 ms) and stock WAL (56-368 ms) were inflated by the harness and are superseded.

## 26. Investigation: why Multi-Writer trails Turso above 16 connections

Method: the same bulk-insert workload (100 rows per transaction, disjoint keys, `synchronous=full`), 32 and 64 connections, `sample` profiles classified by what each thread is blocked on and by the first engine frame above the blocking call, the per-stage timer (`MW_TIMING=1`, now also with the relocation mutex's hold time),
and controlled experiments (thread count, `synchronous=off`, mutex removed, spinning mutex). Turso was measured with its native Rust crate and a varying number of runtime worker threads (`TOKIO_WORKER_THREADS`).

**1. The two engines scale differently with threads.** Turso 32 connections: 2 workers 13.2k, 4 workers 23.1k, 8 workers 28.4k, 12 workers 34.2k, **18 workers 42.9k tx/s** (nearly linear, CPU-bound). Multi-Writer, 32 agents, `synchronous=off`: 4 threads 32.6k, **8 threads 36.7k**, 12 threads 32.7k, 18 threads 31.0k, 32 threads 29.9k (`full`: 22.2k / 30.6k / 30.3k / 25.3k / 25.3k).
At equal thread counts up to 8 Multi-Writer is at or above Turso (30.6k vs 28.4k at 8); it stops scaling at about 8 threads and then loses to contention, while Turso goes on using all cores. Part of the fall-off with one OS thread per agent (32-64 threads on 18 cores) is also lock-holder preemption in *my harness* (Turso runs 18 worker threads and multiplexes tasks): 32 agents on 12 threads are ~10% faster than on 32 threads.

**2. Where the threads wait (32 threads, `full`, share of all profile samples).** 44.6% blocked on mutexes at the start of the investigation; by first engine frame: the relocation mutex 23%, **SQLite's page cache allocator 11%** (`pcache1Alloc`/`pcache1Free`), page reads from the store 4.5% (stripe locks of hot pages), `seq_mu` at transaction start 2.3%; 19% sleeping (`nanosleep`), 14% in condition waits (group commit).

**3. SQLite's global page-cache statistics mutex (fixed, no SQLite source change).** With `SQLITE_DEFAULT` settings `pcache1Alloc` and `pcache1Free` take one process-wide static mutex on every page allocation and free, only to update overflow statistics. Multi-Writer starts every transaction with a cold page cache, so every transaction frees and re-allocates its pages and all threads meet on that mutex.
Compiling SQLite with `-DSQLITE_DISABLE_PAGECACHE_OVERFLOW_STATS` (added to the multiwriter build in the Makefile; **applications that embed SQLite for Multi-Writer should use it too**) moved 32 agents on 18 threads from 25.3k to 36.2k tx/s (+43%), 32 threads with `full` 25.3k -> 33.3k, with `off` 29.9k -> 36.2k.

**4. The relocation mutex (restructured, gain small; the limit is inherent).** After (3) it is 45% of all thread time at 32 threads. Measured: acquisitions 216,700 in 4 s (1.77 per commit: refused attempts also queue), hold 12.8 us (5.7 us with 8 threads), utilisation 69% (24% with 8 threads), mean wait 277 us.
There is a feedback loop: the longer the queue, the longer between a transaction's snapshot and its commit, the more write-write conflicts on existing pages (refused attempts 28.9% at 32 threads, 7.9% at 8), and every refused attempt runs again and queues again. Removing the mutex made it worse (21k vs 26k), a spinning acquire made no difference (30-33k in every setting). What was done: the private page copies and the pointer parse are now prepared *outside* the mutex (with the delta known at that moment; every rewritten reference is recorded and corrected under the mutex if the file grew in between);
the store *adopts* the prepared images instead of copying them a second time inside the critical section; real conflicts on existing pages are detected before queueing. Effect on 32 threads: about +3% (31.5k -> 32.1-33.2k). The end of the file is a serialisation point by construction: every commit that extends it must see the previous one's extension.

**5. What still refuses transactions at 32 threads (not fixed).** Of the conflicts on pages that existed in the snapshot, 14,840 of ~21,000 events in a 3 s run were on one page (page 36, an upper interior page of the table that lists many agents' key runs) and a few hundred each on other early interior pages: concurrent leaf splits under the same parent both rewrite the parent. Page-level validation refuses the second one;
a three-way merge of the parent's cells (the transactions added different children) would resolve most of them. Not implemented.

**6. Durable-write path at 8 threads.** With 32 agents on 8 threads 44% of the thread time is spent waiting for the group commit (mean 98 us): threads are blocked while their fsync is in flight, so throughput is `threads / (execution + wait)`; more threads hide it, and then contention (2-4) takes over. Turso's commit is asynchronous: a worker moves on to another transaction while an fsync is pending.

**Side finding, fixed: the unexplained multi-process test failure.** `mw_multiproc_crash` failed about once in 25 runs (`CREATE INDEX ... disk I/O error`). Cause: a lagging process that detects a log reset reads `(generation, end)` **before** reopening the log; the compactor may already have written the new base into the log header but not yet restarted the log; the process restores its state from the real file, restarts reading at offset 64 with the stale `end`,
finds an old record there (epoch 6547 after 6575), `SQLITE_CORRUPT`, database marked failed. Now the position after a restore is "the first record newer than the base" (not offset 64) and `end` is re-read after the reopen (with a generation check). 200 consecutive runs without a failure (previously ~1 in 25).

### Result (closed loop, tx/s, mean of 3 runs, every run valid)

| connections | SQLite WAL | Multi-Writer (§25) | **Multi-Writer now** | Turso 0.8.1 | MW now / WAL | MW now / Turso |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 12598 | 11861 | 11870 | 5779 | 0.94x | 2.05x |
| 2 | 10954 | 14244 | 15687 | 8559 | 1.43x | 1.83x |
| 4 | 10246 | 22049 | 23131 | 17434 | 2.26x | 1.33x |
| 8 | 10082 | 29427 | 30975 | 28756 | 3.07x | 1.08x |
| 16 | 9531 | 28127 | 36758 | 31883 | 3.86x | 1.15x |
| 32 | 9684 | 23978 | 32583 | 43362 | 3.36x | 0.75x |
| 64 | 9255 | 22012 | 30704 | 43446 | 3.32x | 0.71x |

Multi-Writer is ahead of Turso up to 16 connections (1.15x at 16) and behind at 32-64 (0.71-0.75x); with 12 threads instead of one per agent it reaches 34.5k (32 agents) and 33.5k (64 agents). The remaining gap is a scaling limit (Turso: linear in cores, Multi-Writer: flat above ~8 threads), from the serialisation at the end of the file (4), refusals on shared interior pages (5) and synchronous commits (6).

## 27. Three-way merge of shared interior pages

### Why
§26 item 5: with 32 threads 14,840 of ~21,000 conflicts on pages that existed in the snapshot were on one interior page (the parent of leaves that many agents were splitting): every leaf split adds a divider to the parent, so two transactions that split *different* children of the same parent
both rewrite it, and page-level validation refuses the second one although their changes are disjoint. At 32 threads 28.9% of the attempts were refused, and each refused attempt ran again and queued again for the relocation mutex.

### What (`multiwriter_btree.c`: `mw_interior_merge`, `multiwriter_reloc.c`)
An interior *table* b-tree page is a sorted list of `(child, upper bound)` cells plus the right-most child (no upper bound). When a page of the write set has a newer committed version and is such a page, the relocation path merges it three-way from the version at the snapshot (base), the transaction's image (ours, its own new children already renumbered) and the newest committed version (theirs):
* a child is *changed* by a side if its upper bound differs from the base's; *new* if the base does not list it; the merged page lists every child of `ours` with `theirs`' upper bound where only `theirs` changed it, plus the children new in `theirs`;
* **it refuses** (the transaction is refused and retried as before) if a child is changed by both sides, if a child of the base is missing on either side, if the merged keys are not strictly increasing with exactly one right-most child, or if the cells do not fit the page (the parent would have to split). Leaf pages, index interior pages and anything that is not a table interior page are never merged: a real conflict.
* The children are pages of their own: a child both transactions modified is a conflict on that page and is refused by the normal validation.
* Each merged page is validated at publication against the epoch of the newest version it was merged with (per-page "own epoch" in `mw_validate`); if another commit touched it in between, the publication conflicts and the relocation loop reads the latest again. The merge runs under the relocation mutex, together with the page-1 merge.
The merge applies in the relocation path (a commit that extends the file); a commit that does not extend the file and hits a conflict on a shared parent is still refused. `mw_nomerge=1` in the URI turns it off (A/B switch); `merges` in the database statistics counts merged pages.

### Tests
`mw_reloc` test 6: three transactions from the same snapshot each insert 40 rows of 300 bytes into far-apart regions of a table whose root interior page is the parent of all leaves; committed in both orders, the second and third are merged (2 merges each), rows and order verified, `integrity_check` also on the compacted file; two transactions splitting the *same* leaf: the second is refused. The 8-thread random test (index, overflow chains, deletes) reports 126-129 merges and passes; the routing test 2-4.
The **whole `mw_*` suite ran under AddressSanitizer** for the files that go through the publication path (`mw_reloc`, `mw_lanes`, `mw_fastcommit`, `mw_rebase`, `mw_capture`, `mw_compact`, `mw_crdt`, `mw_readdeps`, `mw_stagedlog`): no errors after the two fixes below. Not tested: index interior pages (never merged), multi-process mode with merges (the suite passes, no dedicated test), page sizes other than 4 KB.

**Bugs found with AddressSanitizer and fixed:** (1) a use-after-free that existed since the relocation was added (§23): `lane_publish` freed the read-set array right after the first publication attempt while the relocation passed the same array to its second publication (the memory happened to stay readable; it corrupted the process in the run that first changed the heap layout);
(2) an out-of-bounds counter index in my diagnostic code. Also fixed on the way: the number of pages to merge was counted in one pass and filled in another while other commits changed the heads (a heap overflow in the same run).

### Result (closed loop, bulk insert, `synchronous=full`, mean of 3 runs, every run valid)

| connections | SQLite WAL | Multi-Writer (§26) | **Multi-Writer, with merge** | Turso 0.8.1 | MW now / WAL | MW now / Turso | p99 ms (§26 -> now) |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 12598 | 11870 | 11792 | 5779 | 0.94x | 2.04x | 0.1 -> 0.1 |
| 2 | 10954 | 15687 | 17391 | 8559 | 1.59x | 2.03x | 0.4 -> 0.2 |
| 4 | 10246 | 23131 | 25517 | 17434 | 2.49x | 1.46x | 0.3 -> 0.3 |
| 8 | 10082 | 30975 | 35038 | 28756 | 3.48x | 1.22x | 0.6 -> 0.4 |
| 16 | 9531 | 36758 | 37239 | 31883 | 3.91x | 1.17x | 2.8 -> 2.7 |
| 32 | 9684 | 32583 | 34749 | 43362 | 3.59x | 0.80x | 7.6 -> 3.8 |
| 64 | 9255 | 30704 | 33754 | 43446 | 3.65x | 0.78x | 13.3 -> 4.4 |

A/B on the same build (`mw_nomerge=1` vs merging, tx/s): 8 connections 32.9k -> 34.3k (+4%), 16: 35.1k -> 37.9k (+8%), 32: 30.0k -> 34.4k (+15%), 64: 28.5k -> 33.5k (+17%). At 32 connections refused attempts went from 21.9% to 0.2% (284 of 140,528). Multi-Writer is now ahead of Turso up to 16 connections (1.17x) and at 0.78-0.80x at 32-64.

### What limits it now
The relocation mutex is saturated: at 32 connections 45,500 acquisitions per second, 17.6 us each (5.7 us with 8 connections: preemption and cache misses of the holder under 32 runnable threads), utilisation ~80%, mean wait 429 us. The merge itself is 11.8 us per call and 6% of the hold time. Everything else in the hold is the publication up to the install (stripe locks, validation, epoch assignment, chain install).
Refused attempts are no longer the problem; the serialisation at the end of the file is.

## 28. Shorter relocation critical section

### Where the time was (bulk insert, `synchronous=full`, per-stage timer `MW_TIMING=1`, mean per acquisition)
The relocation mutex (§26 item 4) was held 5.7 us with 8 connections and **17.6 us with 32**, 45,500 acquisitions/s, utilisation 80%, mean wait 429 us. Inside the hold, with 32 connections (8 connections in brackets), us:
page 1 read 5.4 (0.7), preparation 1.5 (0.4), stripe locks + reserve 3.5 (0.7), validation 1.7 (0.4), the `seq_mu` section 2.7 (0.6), install 2.9 (0.8). Every stage was 4-8x slower under 32 threads, not because it did more work but because **every contended acquisition of a pthread mutex sleeps in the kernel and a wake-up costs tens of microseconds on macOS**:
the store's mutexes (stripes, `seq_mu`, `list_mu`) are held for fractions of a microsecond and contended by all the other transactions' page reads and publications.

### What was done
1. **Spinning acquisition for the store's locks** (`mw_spinlock`, `multiwriter_internal.h`): try-lock and spin up to 50 us (`MW_SPIN_US`, 0 = never), then block. Used for the stripe locks, `seq_mu`, `list_mu` and the relocation mutex. This is what moved the numbers (below); the spin is bounded by time, not by iterations (a failed try-lock takes 4.5 ns on a quiet cache line and much longer on a contended one).
2. Page 1 handed over between relocations: the installing relocation leaves its page-1 image and epoch (`db->p1_cache`); the next one uses it if `p1_head_epoch` says nobody wrote page 1 since, instead of taking page 1's stripe and copying it (5.4 us -> 0.3 us in that stage, but the wait simply moved to the stripe acquisition: no throughput effect by itself).
3. Batched list pushes: pages to add to the GC / dirty lists are chained locally and spliced in with one lock of `list_mu` per commit (they were ~14 acquisitions of a process-wide mutex per commit).
4. The database size at a snapshot is read without `seq_mu` (a ring of the last 1024 epochs; fallback to the locked search), and cached per snapshot in the lane (`xFileSize` runs several times per transaction).
Items 2-4 removed shared traffic but had **no measurable throughput effect on their own** (the hold stayed at 18 us until the spinning acquisition was added); they are kept because they are small and remove work from the critical path, not because they were measured to help.

### Result
Relocation mutex, mean per acquisition (us), spin off -> on: 8 connections wait 2.1 -> 0.7, held 5.0 -> 4.0; **32 connections wait 437 -> 11, held 18.4 -> 9.1** (tx/s 33.9k -> 51.4k); **64 connections wait 563 -> 16, held 18.9 -> 10.1** (31.9k -> 49.9k). The mutex is no longer a bottleneck: 0.3% of the acquisitions wait more than 0.5 ms (27.6% before).
No regression in the other workloads (3 s, 10 and 100 agents, spin off -> on, tx/s): independent 222k -> 229k / 218k -> 221k, same-page tracked 48.0k -> 47.9k / 37.5k -> 37.9k, hot rows 50.0k -> 50.3k / 39.2k -> 39.3k, CRDT inserts 30.4k -> 30.3k / 22.5k -> 22.7k, INTEGER-key appends 234k -> 240k / 203k -> 218k, TEXT-key appends 124k -> 126k / 110k -> 114k;
and gains where reads and short transactions dominate: **mixed 80/20 591k -> 745k (10) and 537k -> 709k (100), read-only 712k -> 1,036k (100), long transactions 118k -> 172k (10) and 71k -> 117k (100)**, bulk insert 16 connections 38.0k -> 45.1k.
Cost: the spinning threads use more CPU (32 connections: 10.5 cores busy against 7.5, i.e. ~205 us of CPU per commit against ~220 us: not more per commit, but the machine is busier for the same second). On a machine with fewer cores than runnable threads a long spin could steal time from the lock holder; 64 agents on 64 threads on 18 cores (this run) were fine, and `MW_SPIN_US` is the knob.

### Result (closed loop, bulk insert, `synchronous=full`, mean of 3 runs, every run valid)

| connections | SQLite WAL | Multi-Writer (§27) | **Multi-Writer now** | Turso 0.8.1 | MW now / WAL | MW now / Turso | p99 ms (§27 -> now) |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 12598 | 11792 | 11706 | 5779 | 0.93x | 2.03x | 0.1 -> 0.1 |
| 2 | 10954 | 17391 | 17296 | 8559 | 1.58x | 2.02x | 0.2 -> 0.2 |
| 4 | 10246 | 25517 | 25219 | 17434 | 2.46x | 1.45x | 0.3 -> 0.3 |
| 8 | 10082 | 35038 | 35715 | 28756 | 3.54x | 1.24x | 0.4 -> 0.3 |
| 16 | 9531 | 37239 | 45110 | 31883 | 4.73x | 1.41x | 2.7 -> 2.6 |
| 32 | 9684 | 34749 | 51327 | 43362 | 5.30x | 1.18x | 3.8 -> 3.6 |
| 64 | 9255 | 33754 | 50191 | 43446 | 5.42x | 1.16x | 4.4 -> 4.0 |

Multi-Writer is now ahead of Turso at every connection count measured (1.16x at 64 connections; Turso's runtime uses 18 worker threads and multiplexes tasks, Multi-Writer uses one OS thread per agent: the two are not the same resource usage). Latency at 1000 tx/s (real-time wake-up, §25): p99 0.64-0.81 ms at 1-32 connections. The former limit of §27 (relocation mutex saturated) is gone; what the threads wait for now is the group commit
(`make visible` and `log sync`: the in-order visibility hand-off after the fsync of the group), i.e. the durable-commit pipeline.

## 29. Analysis of the group commit and of the in-order visibility hand-off

Method: bulk insert (100 rows per transaction), `synchronous=full`, 8 and 32 agents, per-commit counters and timers added under `MW_TIMING=1` (leader cycle split into `pwritev` and `fsync`, records and bytes per group, waits split into "behind a running leader" and "for the prefix", wake-ups per waiting commit, visibility split into spin / park), a file-system baseline (`pwrite` + `fsync` of the same sizes outside the engine), and controlled experiments.

### The durable-commit pipeline (staged log, §25)
copy the record into the ring -> join the contiguous prefix -> the first committer of the prefix that finds no sync running becomes the *leader*: one `pwritev` of the prefix and one `fsync` -> `broadcast` -> every committer wakes, and **each makes its own epoch visible after its predecessor's** (`mw_db_make_visible`), then returns.

### What was measured (32 agents; 8 agents in brackets)
| | 32 agents | (8) |
|---|---:|---:|
| commits per group / KB per group | 6.3 / 188 | (2.3 / 66) |
| leader cycle (avg) | 89 us | (54 us) |
| ... `pwritev` / `fsync` | 24 / 65 us | (14 / 40 us) |
| cycles per second, leader busy | 9.8k, **87% of the time** | |
| commits that are followers | 84% | (57%) |
| mean wait behind a running leader (per wait) | 118 us | (53 us) |
| wake-ups per waiting commit | 1.99 | (1.99) |
| mean `log sync` per commit | 229 us | (101 us) |
| visibility: predecessor already visible / became visible while spinning / had to park | 6% / 8% / **92%** | (73% / 88% / 12%) |
| mean `make visible` per commit (park: 142 us each) | **141 us** | (3.9 us) |
File-system baseline outside the engine: `fsync` 35-45 us for appends of 4 KB to 188 KB, 56 us for 512 KB, 164 us for 2 MB; `pwrite` 9-14 us up to 188 KB.

### Findings
1. **The group commit is a serial pipeline and it is nearly saturated at 32 connections.** Throughput = records per group x cycles per second: 6.3 x 9.8k = 62k commits/s at 100% duty; the leader is busy 87% of the time and the run does 50k. The cost is latency-bound, not bandwidth-bound: the file system charges ~40 us per `fsync` almost independently of size, so a bigger group is nearly free.
2. **Every waiting commit is woken twice.** A commit that arrives while a cycle runs must wait for it to end (it cannot be in it), and then for the next one: ~2 wake-ups per commit and a mean wait of 2.6 cycles (229 us for an 89 us cycle). Followers are 84% of the commits at 32 agents.
3. **The visibility hand-off is a chain of wake-ups.** After the broadcast the ~6 committers of a group must make their epochs visible in order. 92% of them find the predecessor not yet visible, spin for ~7 us (20,000 `yield` instructions take about 7 us on this CPU: far less than the wait) and park on a condition variable; each hop is a kernel wake-up: **141 us per commit at 32 agents against 3.9 us at 8**. This is per-commit latency, not pipeline capacity: it lengthens the time a thread sits idle in a closed loop (32 threads / 50k tx/s = 640 us per commit; sync 229 + visibility 141 of them).
4. **The leader cycle is inflated 1.6x relative to the file system** (89 us against ~54 us for the same bytes; `pwritev` 24 against 14, `fsync` 65 against 40). It is not the compactor: only 0.1% of the cycles are slow (41 of 30,753). The likely cause is scheduling: 32+ runnable threads on a machine of 6 performance and 12 efficiency cores; a thread woken after an I/O completion waits for a core, and the leader may run on an efficiency core. Not proven.
5. **`synchronous=off` is not a ceiling.** With `off` the log is still the mapped one (page faults, no leader): at 32 and 64 agents it is *slower* than `full` (36k / 30k against 51k / 50k). Forcing the staged log for `off` (experiment `MW_STAGE_ALWAYS=1`): 8 agents 49k -> 54k, 16: 48k -> 63k, 32: 34k -> 43-57k, 64: 32k -> 37k; still below `full` at 32-64, because without a group commit every committer makes its own epoch visible and the chain of point 3 is at its worst.
   So the staged log is faster than the mapped one for reasons that have nothing to do with `fsync`, and the visibility hand-off limits `off` and `normal` more than it limits `full`.

### Experiment: the leader publishes the whole group's visibility (`MW_LEADER_VIS=1`, opt-in)
Every epoch up to the highest of the prefix is durable and installed when the leader's `fsync` returns, so the leader sets `db->epoch` to that value in one store before the broadcast; `mw_db_make_visible` returns at once for an epoch that is already visible and uses a compare-and-swap for the case where it is not (parked committers of non-staged paths are woken by the leader).
Result (tx/s, `full`, 3 s, mean of 2-3 runs): 8 agents 34.5k -> 34.8k (+1%, noise), 16: 45.2k -> 46.0k, **32: 50.7k -> 55.0k (+8.5%)**, 64: 50.4k -> 51.9k (+3%); `make visible` 141 us -> 0 at 32 agents. Validation: the whole suite (25 programs) passes with the switch on, 15 repetitions of `mw_stagedlog`, `mw_durability`, `mw_reloc`, `mw_lanes`, `mw_fastcommit`, `mw_gc` without a failure, AddressSanitizer clean. **It stays opt-in**; the gain is smaller than the removed 141 us suggests (+8.5% instead of +25%) because the pipeline of point 1 becomes the limit.

### Experiments that did not help
* A delay before the leader flushes, to make bigger groups: +2% at 32 agents with 10 us, nothing (or a loss at 8 agents) with 30 us. (An experiment that skipped the `pwritev` to bound the gain of overlapping it is invalid: without data in the file the compactor and the log rewrite fail.)
* Compaction is not the cause of the inflated cycle (point 4).

### Recommendations, in order
1. Enable the leader-published visibility (validated above; +8.5% at 32 agents).
2. Use the staged log for every synchronous level and give staged commits that do not wait for a sync the same group visibility (a combining advance by whoever finishes last): it is what limits `off`/`normal` and it removes the mapped log's page-fault cost. Not done; the earlier "combining" experiment (§18-19 of the profiling notes: -23% at 100 agents) spun waiters and would have to be redone with parking.
3. Overlap the `pwritev` of the next group with the `fsync` of the current one: the cycle would drop from 89 to ~65 us (+37% of pipeline capacity at most; the run would not reach it because of the other limits). Delicate: an `fsync` covers only what was written before it started, so the second group needs its own `fsync` after the first one ends.
4. Nothing to gain from bigger groups by waiting; ~2 wake-ups per commit could only be avoided by letting a committer be released by a cycle that is already running but that started after its record was copied (the design already does that; the double wait is for records that arrive during a cycle).

## 30. Overlapping `pwritev` and `fsync` in the staged log (recommendation 3 of §29)

**Change.** The staged-log group commit had one leader doing `pwritev` then `fsync` in sequence (cycle ≈ 89 µs at 32 agents). The two steps are now separate roles, each held by one thread at a time, and they run concurrently: the *writer* `pwritev`s the contiguous prefix of finished records that is not yet in the file (`flushed_off`/`flushed_epoch`), the *syncer* `fsync`s everything written before its `fsync` began (`synced_off`/`synced_upto`). While group n is in `fsync`, group n+1 is written; it then needs its own `fsync` (an `fsync` only covers what was written before it started, so durability is unchanged: a commit returns only when `synced_off >= its record end`). Failures of either step set `failed` and surface as `SQLITE_IOERR_WRITE`/`IOERR_FSYNC`; the `sync_quiesce` used by log rewrites waits for both roles. `MW_NO_PIPELINE=1` restores the sequential behaviour for A/B runs. The mapped-log path (`log_mode` 1) is untouched.

**Results** (bulk, `--sync full`, 6 s, one run each, macOS, tx/s):

| agents | sequential | pipelined | delta |
|---|---|---|---|
| 8  | 32.5k | 31.8k | -2% (noise) |
| 16 | 39.4k | 41.9k | +6% |
| 32 | 43.1k | 45.4k | +5% |
| 64 | 41.1k | 44.1k | +7% |
| 32, `MW_LEADER_VIS=1` (2 runs) | 45.3k / 44.8k | 48.1k / 48.6k | +6% / +8% |

**Reading.** The gain (5–8%) is well below the theoretical +37%: with overlap, `pwritev` time rises from ~24 µs to ~60 µs (it competes with the in-flight `fsync` on the same file inside the kernel), so the cycle only drops to ~62 µs and the remaining limit is the serialised `fsync` (~62 µs, one at a time) plus the visibility hand-off. Absolute numbers in this session are lower than in §29 (≈51k) because of machine state; the A/B ratios are what matter. At low concurrency (8 agents) there is nothing to overlap and no gain.

**Validation.** `make mw-test` 25/25; `mw_stagedlog`, `mw_durability`, `mw_reloc`, `mw_lanes` repeated 10×, and `mw_stagedlog` 5× with `MW_LEADER_VIS=1`, no failures. Default is pipelined; not tested on Linux.

## 31. Defaults: leader-published visibility and the staged log for every sync level

**Changes.** (1) The group leader publishes the visibility of the whole durable group (§29): now the default, `MW_NO_LEADER_VIS=1` restores the per-commit hand-off chain. (2) The staged log (§25) is used for every `synchronous` level, not only `FULL`/`EXTRA`: `MW_STAGE_ALWAYS` is gone and `MW_LOG_STAGING_OFF=1` selects the mapped log. Still single-process only (multi-process keeps the mapped log). With `off`/`normal` there is no fsync and no group leader: each committer copies its record into the ring and writes it out with a plain `pwritev` (no durability promise beyond what `off`/`normal` already give), so leader-published visibility only matters for `full`/`extra`.

**Why.** Measured before the switch (bulk, tx/s, one run each): `off` 8 agents 48.6k → 53.7k, 32 agents 36.1k → 56.5k; `normal` 8 agents 48.5k → 54.4k, 32 agents 37.2k → 56.8k (mapped log vs staged log). The mapped log pays page faults and a msync watermark that the staged log does not, and at 32 agents `off`/`normal` were *slower* than `full`, which made no sense as a user-facing setting.

**Validation.** `make mw-test` 25/25 with the new defaults; `mw_stagedlog`, `mw_durability`, `mw_reloc`, `mw_lanes`, `mw_multiproc_crash`, `mw_multiproc_liveness` repeated 8×, no failures. Not re-run under AddressSanitizer for this change.

**Final numbers with the new defaults** (bulk, tx/s, one run each): `full` 8/32/64 agents 31.9k / 48.9k / 46.8k (32 agents: 45.4k before, +8%); `normal` 53.9k / 56.1k / 34.3k; `off` 54.0k / 56.1k / 35.2k. At 64 agents `off`/`normal` fall behind `full`: without a group commit every committer makes its own epoch visible (the chain of §29), so the durability-free levels still lose to `full` when oversubscribed. Group visibility for `off`/`normal` is the open item.

## 32. Group visibility for `synchronous=off/normal`: implemented, measured, left opt-in (`MW_NOSYNC_GROUP_VIS=1`)

**Change.** With `off`/`normal` the record only has to be in the file to be visible, so the thread that writes a contiguous prefix (`stage_flush`) publishes every epoch of it with one CAS and wakes only the parked slots of those epochs, like the `full` leader does after its `fsync` (§29). Safety: only while no record that waits for an fsync was ever appended (`saw_sync`); with mixed sync levels it falls back to the per-commit hand-off, otherwise a `full` record could become visible to others before it is durable.

**Result: no gain, a loss at 32 agents** (bulk, tx/s, `sync off`, one run each): 16 agents 63.4k vs 63.1k with the hand-off chain; **32 agents 34–35k vs 56–57k**; 64 agents 33.5k vs 33.8k. `normal` is the same. The `MW_TIMING` counters explain it: with the chain 330k of 336k commits park in `make_visible` (243 µs each), i.e. most of the 32 threads sleep and only a few run at any time, so the relocation mutex is nearly uncontended (wait 14.6 µs, held 9.7 µs). With group visibility the threads are released together and all contend for it (wait 109 µs, held 17 µs) on a machine with 18 cores for 32 threads; the spinning locks make that worse (`MW_SPIN_US` 0/5/20 gave 40k/49k/38k, noisy, never above 56k). So for these levels the chain is acting as an admission control, and the 64-agent drop at `off`/`normal` (34k, below `full` 47k) has a different cause than the visibility hand-off: oversubscription of the relocation/stripe locks. Which is also why `full` is ahead there (its fsync group paces the committers).

**Decision.** Code kept, default off. Group visibility for `full`/`extra` (§29/§31) is unchanged. The open item for `off`/`normal` at ≥ 32 agents is admission control (limiting the number of committers inside `publish` at once), not visibility.

## 33. Admission control for `off`/`normal`; 32 separate processes

**Admission control.** For `synchronous<FULL` at most `MW_ADMIT` committers may be inside `publish` at once (default: number of cores, clamped to 4..16; `MW_ADMIT=0` disables). The others sleep on a condition variable before taking any lock; every epoch holder is inside, so the visibility chain cannot deadlock. Bulk, `sync off`, tx/s, one run each: 64 agents unlimited 34.2k, cap 4/8/12/16/24 = 42.9k / 46.9k / 48.2k / 48.8k / 44.4k; 32 agents unlimited 40.5k (this run; 55–57k in others), cap 16 54.9k, cap 24 58.0k. With the default (16 on this 18-core machine): `off` 8/16/32/64 agents 53.7k / 63.3k / 55.1k / 49.3k, `normal` 54.2k / 63.2k / 54.8k / 49.0k (64 agents: 34k before). `full`/`extra` are unaffected (their group fsync paces the committers). 25/25 tests, 5× repeats of the log/durability/relocation/lane tests, no failures.

**32 separate processes** (`test/multiwriter/bench/run_mp_procs.sh`: one process, one connection, same file, `--mp 1`, bulk, `sync full`, summed tx/s over 6 s, one run each):

| processes | plain SQLite WAL | Multi-Writer (multi-process) |
|---|---|---|
| 1 | 12.6k | 11.6k |
| 2 | 10.7k | 10.0k |
| 4 | 10.3k | 17.2k |
| 8 | 9.5k | 9.3k |
| 16 | 9.6k | 6.1k |
| 32 | 9.2k | 4.1k (5.5k with `sync off`) |

**Reading.** Multi-process Multi-Writer does not scale: it peaks at 4 processes and from 8 on it is at or below plain SQLite, at 32 processes less than half. The 32-thread figure of the same workload is 48k, so the in-process machinery of §25–§31 (staged log, group fsync, leader visibility, admission) is what carries the numbers, and none of it applies across processes: the multi-process path uses the mapped log, publishes the shared header under a cross-process lock and every commit catches up on everybody else's records. Not profiled; the cause above is the design difference, not a measured attribution. Multi-process mode is therefore correct (crash and liveness tests) but should be considered a functional feature, not a performance one, until it gets its own staged/group commit.

## 34. Multi-process: why a staged group commit would not help (not implemented)

Requested: the staged log with group commit (§25) for multi-process mode. Before building it I measured where 16 separate processes (bulk, `sync full`, ~5-6k tx/s total) spend their time (`MW_TIMING=1`, new stages `mp: catch-up before lock`, `mp: lock wait`, `mp: lock HELD`; one process of the 16 shown):

- `fsync`: 144-173 ms in 6 s (**≈3%**). The cross-process group commit already exists (a process's fsync covers every record written before it began), so a staged group commit would attack a cost that is not there.
- The **cross-process publication lock is saturated**: about 13.6k acquisitions per process for 1.8-2.2k commits (6-7 attempts per commit), each held ~26-31 µs, wait 370-440 µs; 16 processes × ~420 ms held ≈ 6.7 s of a 6 s run.
- The attempts are refused for page growth (11.4k `page_conflicts` for 2.2k commits, 1.9k relocations): the relocation is prepared under the per-process mutex, then the commit waits ~400 µs for the cross-process lock, and by then another process has extended the file, so the validation fails and the relocation is redone.

**Experiment (reverted).** Taking the gate and the publication lock at the start of the relocation and preparing against the state that will be published (one lock round per relocation) cut refusals from 11.4k to 2.5k per process, but throughput did not move (16 processes: 6.3-6.8k old vs 6.4-6.5k new, 4 processes 13-15.5k vs 14-14.2k, alternated runs): the lock is held longer per round (70 µs vs 26 µs) and the total serialised time per commit stays about the same. Reverted to keep the code simple.

**What would help** (not done): shrink the serialised section itself: (1) the record is copied into the mapped log inside the lock (52 µs per append, page faults; 23 µs staged in-process) - reserve the log range under the lock and copy outside it, which needs a cross-process, epoch-ordered "record complete" hand-off before the header is published; (2) every process re-applies every other process's commit (O(N²) copies of ~7 pages per commit), inherent to one in-memory store per process. Both change the multi-process protocol, so they need their own design and crash tests; multi-process mode stays a functional, not a performance, feature.

## 35. Multi-process: copying the record outside the publication lock (analysed, not implemented)

**Idea.** Reserve the log range under the cross-process lock and copy the record (52 µs, a third of the lock hold time) after releasing it, publishing `log_pos`/`committed_epoch` in epoch order with a hand-off between processes.

**Why it does not work with this protocol.** The next lock holder validates against the published state, and a record that is reserved but not yet complete is not part of it. Either the successor waits for the predecessor's copy (then the lock is merely renamed and nothing is gained) or it validates against the in-flight commit without its images. The second needs the in-flight write set in shared memory and breaks exactly the case that matters here: every growing transaction rewrites page 1 (change counter, size) and the relocation of the successor must merge onto the predecessor's page-1 image (§27/§28), which does not exist yet. A process dying between the reservation and the publication would also leave a hole every other process waits on, so it needs its own recovery path and crash tests. This is a redesign of the multi-process protocol, not an optimisation of it.

**Cheap variant measured (reverted).** If the 52 µs were page faults on the fresh mapped range, prefaulting it before taking the lock would help: `madvise(MADV_WILLNEED)` on the 96 KB after the current log end, 16 processes, bulk, `sync full`: append 42/60 µs with prefault vs 48/54 µs without, throughput 5.9k/6.1k vs 6.0k/5.8k. No effect (macOS may treat the advice as a no-op, and the time may be the copy and the checksum instead), so it was dropped.

**State.** Multi-process stays as measured in §33/§34 (best around 4 processes, below plain SQLite from 16). The realistic options are a different protocol (one committer process/thread that owns the log, others submit records to it) or leaving multi-process as a functional feature.

## 36. Multi-process: checking the structural advantages (measured)

New workloads in `mw_bench` (`slowtx`, `slowhot`, `slowpage`: an explicit `BEGIN; UPDATE; <think 5 ms>; COMMIT`, i.e. an application that does work in the middle of a transaction), and `run_mp_procs.sh` now prints refusals, latency and verifies the result at the end (sum of the updates equals the number of acknowledged commits, `integrity_check` ok). One process per connection, `sync full`, 5 s, one run each; plain SQLite in WAL waits on the write lock (`busy_timeout` 60 s). Fixed a harness gap on the way: a process joining a tracked database with `--no-setup` did not load the CRDT extension.

| test | processes | plain SQLite WAL tx/s | Multi-Writer tx/s | refused+retried (MW) |
|---|---|---|---|---|
| slow txn, disjoint rows | 4 | 159 | 638 | 0 |
| | 8 | 159 | 1275 | 0 |
| | 16 | 159 | 2561 | 0 |
| | 32 | 160 | 5039 | 0 |
| slow txn, different rows of the same (tracked) page | 8 | 157 | 1202 | 0 |
| | 32 | 157 | 4597 | 0 |
| slow txn, 4 hot rows (tracked, real conflicts) | 8 | 158 | 449 | 3737 |
| | 16 | 157 | 531 | 8742 |

All MW runs verified (no lost update, `integrity_check` ok).

1. **Slow transactions do not block each other: confirmed, and it is the largest effect.** SQLite serialises them (throughput = 1 / (think time + commit) ≈ 159 tx/s whatever the number of processes); Multi-Writer scales almost linearly (32 processes: 5039 tx/s, 31×) with p99 12.7 ms at 32 processes against a 5 ms think time. With the short transactions of the throughput benchmark this advantage disappears (§33).
2. **"Conflicts resolved instead of rejected": true only against a naive optimistic scheme, not against SQLite.** SQLite never rejects, it makes writers wait. What is confirmed: different rows of the same page commit concurrently with zero refusals across processes (rebase by the CRDT engine, 29× at 32 processes). Real conflicts on the same row are *not* free: on 4 hot rows Multi-Writer still wins on throughput (2.9-3.4×) but refuses and retries about 0.8-1 attempt per commit and its tail latency is far worse (p99 127 ms at 8 processes, 219 ms at 16, against 6.7 ms for SQLite, retries included). The advantage is throughput, not latency, on contended data.
3. **The file is an ordinary SQLite database after close: confirmed.** After a multi-process run the database opened with a stock SQLite 3.53.4 (Python `sqlite3`, no VFS, no extension) passes `integrity_check`, and only the `.db` file is left (no log or lock files).
4. **A process killed with SIGKILL does not stop the others: confirmed** (8 processes, one killed at t=4 s: the others ran to the end at the same rate, no errors, `integrity_check` ok; the dedicated `mw_multiproc_crash`/`mw_multiproc_liveness` tests cover the lock and log recovery paths).

Caveats: think time is a `nanosleep` inside the transaction, not real work; one run per point; macOS only; the multi-process throughput limit of §33-§35 applies as soon as transactions are short.

## 37. Tail latency on hot rows with long transactions

**Diagnosis.** `slowhot` (§36: 8-32 agents, `BEGIN; UPDATE` of one of 4 hot rows; 5 ms think time; `COMMIT`; refused transactions are retried) has two independent tail sources. (a) The same cell cannot be rebased (`a=a+1` is not a last-writer-wins assignment), so a lost commit means re-running the whole transaction, 5 ms each time: with 8 agents the chance that nobody committed the same row during a 5 ms window is ~13-20%, i.e. a mean of ~6 attempts and a p99 of ~20 (p99 129-197 ms). (b) The hot-spot turn (`retry_credit`) is held from the snapshot to the commit: for micro-second transactions that is the right thing (§24), for transactions that stay open for milliseconds it serialises them all behind each other in a 50 µs polling race with a 20 ms bound (`MW_TIMING`: hot-turn wait 16 ms average, 2199 waits), and the losers of the race starve (max 1.1 s in one experiment with the bound raised).

**Change.** A lane whose last three transactions each stayed open > 2 ms from snapshot to commit attempt is a *long* lane (a run, not an average: a descheduled thread must not change the class; a 100-row insert is ~0.5 ms and never qualifies). A long lane that lost twice in a row (`MW_LONG_STARVE`, default 2) takes the turn for `hot_credit × 16` transactions, and the turn for long lanes is a **FIFO ticket queue** (a waiter that gives up after 1 s marks its ticket abandoned and goes optimistic). In multi-process mode the queue lives in the shared header; the turn of a process that died (holder or queued) is released by the waiters (tested: 3 of 8 processes killed with SIGKILL, the survivors continue with p99 45 ms). A successful rebase no longer grants a serialised credit to a long lane. Short lanes keep the previous behaviour (`hot` 32 agents 13.5k tx/s as before; bulk, samepage unchanged). `MW_TURN_SLOTS` (default 1) allows more concurrent holders.

**Result** (`slowhot`, 5 ms think, `sync full`, 8 s, one run each; before → after):

| agents | tx/s | refused | p50 (ms) | p99 (ms) |
|---|---|---|---|---|
| 8 threads | 252 → 188 | 822 → 0 | 7.6 → 41.7 | **197 → 48** |
| 16 threads | 345 → 191 | 1733 → 18 | 37.7 → 84.0 | **235 → 92** |
| 32 threads | 436 → 201 | 4140 → 132 | 42.7 → 175.7 | 306 → 342 |
| 8 processes | 401 → 190 | 5201 → 6 | 13.8 → 41.7 | **117 → 47** |
| 16 processes | 490 → 189 | 12465 → 7 | 18.4 → 83.7 | **287 → 96** |

**It is a trade, not a free win.** The tail is bounded by the queue (p99 ≈ agents × cycle, 5.5 ms each) and nothing is refused any more, but the throughput falls to ~1/(think time + commit) (the same as plain SQLite, ~190 tx/s) and the median goes up 5×: the optimistic policy had the better median and the better throughput, the queue the better p99. At 32 agents on 18 cores the queue is longer than the old tail and it does not help. Intermediate policies were measured and were not better: 2/3/4 concurrent holders: 290/348/386 tx/s with p99 68-75 ms at 8 agents (between the two, on a straight line), a raised polling bound without a queue (p50 5 ms but p99 283 ms, max 1.1 s), credit 16/64 (partial serialisation, p99 79-111 ms). Row-level ordering would give both (4 hot rows could run 4 in parallel), but the wrapper sees pages, and the 4 rows share one. To restore the previous behaviour: `MW_LONG_STARVE=1000000`.

**Also noticed, not caused by this change.** The short `hot` workload at 32 agents has intermittent runs at ~6-8k tx/s (instead of 13.5k) with a 1 s maximum latency, the same before and after (6 runs each). Not investigated.

## 38. Multi-process throughput: waiting for the publication lock, and what did not help

Per-process timing at 16 processes (`mp: catch-up apply (in lock)`, new stage): of the ~28 µs an attempt holds the lock, 15 µs is the catch-up of the previous holder's record and, on a successful commit, the append into the mapped log is 55 µs (100-160 µs at 32 processes); 6-7 attempts per commit. The measurements below are medians of 5 runs of 5 s (single runs vary ±25%), bulk, `sync full`, tx/s summed over the processes.

**Kept.** The publication lock waited with a spin and `sched_yield`, which keeps every waiting process runnable: with 16-32 processes on 18 cores they compete with the holder for cores. After the short spin the waiter now sleeps 50 µs between polls (`MW_MP_SLEEP_US`, 0 = old behaviour), only when at least 6 processes are registered (`MW_MP_SLEEP_MIN_PROCS`; with 2-4 processes the sleep's hand-off latency costs more than the yields: 4 processes 13-15k -> 11-12k). Sleeping 25/50/100 µs gave the same result.

| processes | plain SQLite WAL | Multi-Writer before | Multi-Writer now |
|---|---|---|---|
| 1 | 12.5k | 11.6k | 9.6-12.1k |
| 2 | 10.6k | 10.0k | 7.5-10.4k |
| 4 | 10.0k | 14-17k | 13.8-16.7k |
| 8 | 9.8k | 8.1-9.3k | 9.8-11.0k |
| 16 | 9.6k | 5.7-6.1k | 7.3-9.1k |
| 32 | 9.4k | 4.1k | 4.4-4.7k |

Multi-process Multi-Writer is ahead of plain SQLite at 4 and 8 processes (1.5x, 1.1x), about level to slightly behind at 16, and less than half at 32. 1-2 processes stay a few % to 25% below (run-to-run noise is of that size at those counts).

**Tried and dropped** (each measured with the 5-run median): applying the others' commits while waiting for the lock (0-6%, within noise); taking the lock at the start of the relocation so it is prepared against the state it publishes on (2.5 lock rounds per commit instead of 7, but longer holds: +3%, dropped for simplicity; §34); a bigger sleep at 32 processes (200/500/1000 µs: 3.9k/3.2k/3.3k, worse); the record written with `pwrite` instead of the mapping (append 116-159 µs instead of 66-73 µs at 8-16 processes, throughput -35%); prefaulting the log range (§35).

**Zero-copy versions read from the mapped log (point 2 of the plan): not worth it.** It would remove the catch-up copies, but the catch-up in the lock is ~15% of the hold time and the whole run uses 20% of one core per process at 32 processes, so it is not the limit. What is left at 32 processes is the append into the shared mapping (grows with the number of processes, 55 -> 160 µs) and 200 µs of lock time per commit in total; both belong to the protocol (one shared log written by everybody) rather than to a tunable. The next real step would be the single log-owner design of §35.

## 39. Multi-process: from 1.5x to 2.3x SQLite at 4 processes, parity at 32, a floor beyond (measured on macOS only)

Goal: faster than plain SQLite with tens or hundreds of processes. Result: **ahead up to 32 processes, not beyond**; what was found, what worked and what did not.

**What was the real cost.** Profiling (per-stage timers, `sample`, and micro-benchmarks outside the project) showed that the append of a 30 KB record into the mapped log, which costs 5-8 µs in isolation, cost 45-160 µs in the running system. Reproduced in `/tmp` with two processes: a write through a `MAP_SHARED` mapping into a *sparse* file takes 40-65 µs while another process is in `fsync`/`msync` on the same file, 3-8 µs if the file's blocks were already written (zero-filled), 6-10 µs without any sync; `F_PREALLOCATE` (allocated but unwritten) does not help, `pwrite` instead of the mapping is worse (74-117 µs). The file system allocates blocks inside the write while the other process's fsync holds it. It is macOS/APFS behaviour; on other file systems the zero-fill is harmless but its effect is not measured.

**What was done** (all in the multi-process path; the thread path, the staged log and the single-process engine are untouched and their tests pass):
1. **The log file is extended with written zeros ahead of the log end** (`shm->log_ready`, 1 MB chunks, 3 MB ahead), by whichever process is past the window *after* it released the publication lock (claimed in `shm->log_fill_pid`, a dead claimer is taken over); the appender falls back to filling under the lock if it is ever behind. Records are only written below `log_ready`, the filler only above it. Append 45 -> 15-28 µs; 8 processes 10.3k -> 16.2k tx/s.
2. **Apply the others' commits while waiting for the publication lock** (near the head of the queue only): the hold is then the last predecessor's record, not everything since our last look. 8 processes 16.2k -> 17.7k, 32 processes 5.2k -> 8.0k.
3. **The publication lock is a FIFO ticket queue in the shared header**: the first few in line poll, the others sleep in proportion to their distance, a queued process that died is skipped, a dead holder is recognised. The polling lock woke every waiter every 50 µs (64 processes: > 1 M wake-ups/s, stealing cores from the holder). 32 processes 8.0k -> 9.9k, 64: 3.0k -> 5.2k.
4. **One lock round per commit**: `lane_publish` takes the lock once, and the plain attempt, the relocation (prepared against the state it publishes on) and the log append run in the same hold, where the old code queued again for the relocation and was refused again when somebody committed meanwhile. +5% at 8-16 processes, and far fewer refusals at 32+.
5. **Admission control** (more than 1.5 processes per core): at most `ncpu/3` (6 on 18 cores) writer transactions in flight, FIFO tickets, bounded wait of 50 ms (a holder that stays open, or dies, never stops the others; long transactions bypass it). Without it the throughput *falls* with the number of processes (128: 2.1k); with it: 64: 7.5k, 128: 5.4k. It costs ~10% at 16-24 processes, so it only engages above 1.5 processes per core.
6. The process registry is raised from 64 to 256 processes and from 512 to 2048 lanes (96 processes used to lose 32 of them at open).

**Result** (bulk, 100-row INSERT per transaction, `synchronous=FULL`, one process and one connection each, median of 3 runs of 6 s, tx/s summed; plain SQLite WAL with a 60 s busy timeout, 2 runs):

| processes | SQLite | Multi-Writer | ratio |
|---|---|---|---|
| 1 | 12.6k | 13.9k | 1.1x |
| 2 | 10.7k | 18.3k | 1.7x |
| 4 | 10.1k | 23.0k | 2.3x |
| 8 | 10.1k | 19.7k | 1.9x |
| 16 | 9.7k | 15.4k | 1.6x |
| 24 | 9.5k | 12.6k | 1.3x |
| 32 | 9.5k | 10.2k | 1.07x |
| 48 | 9.4k | 9.0k | 0.96x |
| 64 | 9.4k | 7.5k | 0.80x |
| 96 | 9.2k | 5.5k | 0.60x |
| 128 | 9.1k | 5.5k | 0.60x |
| 192 | 9.0k | 4.5k | 0.50x |
| 256 | 8.8k | 2.3k | 0.26x |

Every run is verified after the fact (`count(*)` equals 100 x acknowledged transactions, `integrity_check` ok), including 192 and 256 processes. `run_mp_kill.sh` kills a fraction of the processes with SIGKILL at t = 3 s (8 of 2, 16 of 4, 40 of 8, 64 of 12): the survivors keep their throughput (20.1k, 15.6k, 9.4k, 7.5k), the database passes `integrity_check`, holds a whole number of transactions and at least all the acknowledged ones. 25/25 tests, the multi-process ones 10x, AddressSanitizer builds of the multi-process tests and of a 40-process run: clean.

**Why it stops at 32.** Beyond that the cost per commit grows with the number of processes: every process applies every commit (N copies of ~7 pages per commit; `sample` at 128 processes: 92% of the time asleep in waits, the running time is catch-up and log re-mapping), and all of them map and fault the same log file. The serialised part is ~65 µs per commit at 32 processes and ~170 µs at 128. Making it O(1) per process needs page versions that are not copied per process (read lazily from the shared log, or a shared page store) or the single log-owner process of §35; both are large changes of the storage layer and of the crash protocol (and the owner process does not port to iOS/Windows); not done.

**Tried and dropped:** abandoning the queue when the attempt is doomed (-25%: the retry joins the queue again at the tail); applying commits while waiting for an admission slot (-50%: N processes doing N times the work); a bigger sleep at 32 processes; `pwrite` for the record; `MADV_WILLNEED` and `F_PREALLOCATE` on the log. Smaller knobs: `MW_MP_ADMIT`, `MW_MP_ADMIT_FROM`, `MW_MP_LOCK_SPIN=1` (old lock), `MW_MP_FAST`/`MW_MP_PER_US`/`MW_MP_CAP_US` (queue polling).

**Limits.** macOS only (the fsync-versus-mapping interference in particular); 18 cores (`ncpu/3` and 1.5 x ncpu are derived from it, not tuned elsewhere); the benchmark is a short-transaction insert load, slow transactions scale differently (§36).

*(Note: the private-store mode of several processes, with `MW_MP_PRIVATE`/`mw_mp=3`/`MW_MP_LAZY`, described in this and the older sections, was removed in 0.5.0; only the shared mode is left.)*

## 40. Lazy versions read from the shared log (implemented, correct, slower: opt-in `MW_MP_LAZY=1`)

**Idea.** In multi-process mode every process copies the ~7 pages of every commit of every other process into its private store (N copies per commit). With *lazy* versions the catch-up only registers a pointer to the page image inside the shared log mapping (`mw_pv.lazy` = file offset + 1, `data == NULL`); a page is read straight from the mapping (`mw_pv_data`) and is copied only if somebody needs a private copy.

**Implementation.** `mw_store_install_lazy` (catch-up), `mw_pv_data` for every read of a version (reads, head image, validation, compaction); pages whose lazy version was read (page 1, interior pages) are marked and copied eagerly from then on, off the critical path; `mw_store_materialize_lazy` copies every lazy version out before the mapping is replaced (generation change, rewrite, remap), before `seq_mu` (lock order) and before the ordinary close truncates the log in place. The one thing that could overwrite bytes that lazy versions point at, the in-place reset of the log, is gone: the log is only ever *replaced* by a new file (`mw_mp_rewrite_log`), so processes that still map the old file keep a valid image. The prefiller of §39 is excluded while the file is replaced (`mw_log_fill_hold`) and checks that its generation is the current one: a process on the old generation was found writing zeros into the old file with the new file's extent. A SIGBUS found by the first runs (the close path truncated the log with lazy versions alive) is fixed.

**Verification.** 25/25 tests; the multi-process tests 10x with `MW_MP_LAZY=1`; `run_mp_kill.sh` with lazy (8 of 2, 16 of 4, 40 of 8 killed: survivors continue, `integrity_check` ok, row counts consistent); runs of 8-64 processes verified after the fact; AddressSanitizer builds of the multi-process tests and of a 40-process run with lazy on: clean.

**Result: it does not help, it is slower.** Bulk, `synchronous=FULL`, tx/s, one run each, lazy vs copy at catch-up: 4 processes 18.8k vs 22.9k, 16: 10.9k vs 16.0k, 32: 7.4k vs 10.6k, 64: 5.9k vs 7.5k, 128: 3.9k vs 5.4k. The premise was wrong. Measured per call, the lazy catch-up is cheaper per record (install 0.9 µs against 2.7 µs) but total time in catch-up per process is not smaller (16 processes: 955 vs 772 ms of 8 s), the lock hold is longer (70 vs 45 µs at 16 processes), only 11 lazy versions were read in 5404 commits (so reads are not the cost), and the CPU time per process is the same while fewer commits are done. I could not attribute the loss (page-fault behaviour of touching a word in every image instead of streaming through the record is the likely cause; not proven). What `sample` at 128 processes does show is the cost of a generation change in every process (re-open, 1 GB mapping, scan of the log: a third of the running time), not the page copies, and a log of 512 MB instead of 32 MB helps 32 processes (9.6k -> 11.3k) but not 128.

**Decision.** Kept as an opt-in, off by default: the default numbers of §39 are unchanged (4 processes 23.1k, 16: 15.8k, 32: 10.4k, 64: 7.6k, 128: 5.3k). The next candidate for the 64+ range is therefore the per-generation cost (`mw_log_reopen` + scan in every process), not the copies.

## 41. The cost of a log generation change: measured, small; a smaller log for many processes

Asked: bring down the cost of a log generation change (every ~32 MB of log every process re-opens the new file, remaps it and scans it; the compactor rewrites the log). Measured first (new stages `mp: generation change`, `mp: log rewrite`, `MW_TIMING=1`, one process of the run, 8 s):

| processes | generation changes seen | per change | share of the process's time | the rewrite (holds the publication lock) |
|---|---|---|---|---|
| 32 | 76 | 0.52 ms (reopen 0.46, scan 0.06) | 0.5% | 1.6-1.9 ms, ~10 a second |
| 128 | 51-57 | 1.7-2.4 ms (reopen 0.85-1.0) | 1.1-1.5% | 0.9-1.5 ms |

so, in the process, ~1% of the time and ~2% of the lock time: not where the time goes. Where it goes at 128 processes (call graph of `sample`, inclusive, 5 s): 92% of the samples of a process are in the admission wait (`mw_mp_admit`), 5% in the catch-up (`mw_log_apply_at` 140 of 190 samples; 8 µs per applied record at 128 processes against 2-5 µs at 16: page faults and cache misses on a log everybody maps), the rest in the transaction. The system is limited by 6 admitted writers (`ncpu/3`) times their cycle of ~1.7 ms (catch-up of the backlog ~0.9 ms, SQL, commit 0.7 ms), not by the generation change.

**Tried:** (1) a bigger log means fewer generations and was *worse* at 64/128 processes (32 MB 7.3k/5.1k tx/s, 128 MB 6.1k/4.5k, repeated twice), a smaller one better: 4/8/16/32 MB at 32 processes 10.5k/10.9k/10.7k/10.2k, at 64: 7.9k/8.0k/7.7k/7.3k, at 128 equal (5.1-5.3k); independent single-row UPDATEs and same-page UPDATEs are unchanged by the log size (±3%), 8 processes lose 8% with 8 MB; (2) a process about to be admitted applies the others' commits while it waits (6 or 12 positions from the head): -12% to -2%, off; (3) releasing the admission slot before waiting for the group fsync: no change (32/64/128 processes 10.0k/7.2k/5.1k either way), reverted.

**Kept:** with more than 16 processes registered the compaction/rewrite threshold is 8 MB instead of the configured 32 MB (`mw_log_limit`): 32 processes 10.2k -> 11.3k, 64: 7.3k -> 8.2k tx/s; 8 processes keep 32 MB. Current numbers (bulk, `synchronous=FULL`, tx/s): 8 processes 19.8k, 32: 11.3k, 64: 8.2k, 128: 5.3k (SQLite 10.1k / 9.5k / 9.4k / 9.1k). Verified as in §39 (suite, multi-process tests, kill runs).

**Reading.** The per-generation cost is not the lever and the 64+ range stays below SQLite (0.87x at 64, 0.58x at 128). What would move it is the length of the admitted writer's cycle: the backlog it has to apply when its turn comes (N/6 commits) is the biggest part; making that cheaper (the lazy versions of §40 are not, as measured) or keeping it short by catching up before being admitted (§41 (2), which burns more than it saves) needs a different idea, not a cheaper reopen.

## 42. Portability audit of the thread path

**What was done.** Static inventory of every system dependency in `src/multiwriter/*` (5.6k lines); a compile with strict POSIX (`-std=c11 -D_POSIX_C_SOURCE=200809L`, which hides every non-POSIX name); UndefinedBehaviorSanitizer (undefined, alignment, signed overflow) on 8 test programs and 5 benchmark workloads; ThreadSanitizer on the tests and on 7 benchmark workloads with 8-16 threads; AddressSanitizer as before. `test/multiwriter/sanitize.sh asan|ubsan|tsan` repeats all of it. **What could not be done:** nothing was compiled or run outside macOS/arm64 (no iOS SDK, no Android NDK, no Linux or Windows toolchain, the Docker daemon is not running here), so the Linux/Android/iOS/Windows columns below are read from the code and the platform documentation, not verified.

### Fixed during the audit (found by ThreadSanitizer, all in the thread path)
1. **Schema-cookie check read page 1's version chain without page 1's stripe** (`publish_impl`): a publisher installing page 1 (`realloc` of the chain) or the GC (`memmove`) could run under it: a use-after-free window of a few instructions, only when the transaction did not itself write page 1. The cookie and epoch of the newest page 1 are now published in atomics (`p1_cookie`, `p1_head_epoch`) at install and the check reads those.
2. **GC could stay off for good**: the default `gc_interval = 64` was set by "the open that sees `refs == 1`"; with two connections opening at the same time neither did, and (without `mw_gc=` in the URI) GC never ran, versions accumulated. The default is now set when the database state is created.
3. Unlocked reads of `db->log_off` (publish's compaction check, the compactor, statistics), of `db->base_epoch` (statistics) and of `db->logfd` (publish tested `logfd >= 0` while a rewrite replaces it): a size taken under `seq_mu` and used locally, reads under the locks that protect them, and a `has_log` flag set once. A torn 64-bit read on a 32-bit platform could trigger a spurious compaction or the 0.5 ms back-pressure sleep.
4. `gc_interval`, `log_max_bytes` (written by every open that names them) and `gate_owner` are atomics; the 14 `static int v = -1` environment-knob caches are atomics with a sentinel (`mw_knob_int/flag`): two threads initialising them together were a data race.
5. A `_Static_assert` that 32/64-bit atomics are lock-free (the multi-process header is used through atomics by several processes).

After the fixes: UBSan 0 reports (tests and benchmark), TSan 0 reports in `multiwriter_*.c`; what TSan still reports is outside the engine: the sqlite-sync core (`fractional_indexing_set_allocator`, `block_init_allocator` called by the extension init of every connection, `vtab_build_changes_sql` static caches), the benchmark harness and `mw_compact.c`'s stop flag. Not touched (not in the Multi-Writer directory). Suite 25/25, multi-process tests and kill runs repeated, thread and process throughput unchanged (32 threads 48.8k, 32 processes 11.4k tx/s).

### Dependencies and what each platform needs
| dependency | where | on the thread path? | Linux | Android | iOS | Windows (MinGW, the Makefile's compiler) |
|---|---|---|---|---|---|---|
| `pthread` mutex/cond/create, `pthread_atfork` | everywhere; atfork in `db.c` | yes (compactor thread, group commit) | ok, needs `-pthread` (the Makefile only adds it for tests) | ok | ok (no fork) | winpthreads: link `-lpthread` (not in the Makefile's Windows flags); no `pthread_atfork` |
| C11 `_Atomic`, `<stdatomic.h>` | 77 uses | yes | ok | ok | ok | MinGW gcc ok; MSVC no |
| `pwritev`, `pwrite`, `pread`, `fsync`, `ftruncate`, `open`, `rename`, `unlink` | staged log, compaction | yes | `pwritev` needs glibc 2.10 + `_DEFAULT_SOURCE` (the default `gnu` mode has it, strict `-std=c11` does not) | `pwritev` API 24+; **32-bit ABIs need `_FILE_OFFSET_BITS=64`** (off_t is 32 bit otherwise) | ok | **none of them exist** (`_write`/`WriteFile`+offset, `_commit`, `_chsize`, no atomic rename over an open file) |
| `mmap`/`munmap`/`msync`/`madvise` | 8 MB staging ring (`MAP_ANON`); mapped log (multi-process, `MW_LOG_STAGING_OFF`) | ring: yes | `MAP_ANON` is a BSD name (`MAP_ANONYMOUS` is portable; strict mode lacks it) | 1 GB virtual mapping can fail on 32-bit (the code falls back to `pwrite`) | ok | `<sys/mman.h>` does not exist: **does not compile** |
| `flock`, `fcntl` byte locks | log ownership, multi-process | the log file is `flock`ed in single-process mode too | `flock` ok | ok | ok, but holding file locks in a shared (App Group) container while suspended gets the process killed (0xdead10cc): multi-process only | none (`LockFileEx`); `<sys/file.h>` missing |
| `clock_gettime`, `nanosleep`, `sched_yield`, `sysconf(_SC_NPROCESSORS_ONLN)` | timing, waits, admission | yes | ok (`_SC_NPROCESSORS_ONLN` is not POSIX: strict mode fails) | ok | ok | `clock_gettime`/`nanosleep`/`sched_yield` come from winpthreads; no `sysconf` |
| `__attribute__((constructor))`, `__thread`, `__builtin_*`, asm `yield`/`pause` | timing init, TLS, cpu relax | yes | ok | ok (the asm has a no-op fallback for 32-bit ARM) | ok | MinGW ok; MSVC no |
| `<sys/uio.h>`, `<sys/mman.h>`, `<sys/file.h>`, `<sched.h>`, `<unistd.h>` includes | all | yes | ok | ok | ok | **missing in MinGW** (`unistd.h` exists) |
| direct `open(db->path)` and `stat` of the real database file | compaction (`fd_real`), log recovery | yes | ok | ok | ok | bypasses the VFS: wrong for UTF-8 paths, sharing modes, custom/encrypted VFS |
| host SQLite compiled with `SQLITE_DISABLE_PAGECACHE_OVERFLOW_STATS` | Makefile flag | performance only | bundled amalgamation: ok | idem | a *system* SQLite (iOS, Android) does not have it: the global pcache mutex returns as a bottleneck | idem |

### Behaviour that differs even where it compiles
- **Durability.** The log is made durable with plain `fsync` (not through the VFS `xSync`): on macOS and iOS that does not flush the drive's cache (needs `F_FULLFSYNC`), which is also what SQLite does by default (`PRAGMA fullfsync=OFF`), but our code ignores the pragma, so `fullfsync=ON` is not honoured. Linux/Android: `fsync` is the right call.
- **Direct file access.** The log (`<db>-mw`) and the real database are opened with `open()`: any file-protection class (iOS Data Protection), scoped storage or encrypted-file VFS that is applied through the VFS is not applied. An encrypted database (SQLCipher) is untested.
- **fsync against writes through a mapping** (APFS): found for the multi-process log (§39); the thread path writes the staged ring with `pwritev`, so it is not exposed, but the stalls that overlapping `pwritev`/`fsync` cause (§30) are file-system specific too.
- **Memory.** One database allocates an 8 MB zero-filled directory (`MW_DIR_SIZE x 8`) and, with the staged log, an 8 MB ring; both are lazily committed on Linux/macOS, not necessarily with `calloc` on Windows. Version retention is bounded by GC but not by a byte limit: a long reader on a phone keeps growing it (jetsam on iOS).
- **Format.** The log is native-endian structs (`memcpy`), written for the machine that reads it back after a crash; not a transport format.
- **Environment knobs.** All tuning (`MW_*`) is read from the environment: an iOS app cannot set it, the defaults are what runs. They were tuned on 18 cores of one Mac.
- **Cache-line size.** The stripe padding assumes 64 bytes; Apple cores have 128 (performance only).

### Recommended order
1. **Linux (x86_64 and arm64) first**: it should build as is (`gnu` mode); run the suite, the sanitizer script and the benchmarks there (Docker or a CI job); add `-pthread`/`-lpthread` to the non-test link; expect the timings of §25-§31 to move (no APFS, `fsync` is real).
2. **A small `mw_port.h`** with the 12 operations the engine needs (open/pread/pwrite/pwritev/fsync/ftruncate/rename/unlink, anonymous mapping, advisory file lock, monotonic clock, sleep/yield, cpu count, thread create/join), implemented once for POSIX and once for Windows; `_FILE_OFFSET_BITS=64` for Android 32-bit; `MAP_ANONYMOUS` with `MAP_ANON` as fallback; `F_FULLFSYNC` where `fullfsync` is on.
3. **Windows**: thread path only (multi-process, the mapped log and `flock` compiled out), via `mw_port.h`; the rename of the log over an open file needs a different rewrite strategy there.
4. **iOS and Android on a device**: Data Protection with the log file, background suspension, memory limits, 16 KB pages on newer Android, the 32-bit ABI; none of it can be learned from a Mac.
5. Route the real-file access (compaction, recovery) through the VFS methods instead of `open(db->path)`.

## 43. Shared version index, phase 1: the data structure, and a 1000-process test

**Why (the diagnosis that led here).** Multi-process Multi-Writer stops scaling because the state is replicated per process: every process rebuilds in private memory the version chains of every commit of every other process (CPU per commit grows with N: 8 µs per applied record at 128 processes, ~0.9 ms of catch-up per admitted transaction), and the private store costs memory per process (peak RSS 111-166 MB at 64-128 processes: 1000 processes would need >100 GB, the machine has 69). On top of that the commit is one serialised path whose cost grows with N, and the per-process machinery (a compactor thread each, O(N) liveness scans, generation changes in every process, fixed limits of 256 processes) is O(N) too. The tuning of §30-§41 moved the peak, it cannot change the shape.

**What phase 1 is.** `multiwriter_shidx.c/.h`: one index in a shared file (`<db>-mwidx`, mapped by every process) that replaces the private stores. A page's versions are immutable records `(epoch, loc, prev)` in a shared arena, newest first, found through a two-level directory (page number -> head), `loc` being where the page image is (the log; opaque here). Page 0 is the database size per epoch. **One writer at a time** (the caller's lock), **readers take no lock**: a version is completely written before the head that points at it is published (release/acquire) and is never modified; a reader works at a snapshot epoch <= `committed`, registered in a shared slot registry (`shidx_pin`), validated against the GC floor with the announce / fence / re-scan handshake of the compaction. The garbage collector (in the writer) computes the oldest registered snapshot, keeps the newest version <= it and frees everything older, and drops a page's chain altogether when its newest version is already in the real file (`base`); each page is queued for GC once (a flag in the head word). Details that came out of the stress runs: a reader that was standing on the newest version of a page when GC dropped the chain finds a freed (poisoned) entry; the lookup notices (page number check) and looks again, getting "not in the index" = the real file, which holds the same page, so the *answer* is equivalent and the invariant "the same pin gives the same page content" holds while "the same pin gives the same `loc`" does not (the test checks the former). No lookup ever returned a wrong version.

**Verification.** `mw_shidx` (in `make mw-test`): versions and snapshots, pinned readers keep their versions, `base`, arena exhaustion (-1, GC frees, installs again), candidate-list overflow with a custom list (full scan), out-of-range pages (-2), a second opener adopting the file; a multi-process stress (3 writers behind an `flock`, 12 readers verifying that every version they see is real, not from the future, never older than what the same process saw before, a consistent size record, a reader killed while pinned and its slot reaped): 3 runs of 4 s, ~410k commits and 2.5 billion lookups each, 0 errors; also clean under AddressSanitizer and UBSan.

**1000 processes** (`make mw-shidx-bench`; N processes, 8 writers committing 7 pages through an `flock` queue, the others behave like agents: pin, 20 lookups, size record, unpin, think; 1M pages; checks as above on every lookup; macOS, 18 cores, one run each):

| processes | think time | commits/s | reader tx/s | lookups/s | lookup p99 | commit under the lock | errors |
|---|---|---|---|---|---|---|---|
| 100 | 200 µs | 39.3k | 343k | 7.2 M | 0.77 µs | 17 µs | 0 |
| 500 | 200 µs | 5.7k | 586k | 12.3 M | 0.77 µs | 49 µs | 0 |
| 1000 | 200 µs | 3.0k | 702k | 14.8 M | 0.77 µs | 60 µs | 0 |
| 1000 | 1 ms | 3.4k | 566k | 11.9 M | 0.77 µs | 90 µs | 0 |
| 1000 | 5 ms | 35.5k | 176k | 3.7 M | 1.5 µs | 19 µs | 0 |
| 1000 (32 writers) | 2 ms | 24-26k | 376k | 7.9 M | 0.77 µs | 29 µs | 0 |

**Memory.** One process: **1.3 MB physical footprint** (`vmmap`; the 9.4 MB RSS of `getrusage` counts the shared index pages every process has touched), 1000 processes ~1.3 GB private + the shared index (a few tens of MB for 130k live versions), against 110-160 MB per process of the private store (108-162 GB at 1000). The index itself holds 130k live versions of a 4M capacity; GC keeps up (2.4M freed in 8 s at 100 processes).

**Reading.** The index does what it is for: lookups cost ~0.8 µs at the 99th percentile with 1000 processes hammering one structure (14.8 M a second), pinning 0.1-1.6 µs, no per-commit work for anybody but the committer, memory per process is the process. Writers' commit rate falls when 1000 processes with a 200 µs think time keep the 18 cores saturated: the committer is a process like the others and gets descheduled while it holds the lock (the same commit takes 17 µs with 100 processes and 60-90 µs with 1000, and 19 µs when the readers think 5 ms): that is the scheduler, not the index, and in the real system the committer is the agent itself (its own CPU time) with the commit path of §39 around it.

**Not done yet (the next phases).** The index is volatile (rebuilt from the log by the first opener); the page images are not read from the log yet; `loc` is opaque; the liveness of the registry's pids is the caller's (`shidx_reap`); the segmented log without rewrite and without per-process reopen; the elected compactor; integration with `mw_db`: lanes read through `shidx_lookup`, commit validation and the relocation/merge read heads and images through the index, no catch-up and no private store in multi-process mode, limits raised from 256 processes; recovery after crashes; scale tests of the whole engine at 256/512/1000 processes.

**Found on the way.** A NULL dereference that I introduced in §42 (`mw_log_end_locked` on a database without a private store) crashed `mw_identity`; my check of the suite filtered for "failure(s)" lines and missed that `make` had stopped at the crash, so the tests after it were not run in the §42 verification. Fixed; the whole suite (26 programs) passes with `make` exit status 0, and the check now looks at the exit status.

## 44. Shared mode (`mw_mp=2`, phase 2): the shared index inside the engine, 1000 processes

Phase 1 (§43) built the data structure; phase 2 puts it under the commit path. In shared mode a process keeps **no private copy** of the database: pages come from the shared version index (`<db>-mwidx`, lock-free readers, one writer under the publication lock) and the record bytes from the segmented log (`<db>-mw.<N>`, 16 MB segments). There is no catch-up (a commit is installed and visible in one step under the publication lock), so memory and CPU per process no longer grow with the number of commits or processes. **Default**: `mw_mp=1` and `mw_mp=2` both select the shared mode; the private-store mode is reached with `MW_MP_PRIVATE=1` (or `mw_mp=3`). `MW_MP_SHARED` is no longer needed.

What was added on top of §43:

- **Cross-process group commit**: the process that finds no sync in flight becomes the leader (`msync` of the range + `fsync`), the others wait on a futex word and are woken when their record is durable. One fsync covers every commit published meanwhile, from any process. Durability is unchanged (a commit is acknowledged only after an fsync that covers it for `synchronous=FULL`).
- **Futex wake instead of polling/sleeping** for admission, the publication ticket lock and the group sync (macOS `__ulock_wait/wake`, Linux `futex`; `multiwriter_wait.h`). With nanosleep-based hand-off (100-400 us late on macOS) the 1000-process run was 10k tx/s; with futex wake 16-17k.
- Admission cap `ncpu/3` engaged above 1.5 processes per core, wait bound `MW_MP_ADMIT_WAIT_MS` (3 s).

Results (bulk workload, 100 rows per transaction, `synchronous=FULL`, one connection per process, 18-core laptop, APFS; every run verified with `--verify-sum`, all VALID):

| processes | multiwriter shared | stock SQLite WAL |
|---:|---:|---:|
| 8 | 24.9k tx/s | 9.9k |
| 64 | 19.9k | 9.3k |
| 256 | 18.0k | 8.8k |
| 1000 | 16.1k | ~9k (earlier measurement) |

Stock SQLite's throughput stays flat, but its worst-process latency explodes (p99 0.5 s at 64 processes, 4.1 s at 256); the shared mode's p99 is 4 ms at 64 and 28 ms at 256. At 1000 processes the median per-process latency is 57 ms and `refused+retried` is 896 (admission refusals, retried by the client).

Crash tests (`run_mp_kill.sh`, SIGKILL of 25% of the processes at t=4 s, whatever lock/ticket/slot they hold): 8, 64, 256 and 512 processes: survivors keep committing, `integrity_check=ok`, row count a whole number of transactions and at least all acknowledged ones. 12/12 consecutive runs at 512 after the fixes below.

Bugs found by this work (all fixed):

1. **Hang in `mw_gate_close` in shared mode** (13/40 runs of `mw_multiproc`): it waited for `db->epoch == db->next_epoch`, which are process-local and do not follow other processes' commits in shared mode. The wait is now skipped when `db->shared`.
2. **Ticket lock skipped a live ticket** (about 1/40 runs): the per-ticket pid ring (`pub_tk_pid`, 1024 slots) kept the pid of the ticket 1024 earlier; if that process had exited, the waiter that took the slot between `fetch_add` and its own store looked dead and its ticket was skipped, then the serving counter overshot and everyone behind it waited forever. The slot is now cleared when its ticket is over (in unlock and when a dead ticket is skipped). A holder that dies is still detected, because its slot is not cleared.
3. **Attach race with segment trim** (about 1 in 6 runs at 512 processes with kills): a process attaching read `seg_min`, the compactor trimmed that segment, `open` failed with `SQLITE_CANTOPEN`. The attach now rereads `seg_min` and retries (the salt is the same in every segment).
4. Recovery lost data at 512 processes when a segment ended in zero padding; fixed earlier (padding at the end of a segment is not a torn tail).

Limits and things not done:

- The private-store mode is kept for comparison and for the tests of its own log rewrite (`mw_multiproc_liveness` part 2); it is slower from about 8 processes up and uses O(commits) memory per process. Stats (`MW_FCNTL_DBSTATS`) in shared mode report the shared epoch and base, not the process's.
- With 1000 processes the admission queue is the bottleneck, not the disk: throughput per process is 16 tx/s and clients see refusals that they must retry (as with SQLite's `SQLITE_BUSY`, but nobody starves).
- A compactor thread still exists in every process (the election is by fcntl); liveness scans are still O(processes).
- ThreadSanitizer on `mw_shared` and `mw_shidx` is clean. `mw_multiproc` and the tracked-table benchmark workloads still report races that pre-date this phase and are outside the new code: first-open initialisation of statics in `multiwriter_vfs.c` (`mw_pass_methods`, `hook_registered`) and the sqlite-sync globals set at extension init (`fractional_indexing_set_allocator`, `vtab_build_changes_sql`). They look benign (idempotent writes) but are not fixed.
- Only macOS/APFS measured; Linux/Windows/iOS porting deferred by decision.

## 45. Elected compactor, and the O(N) scans: measured first

**What the scans cost.** The suspects were the loops over the process table (1024 entries), the snapshot slots (4096) and the liveness probes (`pid_alive`, `mw_mp_global_oldest`, `mw_mp_ceiling`, the process count of the admission control). Sampling (`sample`, 2 s) nine processes of a 1000-process run and three of a 256-process run: 99% of the samples are in the admission wait (`mw_mp_admit`, asleep on a futex); `mw_shared_compact` is 0.1% (20 of 15000 samples) and none of the scans shows up. The admission count runs once per 64 transactions per process; the slot scans run only in the compaction (once a second, under the publication lock, 4096 loads) and in the export path. So **they were left as they are**: at 1000 processes the limit is the single publisher, not these loops, and an extra structure (a free list or a bitmap of live slots) would be code with nothing to buy. If a workload shows them (many thousands of processes, or a very short compaction interval), the place to look is `mw_mp_global_oldest`.

**The compactor election.** Every process has a compactor thread. In shared mode each of them woke on its timer (and every publisher kicks its own thread when the log is over the limit), so with 1000 processes 1000 threads went for the compaction lock (one fcntl each) at every opportunity. Now the thread first *claims* the compaction with one CAS on a word in the shared header (`compact_req_ns`); the loser goes back to sleep. A claim older than 2 s is void (the claimant died); periodic compactions of all processes share one interval (`compact_end_ns`: a claim within half an interval of the last end is refused); a compaction forced by the size of the log ignores the interval. The fcntl compaction lock stays as the safety net (explicit `mw_db_compact` calls, close).

Effect on throughput: within noise (1000 processes 16.1k -> 18.0k tx/s, 256: 18.0k -> 19.7k, 64: 19.9k -> 20.5k, 8: 24.9k -> 23.1k; run-to-run spread is about 5%).

**A real bug found on the way.** With the election the compaction ran at the moments the test wanted it, and `mw_shared` crashed (SIGSEGV in `mw_seglog_read` from the compactor thread, 4 runs out of 4). The segment map cache (`map_acquire`) replaced a slot after checking `users == 0`, while a reader could count itself in between that check and the `munmap`. The replacing side now sets `dying` first and reads `users` second (both seq_cst), as the reader's fast path assumes. 8/8 clean runs of `mw_shared` afterwards; ASan on `mw_shared`, `mw_shidx`, `mw_multiproc` and TSan on `mw_shared`, `mw_shidx` report nothing; kill runs at 64 and 512 processes: OK.

## 46. Benchmark against stock SQLite: threads (1-64) and processes (1-1000)

Method (`bench/compare_sqlite.py`, raw results in `bench/results/compare_{threads,procs}.jsonl`): clean build, every run on a fresh database, bulk workload (one transaction = 100 rows inserted, `BEGIN IMMEDIATE` ... `COMMIT`), `synchronous=FULL`, 10 s measured after 2 s warm-up, 18-core arm64 laptop, APFS. Variants: **mw** (Multi-Writer, shared multi-process mode); **sqlite** (stock WAL, `busy_timeout` 60 s: the library waits inside the call, the application sees no retry); **sqlite0** (stock WAL, `busy_timeout` 0: the application retries with jittered backoff and the retries are counted). No transaction gave up in any run (all 2 x 3 x 18 runs `valid`, row counts verified). *Wait* = from the first attempt of a transaction to the moment its write can start (SQLite: `BEGIN IMMEDIATE` returned, i.e. it owns the write lock; Multi-Writer: admitted and snapshot taken, the publication-lock wait at commit is in the latency, not here). Retries per commit: Multi-Writer's are refusals (page conflicts / admission), SQLite0's are `SQLITE_BUSY`.

Threads (one process, one connection per thread):

| threads | mw tx/s | sqlite tx/s | sqlite0 tx/s | mw retries/tx | sqlite0 retries/tx | wait mean: mw / sqlite / sqlite0 | RSS mw / sqlite |
|---:|---:|---:|---:|---:|---:|---|---:|
| 1 | 12.7k | 12.5k | 12.5k | 0 | 0 | 0.01 / 0.01 / 0.01 ms | 170 / 12 MB |
| 4 | 23.8k | 10.1k | 8.7k | 0.001 | 1.1 | 0.01 / 0.30 / 0.29 ms | 463 / 12 |
| 8 | 26.8k | 9.9k | 8.3k | 0.003 | 1.4 | 0.02 / 0.69 / 0.79 ms | 546 / 13 |
| 16 | 26.4k | 9.6k | 8.2k | 0.01 | 1.7 | 0.02 / 1.5 / 1.8 ms | 560 / 14 |
| 32 | 25.2k | 9.6k | 7.9k | 0.03 | 2.3 | 0.02 / 3.0 / 3.9 ms | 583 / 18 |
| 64 | 23.8k | 9.6k | 7.8k | 0.06 | 3.5 | 0.03 / 6.2 / 8.1 ms | 619 / 25 |

Processes (one connection per process, same file):

| processes | mw tx/s | sqlite tx/s | sqlite0 tx/s | mw retries/tx | sqlite0 retries/tx | wait mean: mw / sqlite / sqlite0 | system memory growth mw / sqlite |
|---:|---:|---:|---:|---:|---:|---|---|
| 1 | 13.1k | 12.1k | 12.6k | 0 | 0 | 0.01 / 0.01 / 0.01 ms | +5 / +5 MB |
| 8 | 23.2k | 10.1k | 8.2k | 0.001 | 1.4 | 0.01 / 0.74 / 0.80 ms | +53 / ~0 |
| 32 | 21.2k | 9.4k | 7.8k | 0.001 | 2.3 | 1.2 / 3.3 / 3.9 ms | +199 / +144 |
| 64 | 20.3k | 9.3k | 7.5k | 0.001 | 3.2 | 2.9 / 6.7 / 8.3 ms | +386 / +275 |
| 128 | 20.1k | 9.1k | 7.1k | 0.001 | 5.2 | 6.1 / 12 / 18 ms | +755 / +509 |
| 256 | 19.6k | 8.7k | 3.1k | 0.001 | 14.6 | 13 / 27 / 82 ms | +1425 / +838 |
| 512 | 18.9k | 8.0k | 0.8k | 0.001 | 45 | 27 / 61 / 645 ms | +2841 / +1404 |
| 1000 | 17.3k | 2.9k | 0.1k | 0.001 | 82 | 58 / 299 / 3782 ms | +5627 / +2428 |

Tail of the wait (max, 1000 processes): mw 0.96 s, sqlite 11.4 s (p99 11.0 s: with a 60 s timeout nobody fails, but part of the processes does not get the lock for the whole 10 s run), sqlite0 12 s.

What the numbers say, and what they do not:

- Throughput: Multi-Writer is 2.0-2.8x stock SQLite from 4 threads / 8 processes up, and 5.9x at 1000 processes (stock SQLite collapses there: 12.5k -> 2.9k tx/s; with application retries 0.1k). At 1 writer the two are equal (the commit log fsync is the same cost).
- Retries: nobody gave up. Multi-Writer needs about 0.001 retried attempts per commit (thread path 0.06 at 64 threads); SQLite with an application retry loop needs 1-3 per commit at 8-64 writers and 82 at 1000 processes. With `busy_timeout` the retries disappear but become waiting, inside the library.
- Wait: with Multi-Writer a writer waits for admission only when there are more processes than the commit pipeline can use (from ~32 processes); at 64 threads the wait is 0.03 ms. SQLite's wait grows linearly with the writers (serialised write lock) and is very unfair: the median stays at 8-30 us while the mean and the maximum are seconds (some writers starve).
- Memory is the cost of Multi-Writer, and it is measured imprecisely here. Per-process RSS (`ru_maxrss`) of Multi-Writer is 170-620 MB for the threads and hundreds of MB per process with few processes, against 10-25 MB for SQLite; this RSS includes file-backed pages of the mapped log, index and database files (shared between processes and reclaimable), so for processes the sum of the RSS (13-44 GB at 64-1000 processes) overstates it; the system-wide growth of anonymous + wired + compressed memory during the run (right-hand column) is the better figure: about 2x SQLite's (5.6 GB against 2.4 GB at 1000 processes, i.e. ~5.6 MB per process against ~2.4 MB). The thread path's 170-620 MB was not split into anonymous and file-backed memory.
- Caveats: one workload (100-row inserts, one table, fsync-bound), one machine (macOS: `fsync` does not flush the drive cache, so absolute numbers are optimistic for both sides); the stock 1000-process number here (2.9k) is lower than the autocommit measurement of earlier sections (~9k) because the benchmark now uses explicit `BEGIN IMMEDIATE` transactions; the cause was not investigated.

## 47. Linux (gcc 14, aarch64, Docker Desktop VM): build, tests, sanitizers, benchmark

The engine builds on Linux without source changes (futex path of `multiwriter_wait.h`, `mmap`, `fcntl` locks) and all 27 suites of `make mw-test` pass (`make` exit 0). 40/40 runs of `mw_multiproc` in shared mode without hang; `mw_shared` 5/5; `mw_multiproc_liveness` clean. ASan and UBSan (gcc: `-fsanitize=undefined`; `sanitize.sh` detects gcc) report nothing on `mw_lanes mw_reloc mw_stagedlog mw_compact mw_durability mw_structural mw_rebase mw_multiproc mw_shidx mw_shared`. TSan on Linux found three real reports in the thread path that macOS TSan had not (all fixed): a read of `db->logfd` outside `compact_mu` against the log swap; `saw_sync` sitting next to three bools that gcc reads with one 8-byte load; and a plain `int` stop flag in a test. The remaining TSan reports are the known first-open statics (`mw_pass_methods`, `block_init_allocator`) and sqlite-sync's init globals, outside the engine.

What this does **not** establish: the container runs in Docker Desktop's Linux VM (ext4 on a virtual disk), 18 cores, 7.7 GB of memory. `fsync` reaches the VM's virtual disk, not necessarily the physical one, so this is not a power-loss test; and the VM's I/O has different costs from macOS, so absolute numbers do not carry over. A native Linux host (or at least dm-flakey/dm-log-writes) is still needed for the durability tests.

Bulk-insert comparison with stock SQLite WAL on the same VM (same method as §46, 10 s runs, `bench/results/linux_compare_{threads,procs}.jsonl`; sqlite0 = `busy_timeout` 0 with application retries):

| writers | mw tx/s | sqlite tx/s | sqlite0 tx/s | mw retries/tx | sqlite0 retries/tx | wait mean mw / sqlite (ms) |
|---|---:|---:|---:|---:|---:|---|
| 1 thread | 4.5k | 7.0k | 7.0k | 0 | 0 | 0.01 / 0.01 |
| 8 threads | 11.4k | 5.7k | 2.1k | 0.001 | 2.3 | 0.01 / 1.1 |
| 64 threads | 19.8k | 5.3k | 2.1k | 0.003 | 9.5 | 0.03 / 10.6 |
| 8 processes | 10.3k | 5.6k | 1.9k | 0 | 2.5 | 0.02 / 1.2 |
| 64 processes | 7.6k | 5.2k | 2.2k | 0 | 8.8 | 7.6 / 11 |
| 256 processes | 6.9k | 4.1k | 1.8k | 0.002 | 35 | 36 / 59 |
| 512 processes | 6.4k | 3.1k | 0.5k | 0.002 | 189 | 79 / 170 |
| 1000 processes | 5.8k | 2.3k | 0.04k | 0.003 | 288 | 170 / 412 |

All runs `valid`; nobody gave up except sqlite0 at 512 (79 transactions) and 1000 processes (5) after 1000 retries. System memory growth at 1000 processes: mw +975 MB, sqlite +528 MB (Linux: `MemTotal - MemAvailable`); thread-path RSS 66-482 MB against 12-101 MB.

Differences from macOS: with a **single writer Multi-Writer is slower than SQLite** on this VM (4.5k against 7.0k tx/s, p50 202 us against 121 us): the commit pays an `msync` + `fsync` of the log where SQLite pays one `fdatasync` of the WAL; on macOS the two were equal. From 8 writers up it is 2.0-3.8x faster with threads, 1.4-2.5x with processes; the margin over SQLite is smaller than on macOS for processes (1.4x at 64 against 2.2x) because the VM's fsync is the bottleneck for both. At 1000 processes the wait maximum is 0.5 s for Multi-Writer against 11 s for SQLite.

## 48. Where the pages of a tracked insert go (overhead of "sqlite-sync on every table")

Tool: `MW_PAGE_TRACE=1` prints the page numbers of every commit's write set (stderr); `MW_BENCH_KEEP=1` makes `mw_bench` compact and keep the database; `MW_BENCH_PADKEY=1` uses keys that sort in insertion order. Mapping page numbers to B-trees with `dbstat` on the kept file. One agent, `crdtinsert` (one row per transaction into a tracked table `ct(id TEXT PRIMARY KEY, a, b, c, d, pad)`), 2 s, `synchronous=FULL`:

| B-tree (what it is) | pages written per commit | tracked? |
|---|---:|---|
| `ct` (the table) | 1.01 | |
| `sqlite_autoindex_ct_1` (the TEXT primary key) | 1.19 | |
| `ct_cloudsync` (sqlite-sync metadata, WITHOUT ROWID on (pk, col_name)) | 1.55 | tracking |
| `ct_cloudsync_db_idx` (index on db_version) | 1.53 | tracking |
| `sqlite_schema` | 0.07 | |
| **total** | **5.35** (untracked insert: 1.00) | |

Findings:
- The tracking adds **two B-trees** to every tracked table, so the minimum is 4 pages per commit for a one-row insert (table, key index, metadata, db_version index); 64% of commits write exactly that. The metadata has **5 rows per inserted row** (171,090 cells for 34,218 rows: one per column), so each insert adds 5 entries to each of the two metadata trees.
- The rest (+1.35 pages on average, +3..+8 in about a third of the commits) is **B-tree splitting in index-type trees**: the WITHOUT ROWID table and the indexes cannot use SQLite's `balance_quick` (appends to a rowid table), so every split goes through `balance_nonroot`, which rewrites up to three sibling leaves plus the parent. About 80% of the excess comes from the two metadata trees (0.55-0.76 and 0.53-0.70 pages per commit above one), 15% from the key index, the table itself adds nothing. Padded (append-ordered) keys do not change it (5.71), so the cause is not the order of my keys.
- Stock SQLite writes the same pages (its tracked single-row insert ran at 22k tx/s against 11.6k for Multi-Writer in the same run, §above): the amplification is SQLite's and sqlite-sync's, but Multi-Writer pays for it again per page (copy into the ring, log bytes ~21 KB per commit, version-index install).

Optimisations this suggests, in the order of expected gain (not implemented, not measured): (1) fewer metadata rows per insert (one row-level record when all the columns of a row share the version of its creation, per-column rows only after a column changes alone): about 5x fewer cells and index entries, so about 5x fewer splits in the two metadata trees; (2) log a page as a difference from the previous version of the same page (the sibling pages of a split are almost unchanged): fewer log bytes and less copying, helps untracked tables too; (3) a cheaper `db_version` index (it exists for "changes since version X" queries).
## 49. Native rebase from the update hook: tried, measured, removed

An attempt to rebase transactions on plain tables (no sqlite-sync) from `sqlite3_update_hook` candidates (the rows touched), comparing each row as the transaction's pages show it with the snapshot's, and replaying the difference at the latest snapshot with column-level first-committer-wins. It worked (60/60 runs of a test with four writers on one page: no lost update, transfers between accounts exact) and it made the client retries almost disappear (insert with a TEXT key, 8 agents: 6435 -> 283 retries), but a replay costs more than re-executing a one-row insert (p50 414 us against 37 us), so throughput fell (15.0k -> 8.7k tx/s at 8 agents, 10.1k -> 4.3k at 32) and the 100-row bulk insert did not change. Removed (commit reverted): it added complexity for no measured gain.

Worth keeping from it: (a) a retry loop in a test must be patient and must `ROLLBACK` when `sqlite3_get_autocommit()` says a failed commit left the transaction open; a loop that gives up after N attempts and counts the transaction as lost looked like a lost-update bug in the engine for several hours (the published commits equalled the acknowledged ones); (b) the update hook does not fire for WITHOUT ROWID tables and the pre-update hook needs a compile option, which is why the next idea is to capture changes in the VFS (§51).

## 50. Shared index: a memory-ordering bug found by `mw_shidx` (about 1 run in 30)

`mw_shidx` (3 writers, 12 readers, 4 s, 2.5 billion lookups) reported one "not a real version" (a reader got an `(epoch, loc)` pair that no installed version has) in about one run out of thirty, not seen on x86 reasoning but possible on ARM. The lock-free lookup reads `epoch`, `loc`, then re-checks `pgno` and `epoch` (a seqlock-style read of an entry that the garbage collector may free and recycle). Two orderings were missing: on the reader, an acquire fence between loading `loc` and the re-check (the relaxed load of `loc` could complete after the re-check, so it returned the zero written by a later free, with the old epoch); on the writer, a release fence between the poison of a freed entry and the overwriting of its other fields (and before a recycled entry gets its new fields), so that a reader who sees an overwritten field also sees the poison. Fixed in `shidx_lookup`, `arena_free` and `shidx_install`; 0 failures in 120 runs afterwards (1 in 30 before), `make mw-test` clean. Not yet re-run under TSan on Linux.

## 51. Exploration: row-level change capture in the VFS (no triggers, no hooks, no sqlite-sync tables)

**The idea.** sqlite-sync needs, for every change, the table, the primary key, the changed columns and a version; it gets them from triggers and stores them in two B-trees per table (§48: that is 3 of the 5.35 pages written per tracked insert, and it only works for tables the application registered). The VFS already sees every page a transaction writes, and (through the page store) the same pages as the snapshot had them. If the row-level change can be derived from those two images, it works for every table, for every SQLite build, with no hook to lose to the application, and the metadata can live in the log instead of in tables.

**What was built** (`multiwriter_rowdiff.c`, an experiment: a sink that is called once per commit, nothing in the engine uses it). It decodes SQLite's file format directly: b-tree page header (offset 100 on page 1), cell pointer array, table-leaf cells (payload size, rowid, record with serial types, overflow pointer; the local-size formula for payloads that spill), then computes the *net* change of the table rows over the whole write set. Netting by rowid over all the leaf pages written is what makes splits and merges disappear (a row that moved between two written pages is no change). Two subtleties found by the test, both about the freelist: a page the transaction frees is not rewritten, so its old rows are only reachable through the freelist trunk pages (the freed leaf pages are the newly listed ones); and a page on the freelist holds stale rows that are not rows (neither the old nor, if it is freed, the new image of such a page may be read as table content). The freelist is therefore walked (old and new) whenever page 1 or a trunk page is in the write set.

**Test** (`mw_rowdiff`, added to `make mw-test`): 6000 random transactions, 1-6 operations each (insert, update of one column, delete) on two tables, values from 0 to 10 KB (so overflow pages, splits, merges, page reuse through the freelist), compared after every commit with the changes a shadow model says were made.

| | result |
|---|---|
| changes expected | 5792 inserts, 2843 updates, 1874 deletes (4939 commits, 3.4 pages and 2.1 rows each) |
| spurious changes | 0 |
| changes not recovered | 35 (0.33%), **all** of them updates of a row whose value lives in overflow pages, in commits that wrote an overflow page |
| update column masks | exact, except 89 where the columns beyond the local part of an overflow row are reported as changed (a safe superset) |
| decoding time per commit | median 2.0 us, p99 9 us (in `mw_bench`: bulk 100 rows/commit 2.0 us, one-row insert 1.8 us, one-row update 0.6 us; a commit costs 100-500 us) |

**What is still open (design, not built):**
1. *Which table is a page in.* The prototype pools the rows of all tables (the test keeps their rowids disjoint). The owner of a page can be tracked: roots come from `sqlite_schema`, every interior page in a write set lists its children (a page whose parent changes always has its parent in the write set), the freelist releases pages. Initial map: read only the interior pages from each root at open (about 1% of the database). In the shared multi-process mode that map has to be shared or derived from the log, which is new shared state.
2. *Overflow pages.* An update that only changes the tail of a large value rewrites an overflow page in place and leaves the leaf cell, even the leaf page, untouched: nothing in the leaf says which row it was (the 35 misses). It needs a reverse map overflow page -> (table, row), built when the chain is created (the leaf is written then) and, for an existing database, by a scan; until then such a change can only be reported as "some row of tables that have overflow values changed" (coarse, safe).
3. *Primary key and schema.* The identity of a row in sqlite-sync is the primary key, not the rowid. For tables with an INTEGER PRIMARY KEY it is the rowid; otherwise it is columns of the record, so the DDL has to be known (column names, which are the key). `PRAGMA table_info` on a helper connection, refreshed on schema change, is enough. WITHOUT ROWID tables are index-type b-trees with the row in the key: decoding them needs the same ownership map to tell them from secondary indexes.
4. *Versions and merging.* The sqlite-sync algorithms (per-cell column version, causal length for delete/re-insert, site id, last-writer-wins tie-breaks) need the *previous* version of each cell to compute the next and to merge a remote change. That state still has to live somewhere persistent, because a client can be offline for a long time and a peer's change has to be compared with what the cell held. The saving is not in having less state but in how it is written: one compact record per commit appended to the log (table id, key, column mask, db_version: roughly 40 bytes for 2 rows against 8-16 KB of metadata pages), and the per-cell state kept in a log-structured store updated in batches at compaction instead of two B-tree pages per commit. A per-row record when all columns share a version (an insert) cuts that state by the number of columns.
5. *Everything else that writes pages without being a row:* DDL (schema pages: the commit is classified by page 1's schema cookie, as now), VACUUM, `auto_vacuum` pointer-map pages, virtual tables, rebuilding indexes.

**Where this leaves the idea.** The central question (can the VFS recover the row-level changes?) is answered yes for ordinary tables, exactly, at about 2 us per commit, with one real hole (overflow pages) that has a known remedy and a measurable size (0.3% of changes in a test built to provoke it). The remaining work is the ownership map, the overflow map, the schema/key handling and above all the persistent version store, which is where the efficiency would be won or lost and which is a design of its own (an LSM of cell versions, shared across processes). Measured today: nothing about the end-to-end speed, only that the capture is cheap and what it would replace (the pages of §48).

## 52. Would row-level capture in the VFS be faster and cheaper than what we have? (measured upper/lower bounds)

The proposal (§51) replaces "triggers + two metadata B-trees per tracked table" with "decode the row changes from the pages (2 us), append a compact record to the log, keep the per-cell versions in a log-structured store". Not built end to end, so this is bounded by measurements: the cost we would remove (tracked against not tracked, same table), and the cost we would add (the decoder, measured in §51, plus a simulated per-cell version map, `MW_BENCH_ROWDIFF=2`: open addressing in memory, one entry per changed cell, under a mutex). macOS, `synchronous=FULL`, 4 s runs, retry mode; "not tracked" = the same table with no `cloudsync_init`; the bulk table has a TEXT primary key in both columns of the comparison (an INTEGER key would be appended in order and would not conflict).

| 1 agent | not tracked | tracked today | pages per commit (not tracked -> tracked) | not tracked + capture + version map (simulated) |
|---|---:|---:|---|---:|
| bulk, 100 rows/commit, Multi-Writer | 9.2k tx/s | 3.8k | 9.8 -> 19.0 | 8.4k (decode 2.9 us + map 3.9 us per commit) |
| bulk, stock SQLite WAL | 8.4k | 3.6k | | |
| one-row insert (5 value columns), Multi-Writer | 12.6k | 10.0k | 2.2 -> 5.4 | 15.9k (decode 1.8 us + map 0.2 us; run-to-run noise is +-15%) |
| one-row insert, stock SQLite WAL | 20.3k | 22.5k | | |

8 agents, bulk: tracked today 3.0k tx/s (p50 1.97 ms: every conflict is a CRDT rebase, one at a time), not tracked 8.5k (50,338 retries: the TEXT keys scatter over the leaves), not tracked + capture + map 8.2k (48,427 retries). 8 agents, one-row insert: tracked 13.6k, not tracked 16.3k, with capture and map 18.4k; stock SQLite 15.7k not tracked, 21.8k tracked.

**What it says.**
- Against the tracked path we have today, the proposal removes the whole difference to the untracked path: about **2.2x on bulk** (3.8k -> about 8.4k, also 2.7x at 8 agents) and **1.25-1.5x on one-row inserts**, with **half the pages written** per commit (19.0 -> 9.8, 5.4 -> 2.2) and so half the log bytes and half the compaction I/O. Of the 152 us the tracking adds to a 100-row commit, the decoder and a map update would take about 7 us.
- Against stock SQLite with sqlite-sync it is **2.3x on bulk** (8.4k against 3.6k) for the same reason (the triggers disappear), but on **one-row inserts stock SQLite is faster on this machine** whatever we do (20-22k against 12-16k): Multi-Writer's fixed cost per commit (ring, version install, group sync) is about 40 us more than a WAL append, and macOS fsync is nearly free, which is when that cost shows. The proposal does not change that.
- **Concurrency does not improve.** A conflict on a shared leaf still has to be resolved, either by a retry (as untracked) or by a replay of the captured changes; the native rebase of §49 showed that a replay costs more than re-executing a small transaction. The gain of the proposal is the price of tracking, not fewer conflicts. (Today the tracked path hides conflicts behind rebases, which is why tracked 8-agent insert is better than untracked there: that effect would remain only if we also build the replay.)
- **Memory: unproven.** The simulated map costs 24-47 bytes per changed cell in memory; sqlite-sync's metadata tables cost about 52 bytes per cell on disk (2192 pages for 171,090 cells, §48). An in-memory map of every cell does not scale (8.5 million cells = 192-384 MB after 40,000 bulk commits), so the real store has to be on disk, flushed in batches and read back for the previous version of a cell on update. An insert needs no lookup (the decoder knows the row is new); an update does, and a miss in the memory part goes to disk. That lookup, the shared form of the store for several processes, and the page-ownership map of §51 are not in these numbers: they are the cost that could eat part of the gain, and the numbers above are best-case for the part of the proposal that was simulated.

**Verdict.** Faster and cheaper than the tracked path we have: yes, clearly, on every measured shape (1.25-2.7x, half the pages), provided the persistent version store stays cheap on the update path. Faster than stock SQLite: only where tracking dominates (bulk), not on one-row inserts. A better multi-writer for conflicting transactions: no, that is a separate problem.

## 53. Prototype of the per-cell version store (log-structured, on disk) against sqlite-sync's metadata tables

`multiwriter_vstore.c` (not wired into the engine) and `bench/vstore_bench.c` (`make mw-vstore-bench`). The store: key (table, column, row key) -> (column version, db_version), 24 bytes per entry; an in-memory table that is written as an immutable sorted run when full (fsync'd), runs of similar size merged four at a time, a Bloom filter (10 bits per key) and a sparse block index per run kept in memory, point lookup newest run first (one 4 KB read when a filter says "maybe"); and a **change feed** (every put, in order, one file per flush; db_version never decreases along it) that answers "what changed since version X" without an index on db_version. No write-ahead log of its own: the commit log carries the change records and rebuilds the memory table after a crash. The comparison is sqlite-sync's metadata layout in stock SQLite (`WITHOUT ROWID` table on (pk, col_name) plus an index on db_version; WAL, `synchronous=NORMAL`, so that no fsync per commit is in the numbers) running the same operation stream.

**Workload.** 2,000,000 rows x 5 cells = 10 million cells loaded; then 150,000 commits, each updating one column of 20 rows (3,000,000 cell updates: a lookup of the old version, then a write of the new one), keys uniform or zipfian (theta 0.9, scattered by a hash as primary-key hashes would be). macOS, one thread.

| | log-structured store | sqlite-sync metadata in SQLite |
|---|---:|---:|
| lookup of the previous version, mean, data in the page cache | 0.45 us (uniform), 0.29 us (zipf) | 1.88 us / 1.72 us |
| write of the new version | 0.10 us | 2.91 us (row + db_version index) |
| **whole commit (20 cells), mean** | **11.5 us** (7.2 us zipf) | **253 us** (235 us) |
| commits per second (metadata only) | 87,000 (139,000) | 3,900 (4,250) |
| bytes written per commit | 883 + 1,937 (feed) = 2.8 KB | 176,696 (42.9 pages) |
| load of 10 M cells | 1.2 s (121 ns per cell), 1.8x write amplification | 24.3 s (2,431 ns per cell) |
| on disk | 264 MB (+ feed) | 440 MB |
| memory | 62.8 MB (memory table 48 MB, filters 12.5 MB, indexes 1 MB) | 64 MB page cache (set) |
| export "changes of the last 100 commits" (1,979 cells) | 0.3 ms (feed) | 1.5 ms (index) |
| export of the last 5,000 commits (99,490 cells) | 2.9 ms | 122 ms |

Lookups are answered by the memory table 4.9% of the time (uniform) or 53% (zipf), the rest by one run read (0.97 block reads per lookup, 0.02 Bloom false positives). Flushing and merging take 14.5% of the update phase (8.8% zipf), in line, so a flush stalls the caller for up to 158 ms: it has to run in the background in the engine.

**Cold reads: the real cost of the update path.** With the run files opened with `F_NOCACHE` before being written (so no block is in any cache and every block read goes to the drive), a lookup costs **72.5 us** (uniform; p99 144 us) and a 20-cell commit 1.45 ms, because the 20 lookups are serial and each is one random 4 KB read; with the zipfian keys (half of the lookups are answered in memory) 35.7 us and 0.71 ms. Everything else (write, flush, feed) is unchanged. SQLite's side was **not** measured cold (its file cannot be read without the OS cache from here without privileges); it needs the table leaf and the old db_version index leaf per updated cell, so it is at least as exposed to cold reads, but no number is claimed.

**What it says.**
- With the data cached, the update path of a version store is 22x cheaper in CPU (11.5 us against 253 us per commit of 20 cells), writes 60x fewer bytes and loads 20x faster than sqlite-sync's tables; an export is 5-40x faster than through the db_version index. That is the part of the gain that §52 left open ("could the store eat the gain?"): it does not, when the lookups are served from memory or the page cache.
- When the cells are not cached the update costs one random read per cell, about 70 us, and a transaction that updates many cells pays them serially: it needs the reads issued together (they are independent: 20 reads in parallel cost about one read latency), the cells of a row stored next to each other (one block per row instead of one per column; the key order (table, row, column) instead of (table, column, row)), and a block cache. An insert never needs a lookup (the decoder knows the row is new).
- Not in these numbers: the **shared** form of the store (several processes: the runs are plain files that can be mapped read-only by all, but the memory table has to be shared too), deletes and tombstones, variable-length primary keys (the prototype uses a 64-bit row key), merging the feed segments and its retention (a peer that has been offline for months needs the feed back to its version, or a scan of the runs), and crash recovery of the memory table from the commit log. The cost of keeping all of that correct is the open part, not the speed.

## 54. Prototype of the version store shared by several processes

`multiwriter_vsshared.c` (not wired into the engine) and `bench/vsshared_bench.c` (`make mw-vsshared-bench`). The store of §53 split into what several processes can share safely: **immutable files** (sorted runs, their Bloom filter + block index in a `.meta` file, change-feed segments), mapped read-only by every process, and **one shared mapping** holding the manifest of live runs and two memory tables with their feeds. One writer at a time, taken from a lock in the mapping (it stands for the engine's publication lock, which already serialises the publishers); any number of lock-free readers.
- *Memory table*: open addressing in shared memory, a slot is claimed by storing its key's `hi` word last with release ordering (a reader that sees the key sees its value), a value is one 64-bit word updated atomically, so a reader sees the old or the new version of a cell, never a mixture.
- *Lookup*: the active table, the table being flushed, then the runs newest first (filter, index, one block), all under a sequence counter on the manifest and the table roles; if the counter moved the lookup starts again (observed: 1 retry in 3 million lookups with 4 readers and 16 writers).
- *Flush*: the commit that fills the active table switches to the other one (writers go on at once) and, after releasing the lock, writes the full one out as a run and a feed segment (fsync'd), publishes it under the lock (two counter bumps), then empties the table. If the other table is still being flushed, the commit waits for room *before* it takes the lock (never while holding it). Merging: four runs of one size class, outside the lock, replaced in the manifest under the lock; the inputs are unlinked at once (processes that still map them keep working until they unmap).

**Correctness test.** `--mode shared`: 4, 8 and 16 writer processes update the *same* rows (lookup of the old version and put under the lock), memory tables of 100,000-400,000 entries so that there are hundreds of switches, flushes and merges meanwhile; afterwards every column version is summed and compared with the number of updates: **exactly equal, no cell missing**, in 3 configurations (1.6 M and 3.2 M updates; up to 441 stalls). (An earlier run came out 8 short of 1.6 M: it was the test picking the same cell twice in one commit; fixed, and the commit now picks distinct cells.)

**Speed** (macOS, 18 cores, 2 M rows x 5 cells = 10 M cells on disk, memory tables of 1 M entries, commits of 20 cells; writers on their own rows, the lookup of the old version outside the lock, as after the engine's page-level validation):

| writer processes | store commits per second, all together (as fast as they can) | cell updates per second | commit p50 / p99 | stalls |
|---:|---:|---:|---:|---:|
| 1 | 145k | 2.9 M | 6 / 17 us | 0 |
| 2 | 243k | 4.9 M | 7 / 16 us | 0 |
| 4 | 440k | 8.8 M | 8 / 18 us | 0 |
| 8 | 447k | 8.9 M | 10 / 32 us | 14 (1.5 s) |
| 16 | 536k | 10.7 M | 16 / 87 us | 62 (7.6 s) |

The store scales to 4 processes and then saturates on its writer lock and, above all, on flushing: at this speed (up to 10 million cell updates a second) a 1 M-entry table fills in about 0.1 s and a flush (sort, write, fsync) takes 100-200 ms, so writers wait for room. That rate is 20-50x what the engine can commit (10-30k commits/s in all, §46), so the realistic test is paced:

**Paced like the engine**: 16 writer processes at 1,250 commits/s each (20,000 commits/s = 400,000 cell updates/s in all) and 4 reader processes doing lookups at the same time: commit p50 **18 us**, p99 55-100 us (up to 3 ms for the processes that wait on the lock while another publishes a flush), lookup of the old version 0.8 us per cell, **0 stalls**; readers: 2.4 us per lookup mean (p50 1 us, p99 10 us, max 5 ms). The process whose commit fills a table pays the flush (100-200 ms, once per ~2.5 s at this rate): in the engine that has to be a background thread, not a committer.

**Memory.** The two memory tables and their feeds are one shared mapping (96 MB + 96 MB for 1 M-entry tables) whatever the number of processes; the runs are shared page cache. A store per process (what the private multi-process mode did, §30-41) would hold N copies and every process would have to apply every commit. The `ru_maxrss` printed for each writer (280-480 MB) counts the shared pages each process touched; it is not private memory.

**What is not done, and matters.** (1) A process that dies holding the lock, or in the middle of a flush (a table stuck in "being flushed"), blocks everyone: the engine's publication lock already detects a dead holder; the flush needs an owner pid and a takeover (the table is intact in the mapping and the run can be rewritten). (2) The mapping is a file that has to be recreated at open (the memory tables are rebuilt from the commit log after a crash, from the last flushed commit): not written. (3) Merging picks four runs of one size class and puts the result at the oldest input's position, which is right while the same-class runs are neighbours in age (true by construction here) but is not checked. (4) The feed segments are never merged or deleted. (5) Cold reads: the runs are mapped, so a lookup that misses the cache costs a drive read (§53: 70 us), and the lookups of one commit are serial here.

## 55. Integration prototype: row-level capture and the version store inside the commit (INTEGER PRIMARY KEY tables, single process)

`multiwriter_cdc.c` (URI `mw_cdc=1`, off by default), on top of `multiwriter_rowdiff.c` (§51) and `multiwriter_vsshared.c` (§54, with an anonymous shared mapping and a background flush thread). Scope, deliberately narrow: tables whose primary key is the INTEGER PRIMARY KEY (rowid alias), inserts, updates, deletes; one process; the store starts empty at every open (a cell that is not in it counts as version 1). Other tables (TEXT key, WITHOUT ROWID) are left alone.

**Where it sits in the commit.**
1. *prepare* (`lane_publish`, before any lock, so a retried transaction pays it again): decode the row changes from the write set and the snapshot pages (the table of every page from the owner map below), and for every changed cell look the previous version up in the store (an insert needs no lookup); new version = old + 1.
2. *owner map update* (`publish_impl`, under the stripe locks of the pages, right after the pages are installed): the interior pages the commit wrote give the owner of their children; the pages it freed lose theirs. Under the stripes, two commits of one interior page apply in epoch order.
3. *cell puts* (`publish_impl`, after the record is in the log and before the commit becomes visible; db_version = the commit's epoch): a transaction that starts after this one is visible looks its previous versions up after the puts; one that does not see it conflicts on the pages (the same cell is on the same page). (A first version did the puts under the stripe locks: the 63 us per commit at 8 agents of the table below were 10 us there plus the wait for the store's single writer lock.)

**The owner map** (page -> root page of its table; 4 bytes per page, one lazily committed anonymous mapping): built from `sqlite_schema` and, for every table b-tree, its interior pages only (the leaves below the last interior level are named by their parent, never read); rebuilt when the schema cookie changes; kept up to date from the commits as above. In the tests and benchmarks: 0 row changes in pages of unknown owner, 1 build (0.0 ms for the benchmark tables).

**Correctness test** (`mw_cdc`, in `make mw-test`, 25/25 clean runs): inserts give every non-key column version 1 at the commit's db_version (and none to the key column); an update raises only the changed columns, a same-value update changes nothing, three updates in one transaction are one version; a delete puts the row's marker; the TEXT-key and WITHOUT ROWID tables are not captured; 3,000 rows inserted in one statement (splits, interior pages) and half of a range updated: versions exactly as expected on a sample across the table; four threads retrying updates on one page (columns a, and every third time c as well): each row's versions equal 1 + the committed updates, and the values are right.

**Cost per commit, measured** (macOS, `synchronous=off` so that the commit is CPU-bound: with `synchronous=full` the run-to-run spread of the drive was 3-8x that day, too much for differences of microseconds; medians of 5 interleaved runs of 3 s; the capture's own timers are in the last column):

| workload | agents | off | on | change | added per commit | capture timers (prepare + apply) |
|---|---:|---:|---:|---:|---:|---|
| update of one cell (`independent`) | 1 | 145,290 tx/s | 133,057 | -8% | 0.6 us | 0.6 + 0.1 us |
| | 8 | 210,335 | 195,584 | -7% | 0.4 us | 1.8 + 0.2 us |
| insert of one row (`insert-int`) | 1 | 137,885 | 91,562 | -34% | 3.7 us | 3.2 + 0.2 us |
| | 8 | 138,592 | 92,835 | -33% | 3.6 us | 3.2 + 0.2 us |
| bulk insert, 100 rows (`bulk`) | 1 | 19,570 | 15,088 | -23% | 15.2 us | 5.3 + 9.5 us |
| | 8 | 52,815 | 34,677 | -34% | 9.9 us | 18.8 + 63.0 us (wait for the store's writer lock) |

**What it says.**
- In absolute terms the capture adds 0.4-15 microseconds to a commit (more with more rows: a 100-row commit does 100 cell puts and decodes 5 pages); the percentages are large only because `synchronous=off` commits are 7-50 us. With `synchronous=full` a commit is 100-500 us and the same microseconds are 0.1-10% of it.
- That is the number to put against the tracked path of §52: the same bulk shape costs the sqlite-sync metadata tables **152 us per commit (2.4x) and 9.2 extra pages**, the capture **15 us and no extra page** (pages per commit stay 5.0, as untracked); an update is 0.6 us here against roughly 20 us of triggers and two B-trees.
- The writer lock of the store is the first thing that shows at 8 agents on 100-row commits: 52,000 commits/s x 100 cells is 5 million cell puts a second through one lock, close to the 9 million the store sustains (§54); an engine with `synchronous=full` commits 10-30k times a second, so it is not in the way there, but it is the place to look (the puts of one commit can be sorted by shard of the table, or the store's memory table sharded).
- Measured here and not by the test above: the *conflict path*. A transaction whose commit fails (page conflict) has paid the prepare (decode and lookups) and does it again on retry; at 8 agents `insert-int` has 5,000-6,000 retries per 3 s run, and its 3.2 us prepare is already in the numbers.

**What this does not establish.** One process. Several processes need the owner map and the store's tables shared (the store is, §54; the map is a private anonymous mapping here), and the commits of the other processes must reach each process's map. The sqlite-sync algorithms themselves (the merge of a remote change, the causal length, the site id and tie-breaks) are not implemented: the versions here are a counter per cell, which is what the merge needs to *compare*, not the merge. Persistence: the store is rebuilt from nothing at open; recovery from the commit log is not written, and a lookup that goes to a cold run costs a drive read (§53). Tables without an INTEGER PRIMARY KEY need the primary key from the record and the schema. The decoder's known hole (an update that only rewrites an overflow page leaves no trace in the leaf, §51) applies: such a change is not captured.
