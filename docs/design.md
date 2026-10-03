# sqlite-multiwriter: design

## What it is

A wrapper VFS that lets many connections (threads and processes) write one SQLite database concurrently (page-level optimistic concurrency, a commit log with group
commit, versioned pages, compaction into the database file: `docs/engine-history.md` has the measurements behind each choice of the engine), and that captures, for
**every** table, the row-level changes of every commit, so that the database always carries the state a CRDT (the algorithms of sqlite-sync) needs. No SQLite source
is changed, no SQL syntax is added, triggers and hooks are not used (they do not exist for WITHOUT ROWID tables, the pre-update hook needs a compile option, and an
application can replace the update hook of its connection).

sqlite-sync (`deps/sqlite-sync`, a submodule) is the reference for the algorithms and the wire format and the oracle of the differential tests. Its SQL-level API
(`database.h`, the metadata tables built by triggers, the `cloudsync_*` functions) is not used; of the submodule only `lz4.c` is linked into the library.

Open a database through the VFS with `file:db?mw=2&mw_cdc=1` (threads, one process) or `file:db?mw=2&mw_mp=1&mw_cdc=1` (processes; `mw_mp=1` is the shared mode). Without
`mw_cdc=1` it is the plain multi-writer engine.

## Layers

1. **Engine** (`src/multiwriter_*.c`): lanes (one per connection), page store, commit log (staged ring in one process, segmented log shared by processes), shared version
   index, compaction, relocation of pages after concurrent growth, admission control, recovery after a crash. A commit record carries, after its pages, a *metadata
   extension* (the CRDT state of the rows the commit touched).
2. **Capture** (`multiwriter_rowdiff.c`, `multiwriter_catalog.c`, `multiwriter_cdc.c`): from the pages a transaction wrote and the same pages at its snapshot, the net
   row changes of every table b-tree: inserts, updates (with the cells that changed), deletes, key changes. The table of a page comes from the owner map
   (page -> root page of its table, kept up to date from the commits); the layout of a table from the catalog (`sqlite_schema` parsed by SQLite itself in a scratch
   database: generated columns, WITHOUT ROWID, collations, INTEGER PRIMARY KEY aliases are never guessed). Overflow pages are followed; a write to an overflow page alone
   (the tail of a big value changes, the cell does not) is attributed through an overflow owner map.
3. **CRDT core** (`src/crdt/`): the algorithms of sqlite-sync as functions over an abstract state: causal length per row (odd = alive), column version per cell (odd =
   alive, +2 per local update, +1 when it is even), the merge of a remote change (causal length, then column version, then the value, then the site id), the primary-key
   byte format. It is differentially tested against the real sqlite-sync (`test/oracle_*.c`).
4. **Metadata store** (`multiwriter_meta*.c`, `multiwriter_mmeta.c`, `multiwriter_runs.c`, `multiwriter_runstore.c`): per row the cells (column version, db_version, site, sequence). In one process a striped memory table
   in front of sorted runs kept in ordinary tables of the database file; in several processes a shared index of row buckets into the log. See below.
5. **Sync API** (`multiwriter_sync.c`): site id, db_version, export a payload since a db_version, apply a payload atomically.

## What a commit carries

The metadata of a commit is a function of its row changes and of the state before it. It is computed before the commit is published, from the transaction's own pages,
and written *inside the commit record*: the rows the commit touched, each with its complete new state (every cell: column id, version, db_version, site, seq). Applying a
commit's metadata is the single way the state changes, whether the commit is ours (at publication), another process's, or the log's (at recovery), and it is idempotent:
the state of a row is replaced, so applying a commit twice, or after a flush that already holds it, changes nothing.

Ids are stable and nothing has to be persisted for them: a table's id is the hash of its name (case-insensitive FNV-1a), a column's id the hash of its name; a collision between
two tables (or two columns of a table) is detected when the catalog is built and the later one is not synchronised. Sites have ordinals (0 = this database) that the extension
names with their ids, so a replay can rebuild the map.

db_versions: each commit has the db_version `epoch + origin`, where the epoch is the engine's commit counter (it restarts at 1 whenever the database is opened afresh after a
clean close) and `origin` is the largest db_version the file tables have seen, found when the store is first used. So db_versions only grow, over restarts too.

## Where the state lives

### One process (`mw_mp=0`)
A table of rows in memory (striped by row hash, each entry the cells of one row), applied by the publisher after the commit's record is in the log and before the commit is
visible. Rows changed since the last flush are *dirty* and never dropped; the rest is a cache that is filled from the file when a row is asked for and shrinks under a budget.

### Several processes (`mw_mp=1`, the shared mode)
Nothing is kept in a process. The newest state of a row is in the log (the extension of the commit that made it) and a **second shared index** (`<db>-mwrow`, the same
structure as the page index) maps the *bucket* of the row (hash of table and key, 2^21 buckets) to the log position of the newest state of the whole bucket. A commit writes
the complete new state of every bucket it touches (the touched rows and the others the bucket holds, which are almost always none), so the head version of a bucket is
complete and older versions are only for readers that started earlier (none: reads take the head). The publisher, under the publication lock, validates that the buckets it
writes have the head epoch the transaction read them at, installs them, and publishes. The owner maps are a file mapped by every process (`<db>-mwown`), rebuilt, when the schema
cookie moves, under the publication lock. The site table and the list of dropped tables are in the shared header. The flusher is whichever process claims it (a byte lock).

### The file tables
`mw_state(k, v)` (`meta_epoch`, `dv_hwm`, `runs_ver`, `next_run`, `next_age`, `next_slot`, `slot_bytes`), `mw_sites(ord, id)`, `mw_runs`, `mw_slots`, `mw_free`, `mw_resv` and `mw_drops`, created by the first connection.
They are ordinary tables, so the file is self-contained: copy it and the CRDT state comes along. The state of the rows lives in **sorted runs** (`multiwriter_runs.c`, `multiwriter_runstore.c`):

- A *run* is an immutable sequence of rows in key order (table, then key bytes), cut in blocks of about 16 KB. A row is one row of a user table: its key, the largest db_version among its cells and its
  cells packed as varints (a format byte, the number of cells, then per cell the column + 1 so that the sentinel is 0, the version, the db_version, the sequence and the site). A row with no cells is a
  deletion marker. A block is stored compressed with LZ4 when that gains a tenth or more (the rows of one block are very alike: 16 KB become 5.7 KB). The *meta* of a run (the first key and the newest
  db_version of every block, the last key, a Bloom filter of 10 bits a row, where every block is stored) is kept in memory (a few percent of the data); the blocks are read through a cache.
- **Slots.** A block, and the meta of a run, are stored in *slots*: rows of `mw_slots(slot, data)` of exactly the same size (`slot_bytes`: four of them fill a page of the file), each starting with the run, the
  block and the part it belongs to (a slot read for another run is an error, not a wrong answer). A slot that is written again is overwritten where it is (SQLite does that when the new record has the size of the
  old one), so the metadata never allocates a page of the file or gives one back to its free list, which is what every writer of the application meets at its commit (all the conflicts of the transactions of the metadata
  with the writers were on page 1, the header of the file: a database that is growing writes it at every commit). A run that goes (merged) gives its slots to `mw_free`, a bitmap in rows of 512 bytes
  that is updated in place too; the next run takes the lowest free slots first, then new ones past the last. A process takes slots by the thousand into a pool in memory (one small transaction, which also writes the process's entry in `mw_resv(pid, slots)`: everything it holds, as ranges, in the transaction that
  takes them), and gives the unused ones back when its transaction does not commit and, at the close, all of them (and its entry goes). Slots that nobody has (a process that was killed with its pool, a merge that failed)
  are found by comparing the numbers taken so far with the runs, the free bitmap and what the processes that are alive hold (their entries in `mw_resv`; ours is in memory): what is left belongs to nobody and becomes free,
  and the entries of dead processes are deleted. The merge thread does this when it starts and every ten seconds, under the lock that makes it the only merger (a pid that was reused keeps its slots lost
  until it ends: space only, never a slot taken from a live process).
- The flush writes one run (level 0) for every 32768 rows of the batch, in one ordinary logged transaction together with `meta_epoch`, `runs_ver` and the other counters: the batch and the epoch it
  covers are atomic. It does one pass over the dirty rows, packs them straight into the batch and writes blocks: about 0.15 us a row (the b-tree it replaced, one insert per row, cost 0.7 to 1 us, and
  4 us with random keys). The rows in memory are the complete state of the row, so the flush replaces; nothing is read before it is written.
- A *lookup* asks the runs newest first: the key range and the Bloom filter of the run, the fence keys to find the block, the block (cache, or its slots), a binary search in it. The first run that has the key
  answers (a deletion marker: not found). An insert of a key that the filter of known keys has never seen asks nothing. A reader always has the list of runs and the slots from one snapshot (the version of the
  list is read in the transaction that reads the slots).
- *Merging* is done by a thread of its own: the `fanout` (8) oldest age groups of the lowest level that has that many are merged into the next level, in parts of 32 K rows. Every transaction of a merge is small
  (some blocks, one row of `mw_runs`, one run removed) and they take turns with the flush: the new parts first, the inputs removed last and **the oldest input first**, so that a merge that is interrupted (a process
  that is killed) leaves the newest of its inputs, which the output (the age of the newest input) still hides; the other order leaves the oldest, which a later merge with newer runs would turn into an output that
  hides newer versions the first output holds (found by the SIGKILL rounds of `mw_mpmeta`). The oldest data (no run outside the merge up to the age of the output) drops deletion markers and the rows of dropped tables.
  With several processes one of them merges at a time (a byte lock) and the others flush.
- *Back-pressure*: when the merges are behind (more than 48 age groups at level 0, or 144 in all) the commits of the application wait in proportion (up to 20 ms); the flush is never held (the log can only be
  compacted up to the flushed point). The connections of the metadata themselves never wait for the writers: no turn behind them for hot pages, no place in the queue of admission (its few slots shared by all processes were held by the long transactions of the flusher and the merger of every process: 64 processes went from 9k to 3k tx/s), no throttle, no hard limit of the log (which commits wait for past 768 MB
  so that the 1 GB mapping of the log is never outgrown).
- *Export* reads only the blocks whose newest db_version is past `since` (no index to maintain), takes the keys of their rows, and asks the runs for the newest state of each key. `mw_cells` is a read-only
  virtual table over the runs with the columns of a cell (`tbl, pk, col, cv, dv, seq, site`; `col` -1 is the causal length), for looking at the metadata in SQL (it reads everything into memory: it is a tool
  for looking). The commits of the metadata connections themselves are not captured (they write only `mw_*` tables, never tracked).
- Reads that miss the cache of rows go through a small pool of read-only connections, one transaction for the keys of a commit.

Costs and limits: the metadata of a database takes about 20 bytes a row (a row of the benchmark table takes 85) and the file does not grow with the history: slots are reused. Opening a database reads the keys of
all blocks once to fill the filter of known keys (as the b-tree scan did); a database written by an earlier layout of the metadata (`mw_rows`, `mw_cells` as a table, `mw_blocks`) has none of the new tables and starts
without metadata. Tuning for tests: `MW_META_FANOUT`, `MW_META_PART_ROWS`, `MW_META_CACHE_MB`, `MW_META_FLUSH_ROWS`, `MW_META_FLUSH_MS`, `MW_META_MAX_RUNS`, `MW_META_NOMERGE`, `MW_META_NOCOMPRESS`; `make test-stress` runs
the metadata tests with the store under pressure.

### Durability and recovery (the rule that makes it ACID)
- A commit is durable when its record is in the log; its metadata is in the record. A crash can therefore never leave pages without their metadata or metadata without
  pages: they are one record, valid or not as a whole (checksummed).
- Recovery replays the valid prefix of the log: pages as always, and the extensions of the records (memory table in one process, bucket index in several). The flush point of
  the file is of an older incarnation of the epochs, so everything the log holds is applied again (idempotent).
- Compaction (which moves the log into the database file and lets the log forget) is held back to the flushed point as long as there is metadata above it: the metadata of
  a commit exists in the log record or in the file tables, never nowhere. A compaction that is limited asks the flusher to run.
- The last connection to close flushes before the log is dropped.

### Errors of the file system (I/O errors, a full disk)
- **A full disk is not the end of a connection.** The staged log (single process) reserves the disk for its records ahead, in steps of 4 MB (`F_PREALLOCATE`, `fallocate` with
  KEEP_SIZE), before a commit takes its epoch and offset: with no room the commit fails with `SQLITE_FULL`, nothing was assigned, installed or written, and the next commit finds the
  room when the space is back. A write inside the reservation cannot fail for want of space. (The reservation also made the log faster on APFS: 23 against 33-40 us per write and fsync.)
  The shared mode appends through mapped segments that are written whole beforehand: a segment that cannot be made (no room) fails the commit that needed it, leaves nothing behind,
  and is made again by the next one; the append fails before it touched anything the other processes can see, so the database is not failed. Measured with a real full volume: the
  connection that got `SQLITE_FULL` commits again 2000 times after the space returns, with no reopen, in both modes.
- **A failed log recovers in place (single process).** A write or an fsync of the log that does not succeed leaves the outcome of the commits of the group unknown, and they are refused. Nothing
  is visible beyond the last durable commit (a commit becomes visible when its fsync is done), so the next commit that finds the log failed goes back to exactly that point, without a reopen
  (`mw_db_recover`): it waits (bounded) for the commits inside to leave; the log is cut to the end of the last durable record (what lies beyond may be on the disk in part, and the commits it
  holds were refused: they must not come back); the page store is rebuilt from the log as an open does (the old store stays allocated until the database is closed, for a reader that was
  inside it); the memory table of the metadata is dropped and brought in again from the file and the log; the counters of the log and of the epochs go back. The metadata flusher and merger
  stop for it and start again with the next commit. If anything does not fit (the log is not what it was) the database stays failed and is reopened, as it was before. Measured: a fault at
  every file call of a workload (EIO, ENOSPC, a torn write, fsync, rename), once or from there on, then lifted: the connection takes transactions again, every acknowledged one is there, none is
  half, and the metadata agrees with the rows.
- **A write that finds the disk full is tried again** for up to 2 s (`MW_ENOSPC_WAIT_MS`) before the log is failed: the bytes are still in the staging ring and nobody was told. This covers
  another process taking the space between the reservation and the write.
- **The shared mode (several processes) does not roll back.** A commit is visible to the other processes before its fsync (an acknowledged commit is never lost), so a failed fsync cannot be
  taken back: that database is failed until it is reopened. A segment that cannot be made (no room) is not such a failure: it fails the one commit that needed it.
- **Linux.** The files the processes of the shared mode map together (`-mwidx`, `-mwrow`, `-mwown`, `-mwlock`) are sparse files of some hundred MB of which a few pages are touched. On a full
  file system a first touch of a page of a sparse file is a SIGBUS, not an error (measured on tmpfs: the first test run was killed by signal 7 in every shared-mode scenario). On Linux they
  therefore live in `/dev/shm` (`MW_SIDECAR_DIR`; `db` puts them next to the database); what they hold is rebuilt by the first process that opens the database. The log's reservation uses
  `fallocate(KEEP_SIZE)`. Tested in a container (gcc 14, arm64): the whole suite, `mw_ioerr`, and `mw_diskfull` on a size-limited tmpfs (`MW_DF_FS=tmpfs`, the default there).
- **Found on Linux.** A committer waiting for the group fsync compared file offsets: a compaction that finds nothing in flight cuts the log back to its header (offsets restart) while a commit that the group leader has just made visible is still on its way out of the wait, and it waited for ever (1 run of 20 of `mw_compact`). It now also leaves when everything up to its epoch is durable.
- **Closing with metadata that could not be flushed** (the last connection closes on a full disk): the log is kept, not dropped as compacted, and the next open replays it.
- **A tracked database never runs untracked.** If the capture cannot build the metadata of a commit (the metadata store cannot be read, no memory, the maps of the shared mode cannot be
  attached) the commit is refused with the error; it is not written without its metadata. The state of the store (list of runs, identity, epochs, the filter of known keys) is never
  assumed empty because a read failed: a failed read is an error, not a default.
- **Opening.** A failed open gives everything back (in the shared mode the exclusive lock on the header file, which the other processes wait on while the first one initialises, is
  released and no connection of the metadata store is opened for a database that did not finish opening).
- **Tests.** `make test-io`. `mw_ioerr`: every file call of a workload (log and segment writes, fsync, truncation, rename, mapping, reads) fails in turn, once or from there on (a disk
  that stays full), or writes half of its blocks first; in the thread mode and the shared mode; after each, the database is opened again and must hold every acknowledged
  transaction, none half, with its metadata agreeing with the rows, an intact b-tree, and take new transactions. `mw_diskfull`: the same on a real volume (a disk image that is filled
  to the brim before the open, during the workload, after the open), with no SIGBUS from the mapped files, and the database intact afterwards. These found: a deadlock of the open when
  the first opener failed, metadata silently lost (an I/O error taken for "nothing there" in four places, a failed attach of the maps), a commit of a stale schema written without its
  metadata in the shared mode, and a false conflict in the shared mode when the versions of a bucket were retired between the read and the validation.

## Capture and DDL

Tables are tracked when they are not internal (`mw_*`, `sqlite_*`) and are not virtual: with an explicit primary key (INTEGER, composite, text, WITHOUT ROWID), or without one, in which case the **rowid is the key** (a column that is itself called `rowid` moves the key to `_rowid_` or `oid`). A table without a primary key has no identity that is the same on every peer: if peers insert into it concurrently, equal rowids are taken for the same row (a choice: the application that wants otherwise declares a primary key).
- A table created and filled in one transaction: its rows are captured (the catalog of the new schema is built from the transaction's own pages).
- ADD / DROP COLUMN, and a table recreated with another layout: rows are matched by content and cells by column name (a column that is new to the record of a rewritten
  row gets a cell, the default of the column is not known to the capture).
- DROP TABLE removes the cells of the table (it is not a delete of its rows: sqlite-sync's cleanup does the same); dead cells are filtered by db_version until a flush deletes them.
- VACUUM (recognised as a statement) changes no row logically and produces no changes. RENAME TABLE starts the new name clean (the history of the old name goes).
- Savepoints and rollbacks leave no trace (nothing is captured before the commit); triggers and foreign-key actions are rows like any other.

## Sync

`mw_sync_export(db, since, &payload, &len, &upto)`: every change with db_version in `(since, upto]`, as the container of sqlite-sync (header `CLSY`, LZ4, tuples
`(tbl, pk, col_name, col_value, col_version, db_version, site_id, cl, seq)`), so peers of either implementation understand each other (the payload is LZ4-compressed by default; `MW_SYNC_COMPRESS=0` writes it uncompressed, `expanded_size` 0 as the container allows: measured no difference in apply time, 4x the size; both forms are always decoded); `mw_sync_apply(db, payload, len, &stats)`:
the merge in one transaction, all or nothing, retried on conflicts, holding the commit gate of the process when it keeps losing. The merge decisions are made on the metadata
overlay of the transaction; the commit then *declares* its metadata (the merge's cells) instead of having it derived from the pages, and the extension of the commit carries it.
`mw_sync_backfill` creates the metadata of rows that exist without any (a database that had data before the capture was on).

A difference with sqlite-sync that is by design: versions are counted per commit, not per statement (a row changed twice in one transaction gets one version bump, not two).
Cells of equal version and equal value are won by the larger site id (sqlite-sync's `merge_equal_values`); the site of a causal-length entry is whoever got there first when two
peers delete or resurrect the same row, as in sqlite-sync; the *version* is what converges.

## What is covered, what is not

Covered and tested: rowid tables with and without INTEGER PRIMARY KEY, TEXT / composite / BLOB keys, WITHOUT ROWID tables, key changes, overflow values (including overflow-only
updates), tables with more than 63 columns, values of every type, DDL as above, savepoints and rollbacks, triggers, foreign-key actions, upserts, updates that change nothing,
threads and processes, crashes (SIGKILL at any moment, flush at any pace, log compacting), concurrent applies and local writes, convergence of several peers.
Not covered (by design, version 1): block-level (text) merging, the DWS/AWS algorithms, filters/row-level security, the network layer, attached databases, virtual tables
(their shadow tables are tables; the virtual table itself is not tracked), the multi-process mode with private
stores (`mw_mp=3`: the metadata design needs the shared mode; opening it with `mw_cdc=1` is refused). Platforms: macOS; Linux, iOS, Windows, Android later.

## Tests

`make test` (the engine and the metadata: capture against a model, the store across restarts and SIGKILL, DDL, sync convergence and atomicity, multi-process with a live reader
and SIGKILL rounds), `make oracle-test` (differential tests against sqlite-sync: primary-key encoding, local generation, merge of 37 thousand changes, 4000 transactions of the
live store, features, 300 rounds of two peers exchanging payloads through the real encoder and decoder), `make test-mp` (the metadata tests again with the processes mode), `make test-io` (errors of the file system and a full disk: see above). All of it also runs on Linux (a gcc 14 container, `--privileged` for the size-limited tmpfs of `mw_diskfull`; `kill -USR1 <pid>` of a test prints the stacks of its threads).
