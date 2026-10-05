# sqlite-multiwriter: design

## What it is

A wrapper VFS that lets many connections (threads and processes) write one SQLite database concurrently (page-level optimistic concurrency, a commit log with group
commit, versioned pages, compaction into the database file: `docs/engine-history.md` has the measurements behind each choice of the engine), and that captures, for
**every** table, the row-level changes of every commit, so that the database always carries the state a CRDT (the algorithms of sqlite-sync) needs. No SQLite source
is changed, no SQL syntax is added, triggers and hooks are not used (they do not exist for WITHOUT ROWID tables, the pre-update hook needs a compile option, and an
application can replace the update hook of its connection).

sqlite-sync (`deps/sqlite-sync`, an optional submodule: only for `make oracle-test`) is the reference for the algorithms and the wire format and the oracle of the differential tests. Its SQL-level API
(`database.h`, the metadata tables built by triggers, the `cloudsync_*` functions) is not used; nothing of it is linked into the library: SQLite and LZ4 are vendored in `third_party/` (SQLite 3.53.4, LZ4 1.9.x, copied unchanged from the submodule), and the wire format is kept compatible, as the oracle tests check.

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

### What limits the throughput with the capture on (measured, bulk inserts of 100 rows, synchronous=FULL)
- **The flusher and the merger of the metadata.** With the capture on, every row written is also a row of a run of the file tables, and the runs are merged (tiers of 8, about 3 rewrites of a row). At
  20k commits a second that is 2M rows a second written and 6M merged: the flusher and the merger are the first threads of the process to be 100% busy, and when the merger falls behind the
  number of runs slows the writers (back-pressure) - the usual reason for a throughput that is half of what a short run gave. What was done, in the order of what it gave (16 threads, 30-40 s):
  the changes wait for the flusher as packed rows in the stripes (a flush takes the buffers and visits no entry); entries come from free lists; a flush writes 131072 rows, not 16384 (130 runs a
  second were more than the merger could take: 4 threads gave 8k or 20k tx/s depending on whether it ever fell behind); 8192 slots a reservation; the k-way merge by a heap with the first 8 bytes of the
  key compared as a number; the runs are built before the write lock and in three helper threads. 16 threads: 19k -> 28.8k tx/s sustained; 32 threads 22k -> 28k.
- **Several processes.** The flushers of all the processes pace themselves by one clock (a second flusher behind the first wrote a run of a few rows); the compactor decided "by size" from a counter the
  shared mode does not keep (it ran on its 0.5 s timer, the log sat at 270 MB, where the commits are slowed): 4 processes 12.4k -> 17.4k tx/s, and untracked 17.5k -> 25k.
- **What is left** (one process, 16 threads: 31k against 46k untracked; 8 processes 15k against 25k): the capture's own work on the way of a commit (the rows of the pages decoded and the CRDT state of
  every row looked up: ~30 us outside the locks, ~20 us in the publication of the shared mode) and the flush itself (0.25-0.4 us a row in one thread).
- **Smaller cells (tried, neutral).** The cells of a row (extension of a commit, rows of the runs) are written as deltas: the first whole, each next one a control byte that says which of
  column+1, version, db_version, site and sequence repeat (`cz_put`/`cz_get`; row format 2, extension 0x4f; format-1 rows are still read). A row of 100 narrow cells took ~7 bytes a cell and takes ~1.
  Measured (30 s): 16 threads 28.2k tx/s (tracked) against 45.4k untracked; 4 processes 14.8k against 25.6k; 8 processes 15.0k against 24.8k - no gain over before: the volume of the metadata is not what limits the
  gap, the CPU of the capture, the apply and the merge is. Kept for the smaller log and runs.
- **Less CPU in the capture and the apply (measured).** The rows of the memory table are kept packed (the form of a row of a run, ~40 bytes for 17 cells instead of 550): a commit packs a row once, outside
  the lock of its stripe, and the entry and the change that waits for the flusher take a copy of it (before: a copy of the cells, then a second packing). The owner map of the row diff takes only the
  children of an interior page that the commit wrote itself (a root page lists hundreds, and most commits rewrite it) and stops when a pass finds nothing new (it always made three). The stripes spin a little
  before sleeping. 1 thread 8.7k -> 8.9k, 4 threads 15.9k -> 17.2k, 16 threads 29.8k -> 29.8k tx/s (the same within the noise).
- **Why 16 threads did not move.** With `MW_EXP_NOAPPLY=1` (nothing is installed, so nothing is flushed or merged) 16 threads give 40k tx/s against 30k, but the part that is the commit's own CPU is small
  (a row's install is 0.3 us waiting for its stripe + 0.4 us holding it, ~70 us a commit of 520). The rest is the merger: its thread is 100% busy, half of that in the small transactions that write its
  parts and in reading its inputs through the page store, and the run-count back-pressure takes ~10% of the writers' time (`MW_META_MAX_RUNS=100000` gives 34k for 20 s, but the backlog then grows
  without limit: 960 age groups after 20 s). More merger threads (levels split between 2, 3, 4 threads), a fanout of 16 or 32 and bigger parts all measured the same or worse: the merges do not
  scale because they share the page store and the write turn of the file tables. The way ahead is a cheaper merge (fewer small transactions, reading the inputs without going through the page
  store), not more CPU saved in the commit.
- **A cheaper merge (measured).** The merger thread is 100% busy at 16 threads: of its ~55 ms a merge, ~22% reads the input blocks (a snapshot and four slot reads through the page store each), ~35% writes the
  output parts (small transactions: the commit is 60% of them) and the rest is CPU (decode, heap merge, LZ4). Changes: the run numbers of a merge are taken once (a range of 16) instead of one transaction a
  part; the merge reads without checking that the run is still listed when this process alone removes runs (thread mode); and a finished part is written by a thread of its own with a connection of its own
  while the next one is merged (thread mode only: with processes it cost more than it gave). 16 threads 28.7k -> 29.5k tx/s (30 s, three pairs), 4 and 8 processes unchanged.
  Tried and dropped: the levels split among 2-4 merger threads; one merge cut into key ranges merged by 2 threads (pivots at fences of the largest input); merge connections with `synchronous=OFF` (the
  order of the log makes it safe, but the commit time barely moved). None of them gave more, because the small transactions of the merge, the flush and the writers' own commits take turns on
  the same b-trees (`rsx_wlock`) and meet in the page store.
- **What the page store costs everybody.** 10% of the reads of a page wait for the stripe lock, and 85% of those are for page 1: the header is rewritten by every commit that grows the file (the database
  size), and every transaction reads it. The workers spend 8.5% (untracked) to 13.6% (tracked) of their time in `mw_store_read` waiting; more spinning (`MW_SPIN_US` 500, 5000) made it worse. This is
  the next lever for both variants: a lock-free path for the newest version of page 1 (the size and the cookie are already kept in atomics).
- **Page 1 without the lock (tried, dropped).** A ring of the last 64 versions of page 1 read under a seqlock (the installing commit writes it under the page's stripe; a reader takes the newest slot not newer
  than its snapshot, checks the sequence number after the copy, and falls back to the lock if a writer was in the way; 8 slots were not enough: commits are installed before they are visible, and a group of
  the log is longer than that). It worked as intended - contended reads of the store went from 10% to 2.3% and page 1 left the list - and nothing else moved: 16 threads 31.1k / 30.4k / 30.1k / 30.1k
  against 30.9k / 30.8k / 31.2k / 31.3k tx/s (tracked), 4/32/64 threads untracked within 1.5%, latency p50 and CPU the same. The waiting moved to the root page of the table (the same stripe held for the
  ~8 us of a publication by every commit that rewrites it), and the sampled "time in mw_store_read" was waiting that overlaps with other waits (the group commit), not a cost on the critical path. Not kept:
  more code in the page store for no measurable gain. (Also: after a few runs of 4 GB each the machine throttles - 28k became 21k for both binaries until it rested - so a comparison needs
  alternating runs with pauses.)
- **The lock of publication of a hot page (measured, fixed).** A commit holds the stripes of the pages it writes from the validation to the install, ~7 us; 4.7 of them were waiting for `seq_mu` (the lock that
  assigns epochs and log offsets), and 97% of that wait came from 0.09% of the commits: the ones that met the compactor, which rewrote the tail of the log under `seq_mu` (550 times in 20 s; per rewrite: ~43 MB
  of tail, wait for the writes in flight 0.7 ms, copy 3.7 ms, fsync 0.45 ms, rename and swap 0.65 ms = 5.5 ms with every committer stopped, holding its stripes, the root page of the table among them).
  Now (staged log, one process) the compactor copies the records that are durable into the new file and fsyncs it *before* taking `seq_mu`, a second round copies what came in meanwhile, and under the lock only
  the last records are copied (`mw_log_rewrite_prepare` / `mw_log_rewrite_tail(.., prep)`; the old path stays for the multi-process mode). 16 threads, 15 s, three alternating pairs: untracked 45.9k -> 48.3k tx/s and p99
  2.8 -> 1.07 ms; tracked 30.9k -> 30.9k tx/s and p99 5.3 -> 1.9 ms. (A log limit of 128 MB gave the same p99 without the change, 512 MB made it worse: 20k tx/s, p99 21 ms.) Test `mw_logrewrite`
  (1 MB limit, 8 committers, then SIGKILL at ten different moments: every acknowledged commit is there, the integrity check passes).
- **A merge that costs less changes nothing now (measured, nothing kept).** `bench/bench_merge.c` merges 8 runs of rows like the benchmark's in memory: 45 ns a row (22 M rows/s), 14.3 bytes a row stored.
  The merger thread of a real run does ~5.5 M rows/s, so the merge proper is a quarter of its time; the rest is reading blocks and writing parts through the engine. What was tried, each against the same
  binary in alternating 30 s runs (29.4k tx/s, tracked, 16 threads): the inputs of a merge removed by the writer thread while the next merge starts (the runs a merge is working on are excluded from the
  next choice) 29.6-29.9k; the level 0 merged between the parts of a big merge (the runs of the flushes pile up behind a merge of the second level, 8 times the size) no better; a second merger thread for
  the level 0 22-28k (worse: the two take turns on the same b-trees and the page store, RSS up to 2.7 GB); a flush of 4 or 8 times more rows, parts of 64k or 128k rows, fanouts 6 and 12, `LZ4` acceleration
  4 and 8: all within the noise (+-3%); letting the age groups of all levels go to 6 or 12 times the limit (`all - 3 * MW_META_MAX_RUNS` today) +4-5%, and with no back-pressure at all 38k tx/s for 20-30 s, but
  then the merges are starved (35 M rows merged against 100 M) and the backlog grows without limit.
- **Why: the merges and the flushes share the log with the writers.** In a 20 s run the log carried 18.8 GB of user commits (32 KB each) and 3.7 GB of commits of the metadata (21 thousand, 179 KB each, the
  blocks of the runs as page images): a sixth more bytes through the one group-commit pipeline, and the transactions of the merge and the flush take turns on the b-trees of `mw_slots` / `mw_free`
  (`rsx_wlock`). The process uses ~9.8 of the 18 cores, so it is not the CPU. What would cut it is writing fewer bytes: the blocks hold 14.5 bytes a row (9 of them the key, ascending: a key stored as a
  difference from the previous one with restart points would save ~40%), or fewer passes (every row is written once by the flush and once at each level: 4 times in a run of 60 s).
- **Fewer bytes a row in the blocks (measured, kept).** The blocks of the runs (form 3) store a key as the difference from the previous one (the shared prefix and the rest), the db_version as the difference
  from the previous row, and keep an offset only for every 32nd row (a row is found from the restart before it; form 1 is still read): the table of offsets was 4 bytes a row and the keys of an ascending
  range differ in their last byte. `bench/bench_merge.c`: 14.3 -> 8.8 bytes a row stored (restart every 16 rows: 9.1; 64: 8.6); in a real run of the benchmark 14.6 -> 10.0 bytes a row in the slots (the
  Bloom filter of the meta is 1.25 more, and the padding of the last slot of a block). Less in the log (the commits of the metadata) and less to compress and decompress; the merge CPU is the same (47 ns
  a row). 16 threads tracked 29.6k -> 32.2k tx/s (three alternating pairs of 30 s), 4 processes 14.9k -> 15.5k.
- **The Bloom filter of a run (measured; the default stays).** It is 10 bits a row (1.25 of the ~11.3 bytes a row that a run costs in the file, 115 MB of memory for the 2800 runs of a 20 s benchmark) with 7
  probes. Its CPU is nothing (`bench_merge` with 2 bits a row: the same 47 ns a row; the reading back of the metas in `man_load`: 31 samples in 40 thousand) and a filter of 2 bits a row changed
  the throughput by +1%: what it costs is bytes and memory. The size is now chosen when a run is built (`mw_meta_bloom_bits=<n>` in the URI or `MW_META_BLOOM_BITS`, 2..32; the number of probes follows,
  and is kept in the low bits of the size in the meta of the run - a size is a multiple of 64 - so every run says how it was built, and the first metas, with 0 there, mean 7). What a bit
  buys, measured on 20 thousand absent keys (`mw_runs`): 6 bits 5.6% of false positives, 8 bits 2.2%, 10 bits 0.83%, 12 bits 0.33%. In the benchmark (16 threads, 30 s, two runs each): 10 bits 32.0k tx/s
  1462 MB, 8 bits 32.4k 1360 MB, 6 bits 32.9k 1120 MB. A false positive costs a read of a block in every run whose key range holds the key, and the runs are many (a hundred or more groups behind a busy
  merger), so a lookup of a row that is not in the memory table pays 0.8, 2.2 or 5.6 reads for 10, 8 or 6 bits per 100 runs: the default is kept for the databases that update rows from the file;
  a database that mostly inserts can take 6 and save a fifth of its memory. (The memory that goes with the bits is several times the size of the filters, 340 MB for 4 bits a row against 46 MB of
  filters: the allocations of the builders are most of it, not looked into further.)
- **The db_version of the cells (kept).** The packed cells of a row (format 3) keep the db_version of a cell as the difference from the largest one of the row (0 for the rows a commit wrote), because the row
  already carries that number: in the block (as a difference from the previous row), in the item of the flush and in the entry of the memory table (`rdv`). The first cell had it whole, 3-4 bytes that LZ4 could
  not squeeze. `mw_meta_row_cells` takes the db_version of the row; formats 1 and 2 are still read. A real run: 10.0 -> 7.9 bytes a row in the slots (meta included); 16 threads tracked 31.9k -> 33.7k tx/s
  (three alternating pairs of 30 s), 4 processes the same.
- **The capture and the apply in the commit (measured, kept: less CPU, same tx/s).** A commit's rows are packed into one arena outside any lock and installed stripe by stripe (one lock for all the rows of the commit
  that live in a stripe, in their order), with the counters of the table (rows, bytes, dirty) added once a commit instead of once a row (three atomics a row on lines that the 16 committers share); the
  runs of cells that follow the usual pattern (next column, same version, db_version and site, next sequence number) are a byte and a count in the extension and in the packed rows (a row of 17 columns:
  the first cell and 2 bytes; extension 0x50, row format 4; the older ones are read); the row diff reads an old page into the buffer it keeps instead of copying it, reserves the rows of a page at once, and the
  hash of a key is computed once for the filter and the table. `prepare` 36.8 -> 33.9 us a commit, CPU of the run -6% (315 -> 296 s of 30 s), p50 361 -> 352 us; the throughput does not move
  (33.5k tx/s before and after, three alternating pairs; 4 processes the same): the 16 committers' cycle is no longer limited by what they do, but by the back-pressure of the merges (7.7% of their time, 24.5 s
  of 320) and the log that the merges share with them (see above).
- **Where the memory went, and two leaks of address space that were fixed (measured).** `vmmap` / `heap` / `malloc_history` on 16 tracked threads after 28 s: 1.1 GB live in 2.3 million allocations, 19%
  fragmentation (so the allocator was not the problem). By stack, at 140 s (1.28 GB live): 46% `rs_run_decode`, which allocated the arenas of the fence keys and of the locators of a run with the size
  of its whole meta (the filter included) - three 49 KB blocks for every run, 1 needed; 17% the arrays of versions of the page chains (`chain_reserve`: 48-112 bytes for each of the 1.4 million pages
  that were ever written, kept for ever after the garbage collector had dropped their versions); 6% the base cache of pages (cap 64 MB), 8% the memory table (cap 64 MB), 13% SQLite's page caches and the
  benchmark's own buffers. Now the arenas of a run are measured first and allocated exactly, and a chain whose versions are all gone gives its array back. 28 s: footprint 1.1 GB -> 0.62 GB,
  2.3 M -> 89 thousand allocations; 60 s: RSS 2.4 GB -> 1.35-1.42 GB (two runs each); tx/s unchanged (31.2-32.2k before, 31.6-32.1k after), p99 the same. What is left (580 MB live at 28 s): the filters
  (180 MB for 3750 runs: `mw_meta_bloom_bits`), the slabs of the memory table (115 MB), pending rows (70 MB), the base cache (54 MB).
- **What is left of the memory, and why it stays (analysed, nothing changed).** At 28 s, 16 tracked threads: 580 MB live, 640 MB footprint. Slabs of the memory table with 32 KB instead of 256 KB: 115 -> 87 MB
  in the histogram, 579 -> 547 MB allocated, footprint the same (639 against 618 MB, within the noise): reverted. The pending rows (70 MB) cannot share the bytes of the table's entries without the flusher
  visiting the entries again (a cache miss each: it was a quarter of its time before the rows were packed into the stripes' buffers) or the entries copying their cells when a flush ends (the same visit);
  that is at most 11% of what is left, paid in the CPU of the thread that is already the busiest. The filters (180 MB) are the one big item, and shrinking them trades false positives for lookups
  (`mw_meta_bloom_bits`). The base cache (54 MB) and the memory table (64 MB) are caps, set by `MW_META_CACHE_MB`.
- **Durability against a power failure (F_FULLFSYNC).** On macOS `fsync` leaves the data in the drive's cache; SQLite has the same gap and closes it with `PRAGMA fullfsync` (off by default). The log, the
  compaction and the other files of the engine now go through `mw_sys_fsync`, which uses `fcntl(F_FULLFSYNC)` when `mw_fullfsync=1` is in the URI (or `MW_FULLFSYNC=1`; process-wide; if the file system
  does not know it, the plain `fsync`, as SQLite does). Linux's `fsync` already flushes the device. Off by default, like SQLite. The price is the drive's: stock SQLite (WAL, FULL, one writer) 10.1k -> 264 tx/s; here
  one thread 9.9k -> 272 tx/s, but 16 threads 33.5k -> 1532 tx/s: the group commit shares each flush (5.8 times SQLite's best case). A real power cut was not tried (the machine is the only one); what is tested is that the path
  works, that an injected failure of the flush fails the commit, and that the data is there after a reopen and a SIGKILL (`mw_fullfsync`; the staged-log and durability tests also pass with it on).
- **A profile for small devices (`mw_profile=small`, `MW_PROFILE=small`).** The memory table of the metadata and the base cache of pages at 8 MB instead of 64 (`mw_meta_cache_mb`, `mw_base_cache_mb`), filters
  of 8 bits a row, a log of 16 MB before it is compacted. One thread, bulk inserts for 15 s: RSS 304 -> 165 MB (a 2 s run: 192 -> 95 MB), tx/s the same (9.7k -> 10.1k); 16 threads 856 -> 652 MB with
  the same tx/s. What is left at one thread is the run metas (44 MB: the filters), the chain directory (19 MB) and the buffers of SQLite. Test `mw_profile`.
- **The single thread and the many processes (analysed, nothing changed).** One thread tracked is 9.3-9.9k tx/s against SQLite's 12.3k: 26% of the 100 us of a commit is the wait for the fsync, 11% is the capture, 11% the
  apply of the cells and 5% the append of the log record; with one committer the apply cannot overlap anything but the fsync, and the hand-over to another thread costs as much as it hides. Many processes: each spends 58% of its
  time in the queue for the publication lock (`mw_mp_lock`), which one process holds at a time for ~49 us (47 of them the relocation of the new pages, the append of the record and the install in the shared index); 16 processes
  fill 72% of that, so the ceiling is ~20k tx/s and the measure 14-16k. Shorter holds would need the record to be copied outside the lock or a log for each process: a redesign, not a change. The limit of the backlog
  of the merges is now 6 times the limit of level 0 (`MW_META_ALL_MULT`, was 3): 16 threads, 60 s, two runs each, 32.05k -> 32.7k tx/s, p99 1.88 -> 1.77 ms, RSS 1.38 -> 1.2 GB.
- **The other workloads (measured; `bench/results/new_workloads_2026-10-04.md`).** 16 threads / 8 processes against stock SQLite (WAL, FULL), everything tracked. Independent updates 80.7k / 44.9k tx/s against 14.6k / 14.5k;
  mixed read-write 380k / 202k against 74k / 94k; hot rows, the same page, other columns of a row 37-39k against 11-12k; inserts (uuid, int, autoincrement, crdt) 28-33k against 11-17k; wide rows 32k / 15k against 8k;
  read only the same with threads (861k, 883k) and twice SQLite with processes (559k, 283k). Retries: 0 for the independent and read ones, ~10-13% of the transactions with threads and 78-86% with processes on
  the contended ones (hot, samepage, cols, inserts: processes retry on a conflict of the shared index), 0 given up in all but one run (below). Where it does not win: **random keys** (UUID-like bulk, 100 rows spread over the
  whole key space) every engine is bound by the cache misses: 449 / 362 tx/s against 454 / 460 (with processes 21% under SQLite, 54% retries, and one transaction in the thread run that gave up after 1000 retries);
  **transactions that are held open 5 ms on hot rows or on the same page** 194 against 139 (the engine serialises what conflicts, as it should, but a fifth of what the independent rows give: 16 writers
  that hold their page for 5 ms each can only commit one at a time); **a long reader** (a connection that keeps its snapshot for the whole run) 4.6k / 4.2k tx/s against 12.2k / 11.4k and 806 MB of memory, see next.
- **A reader that never ends (known limit, not fixed).** The compactor writes the pages of the log into the file only up to the oldest snapshot still in use (a reader that is older than the file would read a page that
  is newer than its snapshot, and the garbage collector cannot drop the versions that it needs): with a reader pinned at epoch 2 nothing is compacted, the log fills to 16 times its limit and every commit is delayed in proportion
  (0.5 ms at the limit, up to 50 ms; at 768 MB a commit waits for room, at most 20 s), the retained versions fill the memory (755 MB in 22 s, `reclaimed=0`), and the writers are down to 2.5k tx/s. SQLite has the
  same pin (its WAL cannot be checkpointed) but lets the log grow and does not slow down. What would remove it is keeping what the old reader needs apart from the file (the pre-images of the pages that the compactor overwrites,
  as a rollback journal does), or ending a reader that stays too long; neither is done: it is a design decision.
- **Ten minutes at 16 threads (soak, bulk, tracked, a database that grows to 115 GB).** 22.6k tx/s on average (33k in the first minute: the numbers fall with the size of the database and with the heat of the machine), RSS 0.8 GB
  at 30 s, 2.6 GB at 211 s and 3.4-3.55 GB from 300 s on (it stops growing), the log 13.6 MB at the end, the files of the engine 5-372 MB, the backlog of the merges bounded (58 age groups at level 0, 187 in all, 24 thousand
  runs), 4393 merges of 2.2 billion rows, 2.05 million commits delayed by the back-pressure (1778 s of the 9600 thread-seconds). The database of 115 GB opens at once (18 MB of RSS to read it); the integrity check takes 30 s.
  One transaction ended with an error in this run, as in 3 of ~60 other runs of 10-60 s (never in 20 runs with `MW_BENCH_DEBUG=1`, which prints the error code and message; not found yet).
- **Corrections after a review of the whole repository (five readers, by area; the ones that were checked in the code were fixed).** (1) A commit that is taken back because its log record cannot be written now puts back the head
  and the cookie of page 1 (and drops the copy of it that relocations keep): they stayed at the epoch of a commit that never was, which made every later commit see a schema change. (2) A connection that closes while it holds the DDL
  barrier (a `CREATE TEMP TABLE` takes it without a snapshot of the main file) gives it back; the pointer to the closed connection stayed in `ddl_owner` and every writer got `SQLITE_BUSY` for ever (`mw_rollback` fails with `BUSY`
  without the fix). (3) The recovery in place (`mw_meta_reset`) forgets the list of runs, the blocks and the slots that the run store had cached or taken (`rsx_reset`): the file tables went back in time, and run numbers and versions are reused.
  (4) `mw_sync_apply`, the backfill and the declared commit check that no row of the overlay failed to load: a row that could not be read is not a row that is not there. (5) The knobs of measurement that throw
  metadata away (`MW_EXP_NOEXT`, `MW_EXP_NOAPPLY`, `MW_EXP_SKIPWRITE`, `MW_META_NOMERGE`, `MW_META_NOCOMPRESS`) and `MW_MP_PRIVATE_OK` are no longer read by a normal build (`make EXPERIMENTS=1`).
  Reported and **not** fixed yet: the order of the compaction and of the fsync of the log in the shared mode (the file can get ahead of what is durable at a power failure), the epoch of own pages of a lane after several
  commits in one snapshot, the unlink order of the lock files, the ticket lock after `kill -9`, and the recovery of the log on read errors (see the review).
- **Second group of corrections (durability, recovery, locks).** (1) Opening a log: a header that does not check out, with records behind it, is `SQLITE_CORRUPT` (it emptied the log: the commits were thrown away in
  silence); a read that fails during the replay is an error (it was taken for the end of the log, which was then cut there: all the commits after it were lost; `mw_logopen` fails on both without the fix, with 4 failures);
  the cut of a torn tail, the cut of the recovery in place and the rename of a rewritten log are made durable (`fsync` of the file and of the directory); the same for the segments of the shared log, whose replay stops
  at the end of the data but fails on a read that fails, and whose tail is cleared with the errors checked. (2) Shared mode: a failed fsync of the log fails every commit that is not durable yet, in every process, until
  the log is closed by all (a later fsync that succeeds would have acknowledged data that may not be on the disk: `mw_syfail`, the commit of the second process went through before); the compaction makes the log durable up
  to the commits whose pages it writes before it writes them, in the shared mode and in the single-process mode with `synchronous` below FULL (`mw_compactsync`: no sync before, one after; a power failure could leave the file ahead of
  the log). (3) The roll to a new segment sets the segment before the offset in it, and the repair of a dead publisher puts the offset right if it died between the two. (4) The lock files of the shared mode are unlinked
  in the order that cannot leave two processes locking different files; the ticket lock skips a ticket whose owner never wrote its pid (500 ms without progress) and a process whose ticket was skipped takes a new one.
  (5) A lane validates each page that it wrote against the commit that wrote it (`own_ep`), keeps the size of its own last commit, and a snapshot that had a relocated commit takes no more commits (`MW_CONFLICT_READ`): the
  reviewers' scenario of a lost update turned out to be caught earlier (the pages that a snapshot read are validated too), so `mw_ownepoch` is a guard and not a proof. (6) The extension and the sync payload check
  the sizes they are given. Not done: the epochs in the table of 256 first-epochs of the segments with more than 256 live segments, the pid reuse of the processes, the publisher that dies in the garbage collection,
  the orphan record after a failed install in the shared mode, `mm_head` that takes every error for an empty bucket, and the permissions of the sidecars in /dev/shm. The ticket lock and the unlink order were not tested
  (no way to stop a process in those windows from a test).

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

## Parameters (URI and environment)

URI parameters (`file:db?mw=1&...`, read at open): `mw` (0/1/2, the engine), `mw_mp` (0 one process, 1 processes in shared mode, 2 shared, 3 private stores), `mw_cdc` (1: capture of the changes in the commit),
`mw_sys` (internal connections of the metadata store), `mw_fullfsync` (F_FULLFSYNC on macOS), `mw_profile` (`small`: caches 8 MB, Bloom 8 bits, log 16 MB), `cache`, `mw_base_cache_mb`,
`mw_meta_cache_mb`, `mw_meta_bloom_bits`, `mw_log_max_mb`, `mw_gc`, `mw_hot_credit`, `mw_readcheck`, `mw_noreloc`, `mw_noroute`, `mw_nomerge` (tests and experiments: not for production).
`mw_norebase` belongs to the first prototype and is a no-op for the metadata path.

Environment (defaults of the same knobs and diagnostics): `MW_FULLFSYNC`, `MW_PROFILE`, `MW_META_CACHE_MB`, `MW_META_BLOOM_BITS`, `MW_META_ALL_MULT` (back-pressure, in run groups), `MW_META_FLUSH_MS|ROWS`,
`MW_META_FANOUT`, `MW_META_MAX_RUNS`, `MW_META_PART_ROWS`, `MW_META_BUILDERS`, `MW_META_KICK_MS`, `MW_META_SWEEP_MS`, `MW_META_LZ4_ACCEL`, `MW_SEG_MB`, `MW_IDX_ENTRIES`, `MW_ROWIDX_ENTRIES`,
`MW_SIDECAR_DIR` (where the shared maps go: `/dev/shm` on Linux), `MW_MP_LAZY`, `MW_MP_PRIVATE`, `MW_SPIN_US`, `MW_POOL_BATCH`, `MW_SYNC_COMPRESS`; diagnostics: `MW_DEBUG`, `MW_TIMING`, `MW_IO_TRACE`,
`MW_CDC_STATS`, `MW_COMPACT_TRACE`. Measurement only (built with `make EXPERIMENTS=1`): `MW_EXP_NOEXT`, `MW_EXP_NOAPPLY`, `MW_META_NOMERGE`, `MW_META_NOCOMPRESS`.

## Security of the files

The log, the segments, the shared maps and the lock files are created with the mode of the database file (without execute bits; 0600 if it does not exist yet) and opened with `O_NOFOLLOW`; the
CDC map is created with `O_EXCL` after an unlink. A sidecar in `/dev/shm` therefore has the same readers as the database. Not covered: a hostile local user who owns the directory of the database.

## License

The code in this repository is under the licence in `LICENSE.md`. `third_party/` holds SQLite (public domain) and LZ4 (BSD 2-clause), each with its own licence; `deps/sqlite-sync` (optional, oracle tests only) keeps its own.

## Third group of corrections (open items of the review)

Fixed: the first epoch of a segment is kept in a tagged table (no more `%256` collisions) with a fallback to the segment header; the entries of a dead process whose pid was reused are dropped at
registration and repaired at open; a publisher that dies inside GC is repaired (`gc_active`, `shidx_gc_repair`); a record appended before a failed shared install is undone, and a failed `mm_install` marks the
database `broken`; `mm_head`/`read_group` propagate I/O errors instead of reporting "absent"; a full site table is an error, not a silent ordinal; sidecars take the mode of the database and are opened with
`O_NOFOLLOW` (and `O_EXCL` for the CDC map). Removed: the vstore/vsshared prototypes and their benchmarks, `test/pending`. `mw_norebase`/the rebase path remain (still reachable from `lane_can_rebase`).
Tested: the whole suite, test-mp, test-io, test-stress, oracle-test, `mw_mpmeta` x3, ASan on the touched tests, Linux (gcc 14) on the same. Not proven by a dedicated test: pid reuse, GC death repair, the
`broken` flag, ticket stall, file modes. TSan reported one warning in one `mw_mpmeta` run that did not recur in three more (not analysed); `mw_syfail` cannot run under TSan (fork). No A/B perf run for this group.

## The drop above 16 threads with everything tracked (measured 2026-10-05, SQLite 3.53.4, 18 cores)

Bulk, 100 rows per transaction, tracked: 34.7k tx/s at 16 threads, 25.5k at 32, 24.4k at 64 (untracked: 53k at 32). The time was in the garbage collection that the committing thread runs every 64 publishes (`mw_db_gc`):
`gc_ms` 1.7 s at 16 threads against 69 s at 32 (22% of the thread time), `compaction` runs 286 -> 36. The profile put it in `__psynch_mutexwait` inside `mw_db_gc`: the collector took `list_mu` (a blocking mutex that every
committer also takes to queue its pages) once per surviving chain, and waited for every stripe a committer held. Now it keeps the survivors on a local list given back with one lock, and skips (and keeps for the next run) a chain whose
stripe is held (`trylock`). Result: gc_ms 1.9 s at 32 threads, 35.5k tx/s at 32, 33.5k at 64 (the profile runs; the comparison below: 37.2k and 34.7k). Tests: the whole suite, test-stress, ASan and TSan on the metadata tests; no dedicated test of the skipped chains.

| threads | mw tx/s | mw retries | mw failed | SQLite tx/s | SQLite retries | SQLite failed |
|---|---|---|---|---|---|---|
| 1 | 10971 | 21 | 0 | 14173 | 0 | 0 |
| 2 | 15784 | 74 | 0 | 9950 | 114735 | 0 |
| 4 | 23585 | 248 | 0 | 9338 | 126582 | 0 |
| 8 | 30046 | 474 | 0 | 9065 | 142154 | 0 |
| 16 | 34582 | 927 | 0 | 8668 | 171344 | 0 |
| 32 | 37174 | 2046 | 0 | 8643 | 230186 | 0 |
| 64 | 34699 | 5132 | 0 | 8268 | 337060 | 0 |

## Against Turso 0.8.1 (measured 2026-10-05, this Mac, 18 cores, synchronous=FULL, 100-row transactions on disjoint keys, 10 s, one run per point)

Turso through its Rust crate (`bench/turso_rs`: BEGIN CONCURRENT, `journal_mode=mvcc`, built from the cached 0.8.1 crates), SQLite stock WAL with the application retrying, and this engine tracked (CRDT capture of every table) and untracked. tx/s:

| threads | SQLite | Multi-Writer tracked | Multi-Writer untracked | Turso 0.8.1 | tracked / Turso | untracked / Turso |
|---|---|---|---|---|---|---|
| 1 | 14173 | 10971 | 15974 | 5616 | 1.95x | 2.84x |
| 2 | 9950 | 15784 | 22241 | 8376 | 1.88x | 2.66x |
| 4 | 9338 | 23585 | 35343 | 17075 | 1.38x | 2.07x |
| 8 | 9065 | 30046 | 40333 | 28800 | 1.04x | 1.40x |
| 16 | 8668 | 34582 | 49314 | 31571 | 1.10x | 1.56x |
| 32 | 8643 | 37174 | 54384 | 42987 | 0.86x | 1.27x |
| 64 | 8268 | 34699 | 47308 | 43120 | 0.80x | 1.10x |

What the post lists as the optimizations of 0.8 for writes (BEGIN CONCURRENT, MVCC with independent transaction starts, group commit) this engine already has (page versions with snapshot isolation, the group commit of the log with leader/followers). Nothing in
the list is missing; the others (FTS with MVCC, index merging, hash joins, subquery unnesting) are not about the write path. Untracked it is ahead of Turso at every point. Tracked it is behind above 16 threads: the capture of the changes costs 20-27% of the
throughput there (at 64 threads: the groups of the log are smaller, 9.7 records against 13.2, and a commit waits 1.5 ms against 1.2 ms), and Turso keeps no such metadata. That gap is the price of the capture, not a missing optimization; a closer match would need less work per
commit in the capture/apply path, which earlier sections already squeezed. Caveats: the post's own numbers are on another machine (12-core Ryzen, NVMe, Linux: 9.5k tx/s at 64 connections); on this Mac `fsync` does not flush the drive cache (F_FULLFSYNC does), for all the engines compared here.
Not measured here: the latency (Poisson) shape of the post and the processes mode.

## The cost of the capture above 16 threads, and the multi-process publication lock: what was tried (2026-10-05)

After the fix of the GC the tracked throughput no longer drops above 16 threads; it sits at about 72% of the untracked one at every thread count from 8 to 64 (35k against 49k at 16 threads). Ablations (experiments build, 32 threads): skipping the
insertion of the commit's cells into the memory table gives 50k tx/s (as untracked); skipping only the write of the metadata file changes nothing, nor does a flush every two seconds, one builder thread, a run limit of 6, 24 or 100 groups, 8 or 256
stripes instead of 64, another spin time of the stripe locks, or taking the stripes from a start that differs per commit with a second pass for the busy ones (all within the noise, 34-37k). Measured per commit at 32 threads: the insertion takes
60 us (33 us waiting for the 64 stripes, 0.34 us held per stripe), back-pressure from the runs 37 us on average, the capture about 25 us: together about the 130 us that a tracked commit takes more than an untracked one (the closed loop
runs at N / latency). Nothing found that removes it without removing the metadata; the insertion cannot move after the log sync because the group leader publishes the epochs of the whole written prefix.
`test/mw_applyvis.c` (12 writers on 8 hot rows, synchronous=FULL, `cv == 1 + 2n` for every row) guards the invariant that a commit's cells are in the store before the next commit of the same row computes its versions; it passes, also with an
artificial delay of the apply (`MW_EXP_APPLY_DELAY_US`, experiments build): epochs become visible before their apply only rarely (8 of 23000 commits with 300 us of delay, in the bench of the experiments build), and no row disagreed. The leader
of the staged log does publish epochs whose cells may not have been applied yet (`sync_staged`, `leader_vis`): not proven harmful, not proven safe.
Processes (8 processes, tracked, 16.8k tx/s): the publication lock is held 44.6 us per commit (log append 20 us, `mm_install` 7.5 us, the rest of the shared publish ~4 us) and waited for 159 us: utilisation of the lock about 79%, a ceiling of ~22k tx/s.
The merger's own commits go through the same lock. Candidates not done: build the body checksum and copy the record outside the lock (reserve the offset under it), a cheaper checksum, taking the metadata store's commits out of the lock.
