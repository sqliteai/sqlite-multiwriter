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
4. **Metadata store** (`multiwriter_meta*.c`, `multiwriter_mmeta.c`): per row the cells (column version, db_version, site, sequence). In one process a striped memory table
   in front of three ordinary tables of the database file; in several processes a shared index of row buckets into the log. See below.
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
`mw_state(k, v)` (`meta_epoch`, `dv_hwm`), `mw_sites(ord, id)` and `mw_rows(tbl, pk, dv, cells)` (WITHOUT ROWID, key `(tbl, pk)`), created by the first connection. A row of `mw_rows` is **one
row of a user table**: `dv` is the largest db_version among its cells and `cells` is the packed state of all of them (a format byte, the number of cells, then per cell the column + 1 so that the
sentinel is 0, the version, the db_version, the sequence and the site, as varints). The flush *replaces* the row: the cells are always the complete state, so nothing is read before it is written and a
cell that disappeared (a dropped column) disappears with it. Packing is what makes wide tables cheap: the flusher does one b-tree insert per row instead of one per cell (a table with five columns
besides its key flushes in about a quarter of the time, and tracked bulk inserts into it run 3.6 times faster: 2.4k to 8.6k tx/s at 8 threads). The index the export needs, `mw_rows_dv(dv)`, is created by the
first export (databases that never synchronise do not pay for it). `mw_cells` is a read-only virtual table over `mw_rows` with the columns of the cells (`tbl, pk, col, cv, dv, seq, site`; `col` -1 is
the causal length), for looking at the metadata in SQL. They are ordinary tables, so the file is self-contained: copy it and the CRDT state comes along. A flusher thread writes what changed since the last flush
in ordinary logged transactions together with `meta_epoch`: the batch and the epoch it covers are atomic. Reads that miss the cache go through a small pool of read-only connections. With
`MW_META_FLUSH_PAR=2` a large flush is cut in key ranges written by two connections at once (+10 to +17% on narrow tables; more connections do not help: conflicts on the pages above the leaves).

### Durability and recovery (the rule that makes it ACID)
- A commit is durable when its record is in the log; its metadata is in the record. A crash can therefore never leave pages without their metadata or metadata without
  pages: they are one record, valid or not as a whole (checksummed).
- Recovery replays the valid prefix of the log: pages as always, and the extensions of the records (memory table in one process, bucket index in several). The flush point of
  the file is of an older incarnation of the epochs, so everything the log holds is applied again (idempotent).
- Compaction (which moves the log into the database file and lets the log forget) is held back to the flushed point as long as there is metadata above it: the metadata of
  a commit exists in the log record or in the file tables, never nowhere. A compaction that is limited asks the flusher to run.
- The last connection to close flushes before the log is dropped.

## Capture and DDL

Tables are tracked when they have an explicit primary key and are not internal (`mw_*`, `sqlite_*`). A table without a primary key has no stable identity and is not synchronised.
- A table created and filled in one transaction: its rows are captured (the catalog of the new schema is built from the transaction's own pages).
- ADD / DROP COLUMN, and a table recreated with another layout: rows are matched by content and cells by column name (a column that is new to the record of a rewritten
  row gets a cell, the default of the column is not known to the capture).
- DROP TABLE removes the cells of the table (it is not a delete of its rows: sqlite-sync's cleanup does the same); dead cells are filtered by db_version until a flush deletes them.
- VACUUM (recognised as a statement) changes no row logically and produces no changes. RENAME TABLE starts the new name clean (the history of the old name goes).
- Savepoints and rollbacks leave no trace (nothing is captured before the commit); triggers and foreign-key actions are rows like any other.

## Sync

`mw_sync_export(db, since, &payload, &len, &upto)`: every change with db_version in `(since, upto]`, as the container of sqlite-sync (header `CLSY`, LZ4, tuples
`(tbl, pk, col_name, col_value, col_version, db_version, site_id, cl, seq)`), so peers of either implementation understand each other; `mw_sync_apply(db, payload, len, &stats)`:
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
(their shadow tables are tables; the virtual table itself is not tracked), tables without a primary key (kept locally, not exported), the multi-process mode with private
stores (`mw_mp=3`: the metadata design needs the shared mode; opening it with `mw_cdc=1` is refused). Platforms: macOS; Linux, iOS, Windows, Android later.

## Tests

`make test` (the engine and the metadata: capture against a model, the store across restarts and SIGKILL, DDL, sync convergence and atomicity, multi-process with a live reader
and SIGKILL rounds), `make oracle-test` (differential tests against sqlite-sync: primary-key encoding, local generation, merge of 37 thousand changes, 4000 transactions of the
live store, features, 300 rounds of two peers exchanging payloads through the real encoder and decoder), `make test-mp` (the metadata tests again with the processes mode).
