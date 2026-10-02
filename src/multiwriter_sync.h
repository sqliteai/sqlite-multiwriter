//
//  multiwriter_sync.h
//
//  Synchronisation with peers (docs/design.md): the changes of this database since a db_version as a payload, and the atomic merge of a payload received from a peer. The payload is
//  the container of sqlite-sync (header 'CLSY', LZ4, the tuples (tbl, pk, col_name, col_value, col_version, db_version, site_id, cl, seq) in the primary-key byte format), so peers
//  of either implementation understand each other. All functions take an ordinary connection to a database opened with the multi-writer VFS and mw_cdc=1.
//
#ifndef MW_SYNC_H
#define MW_SYNC_H

#include <stdint.h>
#include <stddef.h>
#include "sqlite3.h"

typedef struct { int64_t rows, applied, ignored, retries; } mw_sync_stats;     // rows in the payload, rows that changed something, rows for tables / columns this database does not have

int     mw_sync_site_id (sqlite3 *db, uint8_t out[16]);                       // the id of this database
int64_t mw_sync_db_version (sqlite3 *db);                                     // the last db_version (every change up to it can be exported); -1 on error
// Every change with db_version in (since, *upto]. *payload is NULL (and *len 0) when there is none; otherwise free it with mw_sync_free. The next export starts at *upto.
int     mw_sync_export (sqlite3 *db, int64_t since, uint8_t **payload, size_t *len, int64_t *upto);
// Merges a payload with the rules of the CRDT, in one transaction: either all of it is applied or none (the connection must not be inside a transaction). Safe to call
// concurrently with writers and with other applies; the transaction is retried on a conflict.
int     mw_sync_apply (sqlite3 *db, const uint8_t *payload, size_t len, mw_sync_stats *stats);
// The metadata of the rows that exist without any (a database that had data before the capture was on). Idempotent.
int     mw_sync_backfill (sqlite3 *db);
void    mw_sync_free (void *p);

#endif
