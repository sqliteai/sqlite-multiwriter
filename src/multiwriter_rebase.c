//
//  multiwriter_rebase.c
//
//  Conflict resolution by logical replay. In sqlite-multiwriter the logical changes come from the VFS itself (the row-level capture, docs/design.md); until that
//  replay exists, a transaction whose pages conflict is refused with SQLITE_BUSY_SNAPSHOT and the application retries it.
//
#include "multiwriter_internal.h"

int mw_lane_rebase (mw_lane *lane, mw_overlay *ov_in) { (void)lane; (void)ov_in; return MW_CONFLICT; }
void mw_lane_rebase_free (mw_lane *lane) { (void)lane; }
