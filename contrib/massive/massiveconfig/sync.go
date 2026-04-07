package massiveconfig

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

// PGDB is the subset of pgx query methods used by sync operations.
// Both *pgx.Conn and *pgxpool.Pool satisfy this interface, allowing
// callers to use either a single connection or a concurrency-safe pool.
type PGDB interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// SyncWindow represents the confirmed sync coverage for a symbol and data type.
// If both Oldest and Newest are non-nil, the window [Oldest, Newest] is the
// time range for which we have confirmed that all available API data has been
// fetched and written to the local database.
//
// Nil values indicate no sync record exists (e.g., first run).
type SyncWindow struct {
	Oldest *time.Time
	Newest *time.Time
}

// ReadSyncWindow executes the read query and returns the sync window for a
// symbol. The query must accept $1 (asset_id) and return two nullable
// TIMESTAMPTZ columns (oldest, newest).
//
// If the query returns no rows, an empty SyncWindow (both nil) is returned.
func ReadSyncWindow(ctx context.Context, db PGDB, query string, assetID int64) SyncWindow {
	var oldest, newest *time.Time
	err := db.QueryRow(ctx, query, assetID).Scan(&oldest, &newest)
	if err != nil {
		// pgx.ErrNoRows is expected when no sync record exists yet.
		// Other errors are logged but not fatal — we fall back to full backfill.
		if err != pgx.ErrNoRows {
			log.Warn("[massive] failed to read sync window for asset %d: %v", assetID, err)
		}
		return SyncWindow{}
	}
	return SyncWindow{Oldest: oldest, Newest: newest}
}

// WriteSyncTimestamp executes a write query (either write_oldest or write_newest)
// to update a sync boundary. The query must accept $1 (asset_id) and $2 (timestamp).
func WriteSyncTimestamp(ctx context.Context, db PGDB, query string, assetID int64, ts time.Time) error {
	_, err := db.Exec(ctx, query, assetID, ts)
	return err
}
