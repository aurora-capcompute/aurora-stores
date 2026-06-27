// Package sqlite is a durable SQLite implementation of the Aurora runtime's
// persistence contracts: an append-only event log (one ordered stream per
// thread) and a lease table for cross-instance coordination. The runtime folds
// the log into thread/run/task projections; there is no per-entity row store.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"aurora-capcompute/aurora"
	_ "github.com/mattn/go-sqlite3"
)

var (
	_ aurora.EventLog = (*Store)(nil)
	_ aurora.Leases   = (*Store)(nil)
)

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite3", path+"?_foreign_keys=on&_busy_timeout=5000&_journal_mode=WAL")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	store := &Store{db: db}
	if err := store.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate(ctx context.Context) error {
	const schema = `
CREATE TABLE IF NOT EXISTS events (
	tenant_id   TEXT    NOT NULL,
	thread_id   TEXT    NOT NULL,
	seq         INTEGER NOT NULL,
	kind        TEXT    NOT NULL,
	occurred_at TEXT    NOT NULL,
	run_id      TEXT    NOT NULL DEFAULT '',
	revision    INTEGER NOT NULL DEFAULT 0,
	data        BLOB,
	PRIMARY KEY (tenant_id, thread_id, seq)
);
CREATE TABLE IF NOT EXISTS leases (
	key        TEXT PRIMARY KEY,
	holder     TEXT NOT NULL,
	expires_at TEXT NOT NULL
);`
	_, err := s.db.ExecContext(ctx, schema)
	return err
}

// Append atomically appends events to a thread's stream, assigning each the next
// contiguous sequence, and returns the new head.
func (s *Store) Append(ctx context.Context, scope aurora.LogScope, events ...aurora.LogEvent) (uint64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	var head uint64
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(seq), 0) FROM events WHERE tenant_id=? AND thread_id=?`,
		scope.TenantID, scope.ThreadID).Scan(&head); err != nil {
		return 0, err
	}
	for _, ev := range events {
		head++
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO events(tenant_id,thread_id,seq,kind,occurred_at,run_id,revision,data)
			 VALUES(?,?,?,?,?,?,?,?)`,
			scope.TenantID, scope.ThreadID, head, ev.Kind,
			ev.Time.UTC().Format(time.RFC3339Nano), ev.Run, ev.Rev, []byte(ev.Data)); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return head, nil
}

// Read returns a stream's events with seq > after, in order.
func (s *Store) Read(ctx context.Context, scope aurora.LogScope, after uint64) ([]aurora.LogEvent, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT seq,kind,occurred_at,run_id,revision,data FROM events
		 WHERE tenant_id=? AND thread_id=? AND seq>? ORDER BY seq`,
		scope.TenantID, scope.ThreadID, after)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []aurora.LogEvent
	for rows.Next() {
		var (
			ev         aurora.LogEvent
			occurredAt string
			data       []byte
		)
		if err := rows.Scan(&ev.Seq, &ev.Kind, &occurredAt, &ev.Run, &ev.Rev, &data); err != nil {
			return nil, err
		}
		if t, perr := time.Parse(time.RFC3339Nano, occurredAt); perr == nil {
			ev.Time = t
		}
		ev.Data = append(json.RawMessage(nil), data...)
		out = append(out, ev)
	}
	return out, rows.Err()
}

// Streams lists the thread scopes that have events for a tenant.
func (s *Store) Streams(ctx context.Context, tenantID string) ([]aurora.LogScope, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT DISTINCT thread_id FROM events WHERE tenant_id=? ORDER BY thread_id`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []aurora.LogScope
	for rows.Next() {
		var threadID string
		if err := rows.Scan(&threadID); err != nil {
			return nil, err
		}
		out = append(out, aurora.LogScope{TenantID: tenantID, ThreadID: threadID})
	}
	return out, rows.Err()
}

// Acquire grants or renews a lease unless a different holder's unexpired lease
// exists.
func (s *Store) Acquire(ctx context.Context, tenantID, kind, resourceID, holder string, now time.Time, ttl time.Duration) (bool, error) {
	key := tenantID + "/" + kind + "/" + resourceID
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	var curHolder, curExpires string
	switch err := tx.QueryRowContext(ctx,
		`SELECT holder,expires_at FROM leases WHERE key=?`, key).Scan(&curHolder, &curExpires); {
	case err == sql.ErrNoRows:
	case err != nil:
		return false, err
	default:
		expires, _ := time.Parse(time.RFC3339Nano, curExpires)
		if curHolder != holder && now.Before(expires) {
			return false, nil
		}
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO leases(key,holder,expires_at) VALUES(?,?,?)
		 ON CONFLICT(key) DO UPDATE SET holder=excluded.holder, expires_at=excluded.expires_at`,
		key, holder, now.Add(ttl).UTC().Format(time.RFC3339Nano)); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// Release drops a lease the holder owns.
func (s *Store) Release(ctx context.Context, tenantID, kind, resourceID, holder string) error {
	key := tenantID + "/" + kind + "/" + resourceID
	_, err := s.db.ExecContext(ctx, `DELETE FROM leases WHERE key=? AND holder=?`, key, holder)
	return err
}
