package sqlite

import (
	"bytes"
	"capcompute/dispatcher"
	"capcompute/dispatcher/replay/tape/journaled"
	"context"
	"crypto/hmac"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"aurora-capcompute/aurora"
	_ "github.com/mattn/go-sqlite3"
)

var _ aurora.Store = (*Store)(nil)

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

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) migrate(ctx context.Context) error {
	const schema = `
CREATE TABLE IF NOT EXISTS threads (
	tenant_id TEXT NOT NULL,
	id TEXT NOT NULL,
	title TEXT NOT NULL,
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	manifest BLOB NOT NULL,
	active_run_id TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (tenant_id, id)
);
CREATE TABLE IF NOT EXISTS runs (
	tenant_id TEXT NOT NULL,
	id TEXT NOT NULL,
	thread_id TEXT NOT NULL,
	revision INTEGER NOT NULL,
	message TEXT NOT NULL,
	status TEXT NOT NULL,
	attempt INTEGER NOT NULL,
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	started_at TEXT,
	completed_at TEXT,
	answer TEXT NOT NULL DEFAULT '',
	error_text TEXT NOT NULL DEFAULT '',
	effective_manifest BLOB NOT NULL,
	brain_digest TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (tenant_id, id),
	FOREIGN KEY (tenant_id, thread_id) REFERENCES threads(tenant_id, id)
);
CREATE INDEX IF NOT EXISTS runs_thread_idx ON runs(tenant_id, thread_id, created_at);
CREATE TABLE IF NOT EXISTS messages (
	tenant_id TEXT NOT NULL,
	thread_id TEXT NOT NULL,
	position INTEGER NOT NULL,
	role TEXT NOT NULL,
	content TEXT NOT NULL,
	PRIMARY KEY (tenant_id, thread_id, position),
	FOREIGN KEY (tenant_id, thread_id) REFERENCES threads(tenant_id, id)
);
CREATE TABLE IF NOT EXISTS journal_records (
	tenant_id TEXT NOT NULL,
	run_id TEXT NOT NULL,
	revision INTEGER NOT NULL,
	position INTEGER NOT NULL,
	call_name TEXT NOT NULL,
	call_args BLOB,
	outcome_kind TEXT NOT NULL,
	outcome_result BLOB,
	outcome_message TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (tenant_id, run_id, revision, position)
);
CREATE TABLE IF NOT EXISTS tasks (
	tenant_id TEXT NOT NULL,
	id TEXT NOT NULL,
	thread_id TEXT NOT NULL,
	run_id TEXT NOT NULL,
	revision INTEGER NOT NULL,
	journal_position INTEGER NOT NULL,
	call_hash TEXT NOT NULL,
	state TEXT NOT NULL,
	token_hash BLOB NOT NULL,
	call_name TEXT NOT NULL,
	call_args BLOB,
	summary TEXT NOT NULL DEFAULT '',
	decision TEXT NOT NULL DEFAULT '',
	resolution_data BLOB,
	actor TEXT NOT NULL DEFAULT '',
	reason TEXT NOT NULL DEFAULT '',
	expires_at TEXT,
	created_at TEXT NOT NULL,
	resolved_at TEXT,
	PRIMARY KEY (tenant_id, id),
	UNIQUE (tenant_id, run_id, revision, journal_position, call_hash)
);
CREATE TABLE IF NOT EXISTS leases (
	tenant_id TEXT NOT NULL,
	resource_kind TEXT NOT NULL,
	resource_id TEXT NOT NULL,
	holder TEXT NOT NULL,
	expires_at TEXT NOT NULL,
	PRIMARY KEY (tenant_id, resource_kind, resource_id)
);`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return err
	}
	migrations := []string{
		`ALTER TABLE runs ADD COLUMN brain_digest TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE runs ADD COLUMN depth INTEGER NOT NULL DEFAULT 0`,
	}
	for _, ddl := range migrations {
		_, err := s.db.ExecContext(ctx, ddl)
		if err != nil && !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
			return err
		}
	}
	return nil
}

func (s *Store) Load(ctx context.Context, tenantID string) (aurora.StoredState, error) {
	var state aurora.StoredState
	threads, err := s.loadThreads(ctx, tenantID)
	if err != nil {
		return state, err
	}
	runs, err := s.loadRuns(ctx, tenantID)
	if err != nil {
		return state, err
	}
	messages, err := s.loadMessages(ctx, tenantID)
	if err != nil {
		return state, err
	}
	state.Threads = threads
	state.Runs = runs
	state.Messages = messages
	return state, nil
}

func (s *Store) SaveThread(ctx context.Context, thread aurora.StoredThread) error {
	manifest, err := json.Marshal(thread.Manifest)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO threads (tenant_id,id,title,created_at,updated_at,manifest,active_run_id)
VALUES (?,?,?,?,?,?,?)
ON CONFLICT(tenant_id,id) DO UPDATE SET
	title=excluded.title,
	updated_at=excluded.updated_at,
	manifest=excluded.manifest,
	active_run_id=excluded.active_run_id`,
		thread.TenantID, thread.ID, thread.Title, formatTime(thread.CreatedAt),
		formatTime(thread.UpdatedAt), manifest, thread.ActiveRunID)
	return err
}

func (s *Store) SaveRun(ctx context.Context, run aurora.StoredRun) error {
	manifest, err := json.Marshal(run.EffectiveManifest)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO runs (
	tenant_id,id,thread_id,revision,message,status,attempt,created_at,updated_at,
	started_at,completed_at,answer,error_text,effective_manifest,brain_digest
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(tenant_id,id) DO UPDATE SET
	revision=excluded.revision,
	status=excluded.status,
	attempt=excluded.attempt,
	updated_at=excluded.updated_at,
	started_at=excluded.started_at,
	completed_at=excluded.completed_at,
	answer=excluded.answer,
	error_text=excluded.error_text,
	effective_manifest=excluded.effective_manifest,
	brain_digest=excluded.brain_digest`,
		run.TenantID, run.ID, run.ThreadID, run.Revision, run.Message, run.Status,
		run.Attempt, formatTime(run.CreatedAt), formatTime(run.UpdatedAt),
		nullableTime(run.StartedAt), nullableTime(run.CompletedAt), run.Answer,
		run.Error, manifest, run.BrainDigest)
	return err
}

func (s *Store) AppendMessages(ctx context.Context, tenantID string, threadID string, messages []aurora.HistoryMessage) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var position int
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(position)+1,0) FROM messages WHERE tenant_id=? AND thread_id=?`,
		tenantID, threadID).Scan(&position); err != nil {
		return err
	}
	for _, message := range messages {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO messages (tenant_id,thread_id,position,role,content) VALUES (?,?,?,?,?)`,
			tenantID, threadID, position, message.Role, message.Content); err != nil {
			return err
		}
		position++
	}
	return tx.Commit()
}

func (s *Store) OpenJournal(_ context.Context, scope aurora.RunContext) (journaled.Journal, error) {
	return &journal{db: s.db, scope: scope}, nil
}

func (s *Store) ResetJournal(ctx context.Context, scope aurora.RunContext) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM journal_records WHERE tenant_id=? AND run_id=? AND revision=?`,
		scope.TenantID, scope.RunID, scope.Revision)
	return err
}

func (s *Store) AcquireLease(
	ctx context.Context,
	tenantID, kind, resourceID, holder string,
	now time.Time,
	ttl time.Duration,
) (bool, error) {
	result, err := s.db.ExecContext(ctx, `
INSERT INTO leases (tenant_id,resource_kind,resource_id,holder,expires_at)
VALUES (?,?,?,?,?)
ON CONFLICT(tenant_id,resource_kind,resource_id) DO UPDATE SET
	holder=excluded.holder,
	expires_at=excluded.expires_at
WHERE leases.holder=excluded.holder OR leases.expires_at<=?`,
		tenantID, kind, resourceID, holder, formatTime(now.Add(ttl)), formatTime(now))
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	return affected > 0, err
}

func (s *Store) ReleaseLease(ctx context.Context, tenantID, kind, resourceID, holder string) error {
	_, err := s.db.ExecContext(ctx, `
DELETE FROM leases WHERE tenant_id=? AND resource_kind=? AND resource_id=? AND holder=?`,
		tenantID, kind, resourceID, holder)
	return err
}

func (s *Store) Find(ctx context.Context, scope aurora.TaskScope, position int, callHash string) (aurora.TaskRecord, bool, error) {
	record, err := scanTaskRow(s.db.QueryRowContext(ctx, taskSelect+`
WHERE tenant_id=? AND run_id=? AND revision=? AND journal_position=? AND call_hash=?`,
		scope.TenantID, scope.RunID, scope.Revision, position, callHash))
	if errors.Is(err, sql.ErrNoRows) {
		return aurora.TaskRecord{}, false, nil
	}
	return record, err == nil, err
}

func (s *Store) Create(ctx context.Context, record aurora.TaskRecord) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO tasks (
	tenant_id,id,thread_id,run_id,revision,journal_position,call_hash,state,
	token_hash,call_name,call_args,summary,decision,resolution_data,actor,reason,
	expires_at,created_at,resolved_at
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		record.Scope.TenantID, record.ID, record.Scope.ThreadID, record.Scope.RunID,
		record.Scope.Revision, record.JournalPosition, record.CallHash, record.State,
		record.TokenHash, record.Call.Name, []byte(record.Call.Args), record.Summary,
		record.Resolution.Decision, []byte(record.Resolution.Data), record.Resolution.Actor,
		record.Resolution.Reason, nullableTime(record.ExpiresAt), formatTime(record.CreatedAt),
		nullableTime(record.ResolvedAt))
	return err
}

func (s *Store) Get(ctx context.Context, tenantID, taskID string) (aurora.TaskRecord, error) {
	record, err := scanTaskRow(s.db.QueryRowContext(ctx, taskSelect+`WHERE tenant_id=? AND id=?`, tenantID, taskID))
	if errors.Is(err, sql.ErrNoRows) {
		return aurora.TaskRecord{}, aurora.ErrTaskNotFound
	}
	return record, err
}

func (s *Store) List(ctx context.Context, tenantID, runID string) ([]aurora.TaskRecord, error) {
	query := taskSelect + `WHERE tenant_id=?`
	args := []any{tenantID}
	if runID != "" {
		query += ` AND run_id=?`
		args = append(args, runID)
	}
	query += ` ORDER BY created_at`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []aurora.TaskRecord
	for rows.Next() {
		record, err := scanTaskFromRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	return out, rows.Err()
}

func (s *Store) Resolve(ctx context.Context, tenantID, taskID string, tokenHash []byte, resolution aurora.Resolution, now time.Time) (aurora.TaskRecord, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return aurora.TaskRecord{}, err
	}
	defer tx.Rollback()
	record, err := scanTaskRow(tx.QueryRowContext(ctx, taskSelect+`WHERE tenant_id=? AND id=?`, tenantID, taskID))
	if errors.Is(err, sql.ErrNoRows) {
		return aurora.TaskRecord{}, aurora.ErrTaskNotFound
	}
	if err != nil {
		return aurora.TaskRecord{}, err
	}
	if !hmac.Equal(record.TokenHash, tokenHash) {
		return aurora.TaskRecord{}, aurora.ErrTaskUnauthorized
	}
	if record.State != aurora.TaskStatePending {
		if record.Resolution.Decision == resolution.Decision &&
			bytes.Equal(record.Resolution.Data, resolution.Data) &&
			record.Resolution.Reason == resolution.Reason {
			return record, nil
		}
		return aurora.TaskRecord{}, aurora.ErrTaskConflict
	}
	if record.ExpiresAt != nil && !now.Before(*record.ExpiresAt) {
		return aurora.TaskRecord{}, aurora.ErrTaskGone
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE tasks SET state=?,decision=?,resolution_data=?,actor=?,reason=?,resolved_at=?
WHERE tenant_id=? AND id=?`,
		resolution.Decision, resolution.Decision, []byte(resolution.Data),
		resolution.Actor, resolution.Reason, formatTime(now), tenantID, taskID); err != nil {
		return aurora.TaskRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return aurora.TaskRecord{}, err
	}
	record.State = resolution.Decision
	record.Resolution = resolution
	record.ResolvedAt = &now
	return record, nil
}

func (s *Store) MarkExecuted(ctx context.Context, tenantID, taskID string, _ time.Time) error {
	result, err := s.db.ExecContext(ctx,
		`UPDATE tasks SET state=? WHERE tenant_id=? AND id=?`,
		aurora.TaskStateExecuted, tenantID, taskID)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return aurora.ErrTaskNotFound
	}
	return nil
}

func (s *Store) loadThreads(ctx context.Context, tenantID string) ([]aurora.StoredThread, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id,title,created_at,updated_at,manifest,active_run_id
FROM threads WHERE tenant_id=? ORDER BY created_at`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []aurora.StoredThread
	for rows.Next() {
		var thread aurora.StoredThread
		var created, updated string
		var manifest []byte
		thread.TenantID = tenantID
		if err := rows.Scan(&thread.ID, &thread.Title, &created, &updated, &manifest, &thread.ActiveRunID); err != nil {
			return nil, err
		}
		if thread.CreatedAt, err = parseTime(created); err != nil {
			return nil, err
		}
		if thread.UpdatedAt, err = parseTime(updated); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(manifest, &thread.Manifest); err != nil {
			return nil, err
		}
		out = append(out, thread)
	}
	return out, rows.Err()
}

func (s *Store) loadRuns(ctx context.Context, tenantID string) ([]aurora.StoredRun, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id,thread_id,revision,message,status,attempt,created_at,updated_at,
	started_at,completed_at,answer,error_text,effective_manifest,brain_digest
FROM runs WHERE tenant_id=? ORDER BY created_at`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []aurora.StoredRun
	for rows.Next() {
		var run aurora.StoredRun
		var created, updated string
		var started, completed sql.NullString
		var manifest []byte
		run.TenantID = tenantID
		if err := rows.Scan(
			&run.ID, &run.ThreadID, &run.Revision, &run.Message, &run.Status,
			&run.Attempt, &created, &updated, &started, &completed, &run.Answer,
			&run.Error, &manifest, &run.BrainDigest,
		); err != nil {
			return nil, err
		}
		if run.CreatedAt, err = parseTime(created); err != nil {
			return nil, err
		}
		if run.UpdatedAt, err = parseTime(updated); err != nil {
			return nil, err
		}
		if run.StartedAt, err = parseNullableTime(started); err != nil {
			return nil, err
		}
		if run.CompletedAt, err = parseNullableTime(completed); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(manifest, &run.EffectiveManifest); err != nil {
			return nil, err
		}
		out = append(out, run)
	}
	return out, rows.Err()
}

func (s *Store) loadMessages(ctx context.Context, tenantID string) ([]aurora.StoredMessage, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT thread_id,position,role,content
FROM messages WHERE tenant_id=? ORDER BY thread_id,position`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []aurora.StoredMessage
	for rows.Next() {
		message := aurora.StoredMessage{TenantID: tenantID}
		if err := rows.Scan(&message.ThreadID, &message.Position, &message.Role, &message.Content); err != nil {
			return nil, err
		}
		out = append(out, message)
	}
	return out, rows.Err()
}

type journal struct {
	db    *sql.DB
	scope aurora.RunContext
}

func (j *journal) Load(position int) (journaled.Record, error) {
	var call dispatcher.Call
	var kind dispatcher.OutcomeKind
	var result []byte
	var message string
	err := j.db.QueryRow(`
SELECT call_name,call_args,outcome_kind,outcome_result,outcome_message
FROM journal_records
WHERE tenant_id=? AND run_id=? AND revision=? AND position=?`,
		j.scope.TenantID, j.scope.RunID, j.scope.Revision, position).
		Scan(&call.Name, &call.Args, &kind, &result, &message)
	if errors.Is(err, sql.ErrNoRows) {
		return journaled.Record{}, errors.New("journal record not found")
	}
	if err != nil {
		return journaled.Record{}, err
	}
	var outcome dispatcher.Outcome
	switch kind {
	case dispatcher.OutcomeResult:
		outcome = dispatcher.Result(result)
	case dispatcher.OutcomeFailed:
		outcome = dispatcher.Failed(message)
	default:
		return journaled.Record{}, fmt.Errorf("invalid persisted outcome %q", kind)
	}
	return journaled.Record{Call: call, Outcome: outcome}, nil
}

func (j *journal) Store(position int, call dispatcher.Call, outcome dispatcher.Outcome) error {
	_, err := j.db.Exec(`
INSERT INTO journal_records (
	tenant_id,run_id,revision,position,call_name,call_args,
	outcome_kind,outcome_result,outcome_message
) VALUES (?,?,?,?,?,?,?,?,?)`,
		j.scope.TenantID, j.scope.RunID, j.scope.Revision, position,
		call.Name, []byte(call.Args), outcome.Kind(), []byte(outcome.Result()), outcome.Message())
	return err
}

func (j *journal) Length() int {
	var length int
	if err := j.db.QueryRow(`
SELECT COUNT(*) FROM journal_records WHERE tenant_id=? AND run_id=? AND revision=?`,
		j.scope.TenantID, j.scope.RunID, j.scope.Revision).Scan(&length); err != nil {
		return 0
	}
	return length
}

const taskSelect = `
SELECT tenant_id,id,thread_id,run_id,revision,journal_position,call_hash,state,
	token_hash,call_name,call_args,summary,decision,resolution_data,actor,reason,
	expires_at,created_at,resolved_at
FROM tasks `

type rowScanner interface {
	Scan(...any) error
}

func scanTaskRow(row rowScanner) (aurora.TaskRecord, error) {
	var record aurora.TaskRecord
	var callArgs, resolutionData []byte
	var decision aurora.TaskState
	var expires, resolved sql.NullString
	var created string
	if err := row.Scan(
		&record.Scope.TenantID, &record.ID, &record.Scope.ThreadID, &record.Scope.RunID,
		&record.Scope.Revision, &record.JournalPosition, &record.CallHash, &record.State,
		&record.TokenHash, &record.Call.Name, &callArgs, &record.Summary, &decision,
		&resolutionData, &record.Resolution.Actor, &record.Resolution.Reason,
		&expires, &created, &resolved,
	); err != nil {
		return aurora.TaskRecord{}, err
	}
	record.Call.Args = append(json.RawMessage(nil), callArgs...)
	record.Resolution.Decision = decision
	record.Resolution.Data = append(json.RawMessage(nil), resolutionData...)
	createdAt, err := parseTime(created)
	if err != nil {
		return aurora.TaskRecord{}, err
	}
	record.CreatedAt = createdAt
	record.ExpiresAt, err = parseNullableTime(expires)
	if err != nil {
		return aurora.TaskRecord{}, err
	}
	record.ResolvedAt, err = parseNullableTime(resolved)
	if err != nil {
		return aurora.TaskRecord{}, err
	}
	return record, nil
}

func scanTaskFromRows(row rowScanner) (aurora.TaskRecord, error) {
	return scanTaskRow(row)
}

func formatTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func nullableTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return formatTime(*value)
}

func parseTime(value string) (time.Time, error) {
	return time.Parse(time.RFC3339Nano, value)
}

func parseNullableTime(value sql.NullString) (*time.Time, error) {
	if !value.Valid {
		return nil, nil
	}
	parsed, err := parseTime(value.String)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}
