package memory

import (
	"bytes"
	"capcompute/dispatcher"
	"capcompute/dispatcher/replay/tape/journaled"
	"context"
	"crypto/hmac"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"aurora-capcompute/aurora"
)

var _ aurora.Store = (*Store)(nil)

type Store struct {
	mu       sync.Mutex
	threads  map[string]aurora.StoredThread
	runs     map[string]aurora.StoredRun
	messages map[string][]aurora.StoredMessage
	journals map[string]*storeJournal
	tasks    map[string]aurora.TaskRecord
	leases   map[string]storeLease
}

func NewStore() *Store {
	return &Store{
		threads:  make(map[string]aurora.StoredThread),
		runs:     make(map[string]aurora.StoredRun),
		messages: make(map[string][]aurora.StoredMessage),
		journals: make(map[string]*storeJournal),
		tasks:    make(map[string]aurora.TaskRecord),
		leases:   make(map[string]storeLease),
	}
}

func (s *Store) Load(_ context.Context, tenantID string) (aurora.StoredState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var state aurora.StoredState
	for _, thread := range s.threads {
		if thread.TenantID == tenantID {
			state.Threads = append(state.Threads, cloneStoredThread(thread))
		}
	}
	for _, run := range s.runs {
		if run.TenantID == tenantID {
			state.Runs = append(state.Runs, cloneStoredRun(run))
		}
	}
	for _, messages := range s.messages {
		for _, message := range messages {
			if message.TenantID == tenantID {
				state.Messages = append(state.Messages, message)
			}
		}
	}
	return state, nil
}

func (s *Store) SaveThread(_ context.Context, thread aurora.StoredThread) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.threads[storeKey(thread.TenantID, thread.ID)] = cloneStoredThread(thread)
	return nil
}

func (s *Store) SaveRun(_ context.Context, run aurora.StoredRun) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runs[storeKey(run.TenantID, run.ID)] = cloneStoredRun(run)
	return nil
}

func (s *Store) AppendMessages(_ context.Context, tenantID string, threadID string, messages []aurora.HistoryMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := storeKey(tenantID, threadID)
	position := len(s.messages[key])
	for _, message := range messages {
		s.messages[key] = append(s.messages[key], aurora.StoredMessage{
			TenantID: tenantID,
			ThreadID: threadID,
			Position: position,
			Role:     message.Role,
			Content:  message.Content,
		})
		position++
	}
	return nil
}

func (s *Store) OpenJournal(_ context.Context, scope aurora.RunContext) (journaled.Journal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scope.SessionKey()
	journal := s.journals[key]
	if journal == nil {
		journal = &storeJournal{}
		s.journals[key] = journal
	}
	return journal, nil
}

// ForkJournal mints a new revision (child) that shares the parent revision's
// recorded prefix [0, offset) copy-on-write: reads below the offset fall through
// to the parent journal, while new records are appended to the child's own tail.
// The parent revision is left untouched and remains addressable.
func (s *Store) ForkJournal(_ context.Context, parent, child aurora.RunContext, offset int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if parent.TenantID != child.TenantID || parent.RunID != child.RunID {
		return errors.New("fork journal: parent and child must share tenant and run")
	}
	if offset < 0 {
		return errors.New("fork journal: negative offset")
	}
	parentJournal := s.journals[parent.SessionKey()]
	if offset > 0 && parentJournal == nil {
		return errors.New("fork journal: parent revision has no journal")
	}
	s.journals[child.SessionKey()] = &storeJournal{parent: parentJournal, offset: offset}
	return nil
}

func (s *Store) Close() error {
	return nil
}

type storeLease struct {
	holder    string
	expiresAt time.Time
}

func (s *Store) AcquireLease(_ context.Context, tenantID, kind, resourceID, holder string, now time.Time, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := tenantID + "/" + kind + "/" + resourceID
	lease, exists := s.leases[key]
	if exists && lease.holder != holder && now.Before(lease.expiresAt) {
		return false, nil
	}
	s.leases[key] = storeLease{holder: holder, expiresAt: now.Add(ttl)}
	return true, nil
}

func (s *Store) ReleaseLease(_ context.Context, tenantID, kind, resourceID, holder string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := tenantID + "/" + kind + "/" + resourceID
	if lease, ok := s.leases[key]; ok && lease.holder == holder {
		delete(s.leases, key)
	}
	return nil
}

func (s *Store) Find(_ context.Context, scope aurora.TaskScope, position int, callHash string) (aurora.TaskRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, record := range s.tasks {
		if record.Scope == scope && record.JournalPosition == position && record.CallHash == callHash {
			return cloneTask(record), true, nil
		}
	}
	return aurora.TaskRecord{}, false, nil
}

func (s *Store) Create(_ context.Context, record aurora.TaskRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := storeKey(record.Scope.TenantID, record.ID)
	if _, exists := s.tasks[key]; exists {
		return aurora.ErrTaskConflict
	}
	s.tasks[key] = cloneTask(record)
	return nil
}

func (s *Store) Get(_ context.Context, tenantID, taskID string) (aurora.TaskRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.tasks[storeKey(tenantID, taskID)]
	if !ok {
		return aurora.TaskRecord{}, aurora.ErrTaskNotFound
	}
	return cloneTask(record), nil
}

func (s *Store) List(_ context.Context, tenantID, runID string) ([]aurora.TaskRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []aurora.TaskRecord
	for _, record := range s.tasks {
		if record.Scope.TenantID == tenantID && (runID == "" || record.Scope.RunID == runID) {
			out = append(out, cloneTask(record))
		}
	}
	return out, nil
}

func (s *Store) Resolve(_ context.Context, tenantID, taskID string, tokenHash []byte, resolution aurora.Resolution, now time.Time) (aurora.TaskRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := storeKey(tenantID, taskID)
	record, ok := s.tasks[key]
	if !ok {
		return aurora.TaskRecord{}, aurora.ErrTaskNotFound
	}
	if !hmac.Equal(record.TokenHash, tokenHash) {
		return aurora.TaskRecord{}, aurora.ErrTaskUnauthorized
	}
	if record.State != aurora.TaskStatePending {
		if record.Resolution.Decision == resolution.Decision &&
			bytes.Equal(record.Resolution.Data, resolution.Data) &&
			record.Resolution.Reason == resolution.Reason {
			return cloneTask(record), nil
		}
		return aurora.TaskRecord{}, aurora.ErrTaskConflict
	}
	if record.ExpiresAt != nil && !now.Before(*record.ExpiresAt) {
		return aurora.TaskRecord{}, aurora.ErrTaskGone
	}
	record.State = resolution.Decision
	record.Resolution = resolution
	record.ResolvedAt = &now
	s.tasks[key] = cloneTask(record)
	return cloneTask(record), nil
}

func (s *Store) MarkExecuted(_ context.Context, tenantID, taskID string, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := storeKey(tenantID, taskID)
	record, ok := s.tasks[key]
	if !ok {
		return aurora.ErrTaskNotFound
	}
	record.State = aurora.TaskStateExecuted
	s.tasks[key] = cloneTask(record)
	return nil
}

type storeJournal struct {
	mu      sync.Mutex
	records []journaled.Record
	// parent and offset implement copy-on-write revisions: positions below
	// offset are read from the parent journal, while this journal owns the tail
	// at positions [offset, offset+len(records)).
	parent *storeJournal
	offset int
}

func (j *storeJournal) Load(index int) (journaled.Record, error) {
	j.mu.Lock()
	parent := j.parent
	offset := j.offset
	if parent != nil && index < offset {
		j.mu.Unlock()
		return parent.Load(index)
	}
	defer j.mu.Unlock()
	local := index - offset
	if local < 0 || local >= len(j.records) {
		return journaled.Record{}, errors.New("journal record not found")
	}
	record := j.records[local]
	return journaled.Record{Call: record.Call.Copy(), Outcome: record.Outcome.Copy()}, nil
}

func (j *storeJournal) Store(index int, call dispatcher.Call, outcome dispatcher.Outcome) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if index != j.offset+len(j.records) {
		return errors.New("invalid journal index")
	}
	j.records = append(j.records, journaled.Record{Call: call.Copy(), Outcome: outcome.Copy()})
	return nil
}

func (j *storeJournal) Length() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.offset + len(j.records)
}

func storeKey(a, b string) string {
	return a + "/" + b
}

func cloneStoredThread(thread aurora.StoredThread) aurora.StoredThread {
	thread.Manifest = cloneManifest(thread.Manifest)
	return thread
}

func cloneStoredRun(run aurora.StoredRun) aurora.StoredRun {
	run.EffectiveManifest = cloneManifest(run.EffectiveManifest)
	run.StartedAt = copyTime(run.StartedAt)
	run.CompletedAt = copyTime(run.CompletedAt)
	return run
}

func cloneManifest(manifest aurora.Manifest) aurora.Manifest {
	out := manifest
	out.Capabilities = make([]aurora.CapabilityConfig, len(manifest.Capabilities))
	for i, capability := range manifest.Capabilities {
		out.Capabilities[i] = capability
		out.Capabilities[i].Settings = append(json.RawMessage(nil), capability.Settings...)
	}
	out.Children = cloneChildren(manifest.Children)
	return out
}

func cloneChildren(children []aurora.ChildManifest) []aurora.ChildManifest {
	if len(children) == 0 {
		return nil
	}
	out := make([]aurora.ChildManifest, len(children))
	for i, child := range children {
		out[i] = child
		out[i].Capabilities = make([]aurora.CapabilityConfig, len(child.Capabilities))
		for j, cap := range child.Capabilities {
			out[i].Capabilities[j] = cap
			out[i].Capabilities[j].Settings = append(json.RawMessage(nil), cap.Settings...)
		}
		out[i].Children = cloneChildren(child.Children)
	}
	return out
}

func cloneTask(record aurora.TaskRecord) aurora.TaskRecord {
	record.Call = record.Call.Copy()
	record.TokenHash = append([]byte(nil), record.TokenHash...)
	record.Resolution.Data = append(json.RawMessage(nil), record.Resolution.Data...)
	record.ExpiresAt = copyTime(record.ExpiresAt)
	record.ResolvedAt = copyTime(record.ResolvedAt)
	return record
}

func copyTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
