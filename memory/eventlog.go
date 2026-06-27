// Package memory provides in-memory implementations of the Aurora runtime's
// contracts: an append-only event log, a lease table, and the session store.
// They are useful for tests and single-process runs; durable persistence lives
// in the sqlite package.
package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"aurora-capcompute/aurora"
)

var (
	_ aurora.EventLog = (*EventLog)(nil)
	_ aurora.Leases   = (*Leases)(nil)
)

// EventLog is an in-memory append-only event log: one ordered stream per thread.
type EventLog struct {
	mu      sync.RWMutex
	streams map[aurora.LogScope][]aurora.LogEvent
}

func NewEventLog() *EventLog {
	return &EventLog{streams: make(map[aurora.LogScope][]aurora.LogEvent)}
}

func (m *EventLog) Append(_ context.Context, scope aurora.LogScope, events ...aurora.LogEvent) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	existing := m.streams[scope]
	head := uint64(len(existing))
	if len(events) == 0 {
		return head, nil
	}
	appended := make([]aurora.LogEvent, len(events))
	for i, ev := range events {
		head++
		ev.Seq = head
		ev.Data = append([]byte(nil), ev.Data...)
		appended[i] = ev
	}
	m.streams[scope] = append(existing, appended...)
	return head, nil
}

func (m *EventLog) Read(_ context.Context, scope aurora.LogScope, after uint64) ([]aurora.LogEvent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	stream := m.streams[scope]
	if after >= uint64(len(stream)) {
		return nil, nil
	}
	out := make([]aurora.LogEvent, 0, uint64(len(stream))-after)
	for _, ev := range stream[after:] {
		ev.Data = append([]byte(nil), ev.Data...)
		out = append(out, ev)
	}
	return out, nil
}

func (m *EventLog) Streams(_ context.Context, tenantID string) ([]aurora.LogScope, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []aurora.LogScope
	for scope, stream := range m.streams {
		if scope.TenantID == tenantID && len(stream) > 0 {
			out = append(out, scope)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ThreadID < out[j].ThreadID })
	return out, nil
}

// Leases is an in-memory lease table for cross-goroutine coordination in tests
// and single-process runs.
type Leases struct {
	mu     sync.Mutex
	leases map[string]leaseEntry
}

type leaseEntry struct {
	holder    string
	expiresAt time.Time
}

func NewLeases() *Leases { return &Leases{leases: make(map[string]leaseEntry)} }

func (l *Leases) Acquire(_ context.Context, tenantID, kind, resourceID, holder string, now time.Time, ttl time.Duration) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	key := tenantID + "/" + kind + "/" + resourceID
	if e, ok := l.leases[key]; ok && e.holder != holder && now.Before(e.expiresAt) {
		return false, nil
	}
	l.leases[key] = leaseEntry{holder: holder, expiresAt: now.Add(ttl)}
	return true, nil
}

func (l *Leases) Release(_ context.Context, tenantID, kind, resourceID, holder string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	key := tenantID + "/" + kind + "/" + resourceID
	if e, ok := l.leases[key]; ok && e.holder == holder {
		delete(l.leases, key)
	}
	return nil
}
