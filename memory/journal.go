package memory

import (
	"capcompute/dispatcher"
	"capcompute/dispatcher/replay/tape/journaled"
	"errors"
	"sync"
)

var (
	ErrInvalidIndex   = errors.New("invalid journal index")
	ErrRecordNotFound = errors.New("journal record not found")
)

var _ journaled.Journal = (*Journal)(nil)

type Journal struct {
	mu      sync.Mutex
	records []journaled.Record
}

func NewJournal(records ...journaled.Record) *Journal {
	journal := &Journal{}
	for _, record := range records {
		journal.records = append(journal.records, copyRecord(record))
	}
	return journal
}

func (j *Journal) Load(idx int) (journaled.Record, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if idx < 0 || idx >= len(j.records) {
		return journaled.Record{}, ErrRecordNotFound
	}
	return copyRecord(j.records[idx]), nil
}

func (j *Journal) Store(idx int, call dispatcher.Call, outcome dispatcher.Outcome) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if idx != len(j.records) {
		return ErrInvalidIndex
	}
	j.records = append(j.records, copyRecord(journaled.Record{Call: call, Outcome: outcome}))
	return nil
}

func (j *Journal) Length() int {
	j.mu.Lock()
	defer j.mu.Unlock()

	return len(j.records)
}

func copyRecord(record journaled.Record) journaled.Record {
	return journaled.Record{
		Call:    record.Call.Copy(),
		Outcome: record.Outcome.Copy(),
	}
}
