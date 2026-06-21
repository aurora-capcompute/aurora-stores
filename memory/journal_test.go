package memory

import (
	"capcompute/dispatcher"
	"errors"
	"testing"
)

func TestJournalStoresAndLoadsRecords(t *testing.T) {
	journal := NewJournal()

	call := dispatcher.Call{Name: "step.one", Args: []byte(`{"x":1}`)}
	outcome := dispatcher.Result([]byte(`{"ok":true}`))

	if err := journal.Store(0, call, outcome); err != nil {
		t.Fatalf("store: %v", err)
	}
	if got := journal.Length(); got != 1 {
		t.Fatalf("length = %d, want 1", got)
	}

	record, err := journal.Load(0)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if record.Call.Name != "step.one" {
		t.Fatalf("call name = %q", record.Call.Name)
	}
	if string(record.Call.Args) != `{"x":1}` {
		t.Fatalf("call args = %s", record.Call.Args)
	}
	if string(record.Outcome.Result()) != `{"ok":true}` {
		t.Fatalf("outcome result = %s", record.Outcome.Result())
	}
}

func TestJournalCopiesRecords(t *testing.T) {
	journal := NewJournal()

	call := dispatcher.Call{Name: "step.one", Args: []byte(`{"x":1}`)}
	outcome := dispatcher.Result([]byte(`{"ok":true}`))
	if err := journal.Store(0, call, outcome); err != nil {
		t.Fatalf("store: %v", err)
	}

	call.Args[0] = '!'
	record, err := journal.Load(0)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	record.Call.Args[0] = '!'

	loaded, err := journal.Load(0)
	if err != nil {
		t.Fatalf("load again: %v", err)
	}
	if string(loaded.Call.Args) != `{"x":1}` {
		t.Fatalf("stored call args changed: %s", loaded.Call.Args)
	}
	if string(loaded.Outcome.Result()) != `{"ok":true}` {
		t.Fatalf("stored outcome changed: %s", loaded.Outcome.Result())
	}
}

func TestJournalRejectsNonAppendIndex(t *testing.T) {
	journal := NewJournal()

	err := journal.Store(1, dispatcher.Call{Name: "step.one"}, dispatcher.Result(nil))
	if !errors.Is(err, ErrInvalidIndex) {
		t.Fatalf("error = %v, want ErrInvalidIndex", err)
	}
}

func TestJournalLoadMissingRecord(t *testing.T) {
	journal := NewJournal()

	_, err := journal.Load(0)
	if !errors.Is(err, ErrRecordNotFound) {
		t.Fatalf("error = %v, want ErrRecordNotFound", err)
	}
}
