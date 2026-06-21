package memory

import (
	"capcompute"
	"context"
	"testing"
)

type testSessionKey struct {
	id string
}

func (k testSessionKey) SessionKey() string {
	return k.id
}

func TestSessionStoreSaveLoadDeleteAndList(t *testing.T) {
	ctx := context.Background()
	store := NewSessionStore[string, testSessionKey]()
	session := &capcompute.Session[testSessionKey]{}

	if err := store.SaveSession(ctx, "run-1", session); err != nil {
		t.Fatalf("save: %v", err)
	}
	loaded, err := store.LoadSession(ctx, "run-1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded != session {
		t.Fatal("loaded session differs from saved session")
	}
	sessions, err := store.ListSessions(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(sessions) != 1 || sessions["run-1"] != session {
		t.Fatalf("sessions = %#v", sessions)
	}
	if err := store.DeleteSession(ctx, "run-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := store.LoadSession(ctx, "run-1"); err != capcompute.ErrSessionRequired {
		t.Fatalf("load after delete = %v, want ErrSessionRequired", err)
	}
}
