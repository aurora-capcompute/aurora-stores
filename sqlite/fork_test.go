package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"aurora-capcompute/aurora"
	"capcompute/dispatcher"
)

func forkRC(rev uint64) aurora.RunContext {
	return aurora.RunContext{TenantID: "t", ThreadID: "th", RunID: "r", Revision: rev}
}

func TestForkJournalSharesParentPrefix(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	ctx := context.Background()

	parent, err := store.OpenJournal(ctx, forkRC(1))
	if err != nil {
		t.Fatalf("open parent: %v", err)
	}
	for i, name := range []string{"a", "b", "c"} {
		if err := parent.Store(i, dispatcher.Call{Name: name, Args: []byte("{}")}, dispatcher.Result([]byte(name))); err != nil {
			t.Fatalf("store %d: %v", i, err)
		}
	}

	// Fork revision 2 from revision 1 sharing the first two records (a, b).
	if err := store.ForkJournal(ctx, forkRC(1), forkRC(2), 2); err != nil {
		t.Fatalf("fork: %v", err)
	}
	child, err := store.OpenJournal(ctx, forkRC(2))
	if err != nil {
		t.Fatalf("open child: %v", err)
	}
	if got := child.Length(); got != 2 {
		t.Fatalf("child length = %d, want 2 (shared prefix)", got)
	}
	for i, want := range []string{"a", "b"} {
		rec, err := child.Load(i)
		if err != nil {
			t.Fatalf("child load %d: %v", i, err)
		}
		if rec.Call.Name != want {
			t.Fatalf("child[%d] = %q, want %q (copy-on-write read from parent)", i, rec.Call.Name, want)
		}
	}

	// Append a divergent tail to the child.
	if err := child.Store(2, dispatcher.Call{Name: "c2", Args: []byte("{}")}, dispatcher.Result([]byte("c2"))); err != nil {
		t.Fatalf("child store tail: %v", err)
	}
	if got := child.Length(); got != 3 {
		t.Fatalf("child length after tail = %d, want 3", got)
	}
	rec, err := child.Load(2)
	if err != nil || rec.Call.Name != "c2" {
		t.Fatalf("child[2] = %q (err %v), want c2", rec.Call.Name, err)
	}

	// The parent revision must remain immutable and addressable.
	if got := parent.Length(); got != 3 {
		t.Fatalf("parent length = %d, want 3 (parent unchanged)", got)
	}
	prec, err := parent.Load(2)
	if err != nil || prec.Call.Name != "c" {
		t.Fatalf("parent[2] = %q (err %v), want c (parent must be immutable)", prec.Call.Name, err)
	}
}

func TestForkJournalChainsThroughRevisions(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	ctx := context.Background()

	j1, _ := store.OpenJournal(ctx, forkRC(1))
	for i, name := range []string{"a", "b", "c", "d"} {
		if err := j1.Store(i, dispatcher.Call{Name: name, Args: []byte("{}")}, dispatcher.Result(nil)); err != nil {
			t.Fatalf("j1 store %d: %v", i, err)
		}
	}

	// rev2 forks rev1 at offset 3 (shares a, b, c), then appends its own tail.
	if err := store.ForkJournal(ctx, forkRC(1), forkRC(2), 3); err != nil {
		t.Fatalf("fork rev2: %v", err)
	}
	j2, _ := store.OpenJournal(ctx, forkRC(2))
	if err := j2.Store(3, dispatcher.Call{Name: "d2", Args: []byte("{}")}, dispatcher.Result(nil)); err != nil {
		t.Fatalf("j2 store: %v", err)
	}

	// rev3 forks rev2 at offset 2: positions 0,1 must resolve through rev2 to rev1.
	if err := store.ForkJournal(ctx, forkRC(2), forkRC(3), 2); err != nil {
		t.Fatalf("fork rev3: %v", err)
	}
	j3, _ := store.OpenJournal(ctx, forkRC(3))
	if got := j3.Length(); got != 2 {
		t.Fatalf("j3 length = %d, want 2", got)
	}
	for i, want := range []string{"a", "b"} {
		rec, err := j3.Load(i)
		if err != nil {
			t.Fatalf("j3 load %d: %v", i, err)
		}
		if rec.Call.Name != want {
			t.Fatalf("j3[%d] = %q, want %q (chained fall-through)", i, rec.Call.Name, want)
		}
	}
}
