package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"aurora-capcompute/aurora"
)

func TestSaveRunPersistsParentAndChildLinks(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	thread := aurora.StoredThread{
		TenantID: "local", ID: "thread", CreatedAt: now, UpdatedAt: now,
		Manifest: aurora.Manifest{Version: aurora.ManifestVersion, Brain: "brain@1"},
	}
	if err := store.SaveThread(ctx, thread); err != nil {
		t.Fatalf("save thread: %v", err)
	}

	parent := aurora.StoredRun{
		TenantID: "local", ID: "parent", ThreadID: "thread", Revision: 1,
		Message: "m", Status: aurora.RunCompleted, Attempt: 1, CreatedAt: now, UpdatedAt: now,
		EffectiveManifest: aurora.Manifest{Version: aurora.ManifestVersion, Brain: "brain@1"},
		ChildRunIDs:       []string{"child-a", "child-b"},
	}
	child := aurora.StoredRun{
		TenantID: "local", ID: "child-a", ThreadID: "thread", Revision: 1,
		Message: "m", Status: aurora.RunCompleted, Attempt: 1, CreatedAt: now, UpdatedAt: now,
		EffectiveManifest: aurora.Manifest{Version: aurora.ManifestVersion, Brain: "brain@1"},
		ParentRunID:       "parent",
	}
	if err := store.SaveRun(ctx, parent); err != nil {
		t.Fatalf("save parent: %v", err)
	}
	if err := store.SaveRun(ctx, child); err != nil {
		t.Fatalf("save child: %v", err)
	}

	state, err := store.Load(ctx, "local")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	byID := map[string]aurora.StoredRun{}
	for _, r := range state.Runs {
		byID[r.ID] = r
	}
	gotParent, gotChild := byID["parent"], byID["child-a"]
	if len(gotParent.ChildRunIDs) != 2 || gotParent.ChildRunIDs[0] != "child-a" || gotParent.ChildRunIDs[1] != "child-b" {
		t.Fatalf("parent.ChildRunIDs = %v, want [child-a child-b]", gotParent.ChildRunIDs)
	}
	if gotChild.ParentRunID != "parent" {
		t.Fatalf("child.ParentRunID = %q, want parent", gotChild.ParentRunID)
	}
}
