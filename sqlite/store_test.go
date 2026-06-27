package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"aurora-capcompute/aurora"
)

func TestEventLogAppendReadDurable(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "events.db")
	store, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	scope := aurora.LogScope{TenantID: "t", ThreadID: "th1"}

	head, err := store.Append(ctx, scope,
		aurora.LogEvent{Kind: "run.state", Run: "run1", Rev: 1, Time: time.Unix(0, 0)},
		aurora.LogEvent{Kind: "run.state", Run: "run1", Rev: 1, Time: time.Unix(1, 0)},
	)
	if err != nil || head != 2 {
		t.Fatalf("append head=%d err=%v", head, err)
	}
	if _, err := store.Append(ctx, aurora.LogScope{TenantID: "t", ThreadID: "th2"}, aurora.LogEvent{Kind: "thread.state"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: the log must survive (durability) and read back in order.
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	events, err := reopened.Read(ctx, scope, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Seq != 1 || events[1].Seq != 2 || events[1].Run != "run1" {
		t.Fatalf("read = %+v", events)
	}
	if tail, _ := reopened.Read(ctx, scope, 1); len(tail) != 1 || tail[0].Seq != 2 {
		t.Fatalf("read after 1 = %+v", tail)
	}
	streams, err := reopened.Streams(ctx, "t")
	if err != nil {
		t.Fatal(err)
	}
	if len(streams) != 2 || streams[0].ThreadID != "th1" || streams[1].ThreadID != "th2" {
		t.Fatalf("streams = %+v", streams)
	}
}

func TestLeasesExclusivity(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "leases.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Unix(1000, 0)

	ok, err := store.Acquire(ctx, "t", "run", "r1", "instanceA", now, time.Minute)
	if err != nil || !ok {
		t.Fatalf("first acquire ok=%v err=%v", ok, err)
	}
	// A different holder is rejected while the lease is live.
	if ok, _ := store.Acquire(ctx, "t", "run", "r1", "instanceB", now, time.Minute); ok {
		t.Fatal("second holder acquired a live lease")
	}
	// The same holder can renew.
	if ok, _ := store.Acquire(ctx, "t", "run", "r1", "instanceA", now, time.Minute); !ok {
		t.Fatal("holder could not renew")
	}
	// After expiry, another holder may take it.
	later := now.Add(2 * time.Minute)
	if ok, _ := store.Acquire(ctx, "t", "run", "r1", "instanceB", later, time.Minute); !ok {
		t.Fatal("could not acquire expired lease")
	}
	// Release by the owner frees it.
	if err := store.Release(ctx, "t", "run", "r1", "instanceB"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := store.Acquire(ctx, "t", "run", "r1", "instanceA", later, time.Minute); !ok {
		t.Fatal("could not acquire after release")
	}
}
