package cluster

import (
	"context"
	"testing"
	"time"
)

func TestScaleLinkReplicaCompactsExpiredAllocationAndRejectsOldRepair(t *testing.T) {
	ctx := context.Background()
	rep := NewMemoryScaleLinkReplica()
	key := ScaleLinkAllocationKey("/g")
	rec := ScaleLinkRecord{
		Meta: RecordMeta{Ballot: Ballot{Round: 5, Writer: "a"}, Rev: 3, UpdatedAt: time.Now()},
		Key:  key, Kind: ScaleLinkKindAllocation, Group: "/g",
		NodeIDs: []string{"n1"}, KeyFingerprint: "fp", ManifestKeyType: SecretInline, ManifestKey: "mk",
		ExpiresUnixMs: 100,
	}
	if ok, err := rep.Accept(ctx, key, rec, rec.Meta.Ballot); err != nil || !ok {
		t.Fatalf("accept expired allocation ok=%v err=%v", ok, err)
	}
	if got := rep.CompactExpiredAllocations(101); got != 1 {
		t.Fatalf("compacted=%d, want 1", got)
	}
	if _, found, err := rep.Read(ctx, key); err != nil || found {
		t.Fatalf("expired allocation still readable found=%v err=%v", found, err)
	}
	if err := rep.Repair(ctx, key, rec); err != nil {
		t.Fatal(err)
	}
	if _, found, err := rep.Read(ctx, key); err != nil || found {
		t.Fatalf("old repair resurrected compacted allocation found=%v err=%v", found, err)
	}

	next := rec
	next.Meta = RecordMeta{Ballot: Ballot{Round: 6, Writer: "b"}, Rev: 4, UpdatedAt: time.Now()}
	next.ExpiresUnixMs = 200
	if err := rep.Repair(ctx, key, next); err != nil {
		t.Fatal(err)
	}
	if got, found, err := rep.Read(ctx, key); err != nil || !found || got.Meta.Ballot != next.Meta.Ballot {
		t.Fatalf("newer repair not accepted found=%v err=%v got=%+v", found, err, got)
	}
}
