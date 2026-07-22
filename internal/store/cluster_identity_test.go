package store

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

func TestClusterIdentityLifecycle(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	if _, err := st.GetClusterIdentity(ctx); !errors.Is(err, ErrClusterIdentityNotEnrolled) {
		t.Fatalf("unenrolled identity error = %v", err)
	}

	enrolled, err := st.EnrollClusterIdentity(ctx, "node-1", "boot-1", "10.0.0.1:8443")
	if err != nil {
		t.Fatal(err)
	}
	if enrolled.NodeEpoch != 1 || enrolled.SessionSeq != 0 || enrolled.EnrollmentID == "" {
		t.Fatalf("enrolled identity = %+v", enrolled)
	}
	if _, err := st.EnrollClusterIdentity(ctx, "node-2", "boot-1", "10.0.0.2:8443"); !errors.Is(err, ErrClusterIdentityEnrolled) {
		t.Fatalf("duplicate enrollment error = %v", err)
	}

	same, err := st.PrepareClusterStart(ctx, "node-1", "boot-1", "10.0.0.1:8443", false)
	if err != nil || same.EpochAdvanced || same.NodeEpoch != 1 {
		t.Fatalf("same start = %+v err=%v", same, err)
	}
	firstSession, err := st.NextClusterSession(ctx, 1)
	if err != nil || firstSession.SessionSeq != 1 {
		t.Fatalf("first session = %+v err=%v", firstSession, err)
	}
	secondSession, err := st.NextClusterSession(ctx, 1)
	if err != nil || secondSession.SessionSeq != 2 {
		t.Fatalf("second session = %+v err=%v", secondSession, err)
	}

	rebooted, err := st.PrepareClusterStart(ctx, "node-1", "boot-2", "10.0.0.1:8443", false)
	if err != nil || !rebooted.EpochAdvanced || rebooted.NodeEpoch != 2 || rebooted.SessionSeq != 0 || rebooted.AdvanceReason != "host_reboot" {
		t.Fatalf("rebooted identity = %+v err=%v", rebooted, err)
	}
	if _, err := st.NextClusterSession(ctx, 1); err == nil {
		t.Fatal("stale process advanced a newer node epoch")
	}
}

func TestClusterIdentityRejectsNodeIDThatCannotFormBinding(t *testing.T) {
	st := testStore(t)
	for name, nodeID := range map[string]string{
		"oversized":    strings.Repeat("n", 129),
		"invalid UTF8": string([]byte{'n', 0xff}),
		"NUL":          "node\x00alias",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := st.EnrollClusterIdentity(
				context.Background(), nodeID, "boot-1", "10.0.0.1:8443",
			); err == nil {
				t.Fatal("unusable node ID was durably enrolled")
			}
		})
	}
}

func TestClusterIdentityRejectsNonCanonicalDataEndpoint(t *testing.T) {
	for name, endpoint := range map[string]string{
		"URL":          "https://node.example:8443",
		"missing port": "node.example",
		"missing host": ":8443",
		"zero port":    "node.example:0",
		"leading zero": "node.example:08443",
		"whitespace":   " node.example:8443",
	} {
		t.Run(name, func(t *testing.T) {
			st := testStore(t)
			if _, err := st.EnrollClusterIdentity(context.Background(), "node-1", "boot-1", endpoint); err == nil {
				t.Fatal("invalid data endpoint was durably enrolled")
			}
		})
	}

	st := testStore(t)
	ctx := context.Background()
	if _, err := st.EnrollClusterIdentity(ctx, "node-1", "boot-1", "[2001:db8::1]:8443"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PrepareClusterStart(ctx, "node-1", "boot-1", "https://node.example:8443", false); err == nil {
		t.Fatal("invalid changed data endpoint was accepted")
	}
}

func TestClusterIdentityEndpointChangeRequiresFence(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	if _, err := st.EnrollClusterIdentity(ctx, "node-1", "boot-1", "10.0.0.1:8443"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PrepareClusterStart(ctx, "node-1", "boot-1", "10.0.0.2:8443", false); !errors.Is(err, ErrPriorNodeEpochNotFenced) {
		t.Fatalf("unfenced endpoint change error = %v", err)
	}
	identity, err := st.GetClusterIdentity(ctx)
	if err != nil || identity.NodeEpoch != 1 || identity.DataEndpoint != "10.0.0.1:8443" {
		t.Fatalf("failed change mutated identity = %+v err=%v", identity, err)
	}
	changed, err := st.PrepareClusterStart(ctx, "node-1", "boot-1", "10.0.0.2:8443", true)
	if err != nil || changed.NodeEpoch != 2 || changed.AdvanceReason != "data_endpoint_change" {
		t.Fatalf("fenced endpoint change = %+v err=%v", changed, err)
	}
}

func TestClusterIdentityConfiguredIDCannotChange(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	if _, err := st.EnrollClusterIdentity(ctx, "node-1", "boot-1", "10.0.0.1:8443"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PrepareClusterStart(ctx, "node-2", "boot-1", "10.0.0.1:8443", false); err == nil {
		t.Fatal("configured node ID changed enrolled identity")
	}
}

func TestClusterSessionSequenceIsSerialized(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	if _, err := st.EnrollClusterIdentity(ctx, "node-1", "boot-1", "10.0.0.1:8443"); err != nil {
		t.Fatal(err)
	}
	const attempts = 16
	results := make(chan ClusterIdentity, attempts)
	errs := make(chan error, attempts)
	var wg sync.WaitGroup
	for range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			identity, err := st.NextClusterSession(ctx, 1)
			if err != nil {
				errs <- err
				return
			}
			results <- identity
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	seen := make(map[uint64]bool, attempts)
	for identity := range results {
		if seen[identity.SessionSeq] {
			t.Fatalf("duplicate session sequence %d", identity.SessionSeq)
		}
		seen[identity.SessionSeq] = true
	}
	if len(seen) != attempts || !seen[1] || !seen[attempts] {
		t.Fatalf("session sequences = %#v", seen)
	}
}
