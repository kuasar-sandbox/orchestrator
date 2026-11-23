package registry

import (
	"context"
	"errors"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"sync"
	"testing"
	"time"
)

type replayOwnershipBarrier struct {
	metadata.BuildRepository
	entered, release chan struct{}
	once             sync.Once
}

func (b *replayOwnershipBarrier) EnsureRegistrationOwnership(ctx context.Context, key metadata.BuildKey, node, template string) error {
	if template == "transient-old" {
		b.once.Do(func() { close(b.entered) })
		select {
		case <-b.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return b.BuildRepository.EnsureRegistrationOwnership(ctx, key, node, template)
}
func TestBuildReplayCannotPublishOwnershipAfterIdentityReplacement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r := testReg(t)
	seedTestNode(t, r.stores, "n1")
	if err := putTestNode(r.stores, ctx, &NodeRecord{NodeID: "n1"}); err != nil {
		t.Fatal(err)
	}
	publishTestGroups(t, r, "n1", "/g")
	owner := &recordingNodeOwner{ackErr: errors.New("first ACK lost")}
	r.SetNodeOwner(owner)
	req := BuildReserveReq{APIKey: testAPIKeyValue(), Group: "/g", BuildID: "reused", TemplateID: "transient-old", Profile: types.ProfileE2B, Resources: testWireBuildResources()}
	if _, err := r.ReserveBuild(ctx, req); err == nil {
		t.Fatal("wanted ambiguous first dispatch")
	}
	owner.ackErr = nil
	barrier := &replayOwnershipBarrier{BuildRepository: r.stores.builds, entered: make(chan struct{}), release: make(chan struct{})}
	r.stores.builds = barrier
	done := make(chan error, 1)
	go func() { _, err := r.ReserveBuild(ctx, req); done <- err }()
	select {
	case <-barrier.entered:
	case <-ctx.Done():
		t.Fatal("replay did not enter barrier")
	}
	if err := testApplyBuildUpsert(r, ctx, "n1", &routesync.BuildEvent{BuildID: req.BuildID, TemplateID: req.TemplateID, State: "registered"}); err != nil {
		t.Fatal(err)
	}
	if err := testApplyBuildDelete(r, ctx, "n1", req.BuildID, req.TemplateID); err != nil {
		t.Fatal(err)
	}
	successor := req
	successor.TemplateID = "transient-new"
	if _, err := r.ReserveBuild(ctx, successor); err != nil {
		t.Fatal(err)
	}
	commandsBefore := len(owner.commands)
	close(barrier.release)
	if err := <-done; err == nil {
		t.Fatal("stale replay succeeded")
	}
	if len(owner.commands) != commandsBefore {
		t.Fatal("stale replay dispatched a command")
	}
	ref, found, err := r.stores.GetNodeBuildRef(ctx, "n1", req.BuildID)
	if err != nil || !found || ref.TemplateID != successor.TemplateID {
		t.Fatalf("successor ownership=%+v found=%v err=%v", ref, found, err)
	}
	record, found, err := r.stores.GetBuildInGroup(ctx, req.Group, req.BuildID)
	if err != nil || !found || record.TemplateID != successor.TemplateID {
		t.Fatalf("successor build=%+v found=%v err=%v", record, found, err)
	}
}
