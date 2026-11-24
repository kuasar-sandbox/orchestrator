package registry

import (
	"context"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type identityBuildDispatch struct {
	cmd *routesync.Command
	ack chan *routesync.CmdAck
}
type identityBarrierOwner struct {
	recordingNodeOwner
	dispatch chan identityBuildDispatch
}

func (o *identityBarrierOwner) SendCommandAndWait(ctx context.Context, _ string, cmd *routesync.Command, _ time.Duration) (*routesync.CmdAck, error) {
	d := identityBuildDispatch{cmd, make(chan *routesync.CmdAck, 1)}
	select {
	case o.dispatch <- d:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case ack := <-d.ack:
		return ack, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func TestBuildOldRegistrationACKCannotAcceptReplacement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	r := testReg(t)
	seedTestNode(t, r.stores, "n1")
	publishTestGroups(t, r, "n1", "/g")
	o := &identityBarrierOwner{dispatch: make(chan identityBuildDispatch, 2)}
	r.SetNodeOwner(o)
	type result struct {
		res *BuildReserveResult
		err error
	}
	finished := make(chan result, 2)
	register := func() {
		res, err := r.ReserveBuild(ctx, BuildReserveReq{APIKey: testAPIKeyValue(), Group: "/g", BuildID: "reused", Profile: types.ProfileE2B, Resources: testWireBuildResources()})
		finished <- result{res, err}
	}
	go register()
	old := nextBuildDispatch(t, ctx, o.dispatch)
	if err := testApplyBuildUpsert(r, ctx, "n1", &routesync.BuildEvent{BuildID: "reused", TemplateID: old.cmd.TemplateRef, State: "registered"}); err != nil {
		t.Fatal(err)
	}
	// The node admin can directly delete this owner-free row before registration ACK.
	if err := testApplyBuildDelete(r, ctx, "n1", "reused", old.cmd.TemplateRef); err != nil {
		t.Fatal(err)
	}
	go register()
	next := nextBuildDispatch(t, ctx, o.dispatch)
	if next.cmd.TemplateRef == old.cmd.TemplateRef {
		t.Fatal("same transient")
	}
	old.ack <- &routesync.CmdAck{CmdID: old.cmd.CmdID, Status: routesync.AckAccepted, BuildRegister: &routesync.BuildRegisterResult{}}
	oldResult := <-finished
	current, found, err := r.stores.GetBuildInGroup(ctx, "/g", "reused")
	if err != nil || !found {
		t.Fatal(err)
	}
	// Release the new caller too, without allowing its ACK to hide what the old one changed.
	next.ack <- &routesync.CmdAck{CmdID: next.cmd.CmdID, Status: routesync.AckAccepted, BuildRegister: &routesync.BuildRegisterResult{}}
	<-finished
	if oldResult.err == nil || current.State != BuildStarting || current.RegistrationTargetSet {
		t.Fatalf("stale ACK accepted replacement: old=%s new=%s returned=%+v error=%v current.state=%s targetSet=%v", old.cmd.TemplateRef, next.cmd.TemplateRef, oldResult.res, oldResult.err, current.State, current.RegistrationTargetSet)
	}
}

func TestBuildOldRegistrationRejectionCannotDeleteReplacement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	r := testReg(t)
	seedTestNode(t, r.stores, "n1")
	publishTestGroups(t, r, "n1", "/g")
	o := &identityBarrierOwner{dispatch: make(chan identityBuildDispatch, 2)}
	r.SetNodeOwner(o)
	type result struct {
		res *BuildReserveResult
		err error
	}
	finished := make(chan result, 2)
	register := func() {
		res, err := r.ReserveBuild(ctx, BuildReserveReq{APIKey: testAPIKeyValue(), Group: "/g", BuildID: "reused", Profile: types.ProfileE2B, Resources: testWireBuildResources()})
		finished <- result{res, err}
	}
	go register()
	old := nextBuildDispatch(t, ctx, o.dispatch)
	// An overlapping replay of registration A has committed its node row and
	// Upsert while this earlier A dispatch is still delayed before node handling.
	if err := testApplyBuildUpsert(r, ctx, "n1", &routesync.BuildEvent{BuildID: "reused", TemplateID: old.cmd.TemplateRef, State: "registered"}); err != nil {
		t.Fatal(err)
	}
	if err := testApplyBuildDelete(r, ctx, "n1", "reused", old.cmd.TemplateRef); err != nil {
		t.Fatal(err)
	}
	go register()
	next := nextBuildDispatch(t, ctx, o.dispatch)
	// The delayed A command meets the replacement node row and receives conflict.
	old.ack <- &routesync.CmdAck{CmdID: old.cmd.CmdID, Status: routesync.AckRejected, HTTPStatus: 409, Reason: "immutable definition conflict"}
	oldResult := <-finished
	current, found, err := r.stores.GetBuildInGroup(ctx, "/g", "reused")
	if err != nil {
		t.Fatal(err)
	}
	_, refFound, err := r.stores.GetNodeBuildRef(ctx, "n1", "reused")
	if err != nil {
		t.Fatal(err)
	}
	next.ack <- &routesync.CmdAck{CmdID: next.cmd.CmdID, Status: routesync.AckAccepted, BuildRegister: &routesync.BuildRegisterResult{}}
	<-finished
	if !found || !refFound || current.TemplateID != next.cmd.TemplateRef {
		t.Fatalf("stale rejection deleted replacement: old=%s new=%s result=%+v error=%v routeFound=%v refFound=%v", old.cmd.TemplateRef, next.cmd.TemplateRef, oldResult.res, oldResult.err, found, refFound)
	}
}

func TestBuildStaleReplayRefreshCannotReplaceNewOwnerRef(t *testing.T) {
	ctx := context.Background()
	r := testReg(t)
	seedTestNode(t, r.stores, "n1")
	publishTestGroups(t, r, "n1", "/g")
	r.SetNodeOwner(&recordingNodeOwner{})
	old, err := r.ReserveBuild(ctx, BuildReserveReq{APIKey: testAPIKeyValue(), Group: "/g", BuildID: "reused", Profile: types.ProfileE2B, Resources: testWireBuildResources()})
	if err != nil {
		t.Fatal(err)
	}
	// Captured by the ambiguous-registration replay before concurrent deletion.
	oldRef, found, err := r.stores.GetNodeBuildRef(ctx, "n1", "reused")
	if err != nil || !found {
		t.Fatal(err)
	}
	if err := testApplyBuildDelete(r, ctx, "n1", "reused", old.TemplateID); err != nil {
		t.Fatal(err)
	}
	next, err := r.ReserveBuild(ctx, BuildReserveReq{APIKey: testAPIKeyValue(), Group: "/g", BuildID: "reused", Profile: types.ProfileE2B, Resources: testWireBuildResources()})
	if err != nil {
		t.Fatal(err)
	}
	// Exactly the delayed refresh performed at build.go:143 before dispatch.
	refreshErr := r.stores.AddNodeBuildRef(ctx, "n1", oldRef)
	if err := testApplyBuildUpsert(r, ctx, "n1", &routesync.BuildEvent{BuildID: "reused", TemplateID: next.TemplateID, State: "building"}); err != nil {
		t.Fatal(err)
	}
	current, found, err := r.stores.GetBuildInGroup(ctx, "/g", "reused")
	if err != nil || !found {
		t.Fatal(err)
	}
	ref, _, _ := r.stores.GetNodeBuildRef(ctx, "n1", "reused")
	if ref.TemplateID != next.TemplateID || current.State != BuildBuilding {
		t.Fatalf("stale replay refreshed old owner and dropped new Upsert: old=%s new=%s ref=%s state=%s refreshErr=%v", old.TemplateID, next.TemplateID, ref.TemplateID, current.State, refreshErr)
	}
}

type delayedPlacementKey struct{}

type delayedBuildIntent struct {
	metadata.BuildRepository
	entered, release chan struct{}
}

func (b *delayedBuildIntent) CommitGroupRegistrationIntent(ctx context.Context, record metadata.BuildRecord, ref metadata.NodeBuildRef, admission metadata.GroupAdmission) (metadata.Version, error) {
	if ctx.Value(delayedPlacementKey{}) != nil {
		close(b.entered)
		select {
		case <-b.release:
		case <-ctx.Done():
			return metadata.Version{}, ctx.Err()
		}
	}
	return b.BuildRepository.CommitGroupRegistrationIntent(ctx, record, ref, admission)
}

func TestBuildLatePlacementCannotEraseAcceptedSuccessor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r := testReg(t)
	seedTestNode(t, r.stores, "n1")
	r.SetNodeOwner(&recordingNodeOwner{})
	entered, release := make(chan struct{}), make(chan struct{})
	publishTestGroups(t, r, "n1", "/g")
	r.stores.builds = &delayedBuildIntent{BuildRepository: r.stores.builds, entered: entered, release: release}
	done := make(chan error, 1)
	go func() {
		_, err := r.ReserveBuild(context.WithValue(ctx, delayedPlacementKey{}, true), BuildReserveReq{APIKey: testAPIKeyValue(), Group: "/g", BuildID: "reused", TemplateID: "transient-delayed", Profile: types.ProfileE2B, Resources: testWireBuildResources()})
		done <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("intent commit was not reached")
	}
	replacement, err := r.ReserveBuild(ctx, BuildReserveReq{APIKey: testAPIKeyValue(), Group: "/g", BuildID: "reused", TemplateID: "transient-replacement", Profile: types.ProfileE2B, Resources: testWireBuildResources()})
	if err != nil {
		t.Fatal(err)
	}
	close(release)
	oldErr := <-done
	row, found, err := r.stores.GetBuildInGroup(ctx, "/g", "reused")
	if err != nil {
		t.Fatal(err)
	}
	ref, refFound, err := r.stores.GetNodeBuildRef(ctx, "n1", "reused")
	if err != nil {
		t.Fatal(err)
	}
	if !found || row.TemplateID != replacement.TemplateID || !refFound || ref.TemplateID != replacement.TemplateID {
		t.Fatalf("late placement erased accepted successor: oldErr=%v routeFound=%v row=%+v refFound=%v ref=%+v", oldErr, found, row, refFound, ref)
	}
}

func nextBuildDispatch(t *testing.T, ctx context.Context, ch <-chan identityBuildDispatch) identityBuildDispatch {
	t.Helper()
	select {
	case d := <-ch:
		return d
	case <-ctx.Done():
		t.Fatal("build dispatch not reached:", ctx.Err())
		return identityBuildDispatch{}
	}
}
