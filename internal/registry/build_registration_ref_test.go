package registry

import (
	"context"
	"errors"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/shardkv"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"sync/atomic"
	"testing"
	"time"
)

type replayTransportBarrierKey struct{}

func TestBuildReplayRefGapThroughShardTransport(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cluster := newShardStoreCluster(t, []string{"a", "b", "c"}, 3, 3, 3, 3)
	enteredOld, releaseOld := make(chan struct{}), make(chan struct{})
	enteredNew, releaseNew := make(chan struct{}), make(chan struct{})
	var oldBlocked, newBlocked atomic.Bool
	transport := shardkv.TransportFunc(func(ctx context.Context, member shardkv.MemberID, req shardkv.Request) (shardkv.Response, error) {
		if member == "b" && req.Op == shardkv.OpRead && req.Namespace == shardkv.Namespace(clusterstate.NamespaceNodeLink) && req.RecordSet == clusterstate.RecordSetNodeBuild && req.Key == clusterstate.NodeBuildRecordKey("reused") {
			switch ctx.Value(replayTransportBarrierKey{}) {
			case "old":
				if oldBlocked.CompareAndSwap(false, true) {
					close(enteredOld)
					select {
					case <-releaseOld:
					case <-ctx.Done():
						return shardkv.Response{}, ctx.Err()
					}
				}
			case "new":
				if newBlocked.CompareAndSwap(false, true) {
					close(enteredNew)
					select {
					case <-releaseNew:
					case <-ctx.Done():
						return shardkv.Response{}, ctx.Err()
					}
				}
			}
		}
		return cluster[string(member)].ShardStore().Handle(ctx, req)
	})
	for _, s := range cluster {
		s.SetShardTransport(transport, nil)
	}
	r := New(cluster["a"], placementWithToken("n1"), time.Second, nil)
	owner := &recordingNodeOwner{ackErr: errors.New("first ACK lost")}
	r.SetNodeOwner(owner)
	req := BuildReserveReq{Group: "/g", BuildID: "reused", TemplateID: "transient-old", Profile: types.ProfileE2B, Resources: testWireBuildResources()}
	if _, err := r.ReserveBuild(ctx, req); err == nil {
		t.Fatal("wanted initial ambiguous ACK")
	}
	owner.ackErr = nil
	oldDone, newDone := make(chan error, 1), make(chan error, 1)
	go func() {
		_, err := r.ReserveBuild(context.WithValue(ctx, replayTransportBarrierKey{}, "old"), req)
		oldDone <- err
	}()
	select {
	case <-enteredOld:
	case <-ctx.Done():
		t.Fatal("old barrier", ctx.Err())
	}
	if err := r.applyBuildUpsert(ctx, "n1", &routesync.BuildEvent{BuildID: "reused", TemplateID: req.TemplateID, State: "registered"}); err != nil {
		t.Fatal(err)
	}
	if err := r.applyBuildDelete(ctx, "n1", "reused", req.TemplateID); err != nil {
		t.Fatal(err)
	}
	nextReq := req
	nextReq.TemplateID = "transient-new"
	go func() {
		_, err := r.ReserveBuild(context.WithValue(ctx, replayTransportBarrierKey{}, "new"), nextReq)
		newDone <- err
	}()
	select {
	case <-enteredNew:
	case <-ctx.Done():
		t.Fatal("new barrier", ctx.Err())
	}
	close(releaseOld)
	oldErr := <-oldDone
	afterOld, refFound, err := r.stores.GetNodeBuildRef(ctx, "n1", "reused")
	if err != nil {
		t.Fatal(err)
	}
	commands := len(owner.commands)
	close(releaseNew)
	newErr := <-newDone
	final, found, err := r.stores.GetBuildInGroup(ctx, "/g", "reused")
	if err != nil {
		t.Fatal(err)
	}
	if (refFound && afterOld.TemplateID == req.TemplateID) || commands != 1 || oldErr == nil || !found || final.TemplateID != nextReq.TemplateID || newErr != nil {
		t.Fatalf("stale replay filled successor ref gap: ref=%+v refFound=%v commandsAfterDelete=%d oldErr=%v newErr=%v routeFound=%v route=%+v", afterOld, refFound, commands-1, oldErr, newErr, found, final)
	}
}

func TestBuildLegacyProjectionFullSyncAfterIdentityUpgrade(t *testing.T) {
	ctx := context.Background()
	r := testReg(t)
	r.SetNodeOwner(&recordingNodeOwner{runtime: map[string]*NodeRecord{"n1": {NodeID: "n1", APIEndpoint: "node-api:7443"}}})
	const tid = "transient-legacy"
	const canonical = "e2b-img-pre-upgrade-result"
	if err := r.stores.PutBuild(ctx, &BuildRecord{Group: "/g", BuildID: "legacy", NodeID: "n1", State: BuildReady, TemplateID: canonical, RegistrationTargetSet: true}); err != nil {
		t.Fatal(err)
	}
	baseline := clusterstate.NodeBuildRef{Group: "/g", BuildID: "legacy"}
	if err := r.stores.AddNodeBuildRef(ctx, "n1", baseline); err != nil {
		t.Fatal(err)
	}
	if err := r.applyBuildUpsert(ctx, "n1", &routesync.BuildEvent{BuildID: "legacy", TemplateID: tid, PersistID: canonical, State: "ready"}); err != nil {
		t.Fatal(err)
	}
	res, found, err := r.ResolveBuildByTemplate(ctx, "/g", tid)
	if err != nil || !found || res.TemplateID != tid {
		t.Fatalf("upgrade resolve=%+v found=%v err=%v", res, found, err)
	}
	ref := baseline
	ref.TemplateID = tid
	if err := r.stores.AddNodeBuildRef(ctx, "n1", ref); err != nil {
		t.Fatal(err)
	}
	if err := r.applyNodeBuildFullSnapshot(ctx, "n1", []clusterstate.NodeBuildRef{baseline}, map[string]struct{}{}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := r.stores.GetBuildInGroup(ctx, "/g", "legacy"); err != nil || !found {
		t.Fatalf("older baseline pruned refreshed identity: found=%v err=%v", found, err)
	}
	if err := r.applyNodeBuildFullSnapshot(ctx, "n1", []clusterstate.NodeBuildRef{ref}, map[string]struct{}{}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := r.ResolveBuildByTemplate(ctx, "/g", tid); err != nil || found {
		t.Fatalf("next full sync did not converge deletion: found=%v err=%v", found, err)
	}
	if _, found, err := r.stores.GetNodeBuildRef(ctx, "n1", "legacy"); err != nil || found {
		t.Fatalf("upgraded ref leaked: found=%v err=%v", found, err)
	}
}
