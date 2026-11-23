package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

type delayedGroupProfile struct {
	metadata.NodeRepository
	afterRead func()
}

func (n *delayedGroupProfile) ReadNodeProfile(ctx context.Context, id string) (metadata.NodeSnapshot, error) {
	profile, err := n.NodeRepository.ReadNodeProfile(ctx, id)
	if n.afterRead != nil {
		n.afterRead()
	}
	return profile, err
}

func TestGroupRelayChecksSourceLeaseAfterProfileRead(t *testing.T) {
	for _, expired := range []bool{false, true} {
		t.Run(map[bool]string{false: "current", true: "expired_during_read"}[expired], func(t *testing.T) {
			r := testReg(t)
			seedTestNode(t, r.stores, "node")
			lease, err := r.acquireImportSourceLease(t.Context(), ImportSourceLeaseRequest{SourceID: "source", OwnerID: "owner", RunID: "run", TTLMillis: 500})
			if err != nil || !lease.Acquired {
				t.Fatalf("lease: %v", err)
			}
			profile, err := r.stores.nodes.ReadNodeProfile(t.Context(), "node")
			if err != nil {
				t.Fatal(err)
			}
			if expired {
				r.stores.nodes = &delayedGroupProfile{NodeRepository: r.stores.nodes, afterRead: func() {
					time.Sleep(time.Until(time.UnixMilli(lease.Lease.ExpiresUnixMs).Add(5 * time.Millisecond)))
				}}
			}
			o := &groupCommandOwner{}
			o.call = func(ctx context.Context, id string, cmd *routesync.Command) (*routesync.CmdAck, error) {
				if id != "node" || ctx.Value(nodeRegistrationContextKey{}) != profile.Registration {
					t.Fatal("relay lost original registration")
				}
				return &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted}, nil
			}
			r.SetNodeOwner(o)
			body, err := json.Marshal(NodeGroupRequest{NodeID: "node", Kind: routesync.CmdGroupGet, Group: routesync.GroupCommand{Group: "/g", APIKey: testAPIKeyValue()}, Lease: ImportLease{SourceID: "source", OwnerID: "owner", RunID: "run", Term: lease.Lease.Term}})
			if err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			r.serveNodeGroup(response, httptest.NewRequest(http.MethodPost, PlacerLinkNodeGroupPath, bytes.NewReader(body)))
			want, calls := http.StatusOK, 1
			if expired {
				want, calls = http.StatusConflict, 0
			}
			if response.Code != want || len(o.commands) != calls {
				t.Fatalf("status=%d forwarded=%d, want %d/%d", response.Code, len(o.commands), want, calls)
			}
			pairs, err := r.stores.keyPairs.ListKeyPairs(t.Context(), "node", "", 128)
			if err != nil || len(pairs.Items) != 0 {
				t.Fatal("Group relay wrote Registry key cache")
			}
		})
	}
}

func TestRemovedPlacerEndpointsAreNotExposed(t *testing.T) {
	r := testReg(t)
	mux := http.NewServeMux()
	r.ServePlacerLink(mux)
	for _, path := range []string{"/placer-link/place", "/placer-link/verify-key", "/placer-link/selector-patch"} {
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, httptest.NewRequest(http.MethodPost, path, nil))
		if response.Code != http.StatusNotFound {
			t.Fatalf("obsolete %s remains exposed: %d", path, response.Code)
		}
	}
}

func TestHTTPNodeOwnerRejectsMismatchedCommandAck(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(nodeOwnerResponse{Ack: &routesync.CmdAck{CmdID: "unrelated", Status: routesync.AckRejected, ErrorCode: routesync.ErrorGroupVersionConflict}})
	}))
	defer server.Close()
	owner := NewHTTPNodeOwner(server.URL, server.Client())
	ack, err := owner.SendCommandAndWait(t.Context(), "node", &routesync.Command{CmdID: "requested", Kind: routesync.CmdCreate}, time.Second)
	if err == nil || ack != nil {
		t.Fatalf("unrelated rejection supplied retirement proof: %+v %v", ack, err)
	}
}
