package controlplane

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/session"
)

type meshEnrollmentAuthority struct {
	mu          sync.Mutex
	retireCalls int
}

func (a *meshEnrollmentAuthority) RunSessionRegistration(
	_ context.Context,
	_ session.Registration,
	install func() error,
) error {
	return install()
}

func (a *meshEnrollmentAuthority) RunIdentityRetirement(
	_ context.Context,
	_ session.IdentityRetirement,
	remove func() (bool, error),
) (bool, error) {
	a.mu.Lock()
	a.retireCalls++
	a.mu.Unlock()
	return remove()
}

type meshSessionEndpoint struct {
	mu     sync.Mutex
	fenced bool
}

func (e *meshSessionEndpoint) FenceStaleSession() {
	e.mu.Lock()
	e.fenced = true
	e.mu.Unlock()
}

func (*meshSessionEndpoint) AdmitAndDispatch(
	context.Context,
	session.DispatchCommand,
) (session.DispatchReply, error) {
	return session.DispatchReply{Outcome: clusterstate.DispatchAcceptedAdmitted}, nil
}

func newRetirementTestMesh(
	t *testing.T,
	memberID string,
	peers []SessionPeer,
) (*SessionMesh, *session.Holder, *meshEnrollmentAuthority) {
	t.Helper()
	mesh, err := NewSessionMesh(memberID, session.NewDirectory(nil), peers, nil)
	if err != nil {
		t.Fatal(err)
	}
	authority := &meshEnrollmentAuthority{}
	holder, err := session.NewHolder(memberID, 10, time.Now, nil, mesh, authority)
	if err != nil {
		t.Fatal(err)
	}
	mesh.SetHolder(holder)
	return mesh, holder, authority
}

func TestSessionMeshBroadcastsCommittedIdentityRetirementToEveryHolder(t *testing.T) {
	meshB, holderB, authorityB := newRetirementTestMesh(t, "registry-b", nil)
	muxB := http.NewServeMux()
	meshB.Mount(muxB)
	serverB := httptest.NewServer(muxB)
	defer serverB.Close()

	meshC, holderC, authorityC := newRetirementTestMesh(t, "registry-c", nil)
	muxC := http.NewServeMux()
	meshC.Mount(muxC)
	serverC := httptest.NewServer(muxC)
	defer serverC.Close()

	meshA, holderA, authorityA := newRetirementTestMesh(t, "registry-a", []SessionPeer{
		{MemberID: "registry-b", Endpoint: serverB.URL, Client: serverB.Client()},
		{MemberID: "registry-c", Endpoint: serverC.URL, Client: serverC.Client()},
	})
	registration := session.Registration{
		NodeID: "node-1", EnrollmentID: "enrollment-1",
		Tuple: session.Tuple{NodeEpoch: 7, SessionSeq: 3}, DataEndpoint: "10.0.0.1:8443",
		LoadModelVersion: placement.LoadModelVersion, SandboxSlots: 64,
	}
	endpoint := &meshSessionEndpoint{}
	lease, err := holderB.Register(context.Background(), registration, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	retired, err := meshA.RetireIdentity(context.Background(), session.IdentityRetirement{
		NodeID: registration.NodeID, EnrollmentID: registration.EnrollmentID,
		LastNodeEpoch: registration.NodeEpoch,
	})
	if err != nil || !retired {
		t.Fatalf("RetireIdentity = %v, %v", retired, err)
	}
	endpoint.mu.Lock()
	fenced := endpoint.fenced
	endpoint.mu.Unlock()
	if !fenced || holderB.Active() != 0 || holderA.Active() != 0 || holderC.Active() != 0 {
		t.Fatalf("Holder retirement = fenced=%v active=(%d,%d,%d)", fenced, holderA.Active(), holderB.Active(), holderC.Active())
	}
	for memberID, authority := range map[string]*meshEnrollmentAuthority{
		"registry-a": authorityA, "registry-b": authorityB, "registry-c": authorityC,
	} {
		authority.mu.Lock()
		calls := authority.retireCalls
		authority.mu.Unlock()
		if calls != 1 {
			t.Errorf("%s retirement consensus calls = %d, want 1", memberID, calls)
		}
	}
}
