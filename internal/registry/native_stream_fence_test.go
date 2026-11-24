package registry

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type nativeFenceStream struct {
	send *io.PipeWriter
	body io.ReadCloser
}

func openNativeFenceStream(t *testing.T, r *Registry, version int64) *nativeFenceStream {
	t.Helper()
	server := httptest.NewServer(h2c.NewHandler(http.HandlerFunc(r.ServeNodeLink), &http2.Server{}))
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	reader, writer := io.Pipe()
	t.Cleanup(func() { writer.Close() })
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, server.URL+routesync.NodeLinkPath, reader)
	if err != nil {
		t.Fatal(err)
	}
	client := testH2CClient()
	registered := make(chan error, 1)
	go func() {
		registered <- routesync.WriteMsg(writer, &routesync.Msg{Type: routesync.TypeNodeRegister, NodeReg: &routesync.NodeRegister{GroupSupportVersion: routesync.GroupSupportVersion, NodeID: "n1", APIEndpoint: "api", DataEndpoint: "data"}})
	}()
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { response.Body.Close(); client.CloseIdleConnections() })
	if err := <-registered; err != nil {
		t.Fatal(err)
	}
	hello, err := routesync.ReadMsg(response.Body)
	if err != nil || hello.Type != routesync.TypeHello {
		t.Fatalf("NodeLink hello: %v", err)
	}
	stream := &nativeFenceStream{send: writer, body: response.Body}
	group := nodegroup.Support{Group: "/g", Version: version,
		Entries:        []nodegroup.Entry{{AuthRef: testAPIFingerprint, AuthMaterial: nodegroup.AuthMaterial{APISecret: testAPISecret}}},
		DefaultContext: &nodegroup.DefaultContext{TemplateID: testTemplateRef}}
	for _, frame := range []*routesync.Msg{
		{Type: routesync.TypeGroupBegin, Group: &routesync.GroupReport{SyncID: "snapshot", Sequence: version, Total: 1}},
		{Type: routesync.TypeGroupBatch, Group: &routesync.GroupReport{SyncID: "snapshot", Sequence: version, Groups: []nodegroup.Support{group}}},
		{Type: routesync.TypeGroupEnd, Group: &routesync.GroupReport{SyncID: "snapshot", Sequence: version}},
	} {
		stream.write(t, frame)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		selection, err := r.stores.groups.ReadGroupSupport(t.Context(), "/g", "n1")
		if err != nil {
			t.Fatal(err)
		}
		if selection.Support.Exists && selection.Support.Value.GroupVersion == version {
			return stream
		}
		if time.Now().After(deadline) {
			t.Fatal("real NodeLink did not publish the replacement Group directory")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (s *nativeFenceStream) write(t *testing.T, frame *routesync.Msg) {
	t.Helper()
	if err := routesync.WriteMsg(s.send, frame); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryNativeOldAndNewNodeLinkStreams(t *testing.T) {
	for backend, factory := range registryNativeBackends {
		t.Run(backend, func(t *testing.T) {
			r, _ := newNativeFaultRegistry(t, factory(t))
			old := openNativeFenceStream(t, r, 1)
			selected, err := r.selectGroupNode(t.Context(), PlaceRequest{Group: "/g", APIKey: testAPIKeyValue()})
			if err != nil {
				t.Fatal(err)
			}
			route := testSandboxIdentityRecord("/g", "stream-fence", "sb-stream-fence", "n1", StateReserved)
			route.GroupVersion, route.TemplateSource, route.CreateTemplateID = 1, "default", testTemplateRef
			route.TemplateID = testTemplateRef
			revision, ok, err := r.stores.commitGroupSandboxIntent(t.Context(), route, metadata.Version{}, *selected.GroupAdmission)
			if err != nil || !ok {
				t.Fatalf("intent: %v %v", ok, err)
			}
			oldConn, live := r.node("n1")
			if !live {
				t.Fatal("old stream is not live")
			}
			oldContext, cancelOld := context.WithCancel(t.Context())
			oldDone := make(chan *routesync.CmdAck, 1)
			go func() {
				ack, _ := r.sendAndWait(oldContext, oldConn, &routesync.Command{CmdID: "same-command", Kind: routesync.CmdCreate, SID: route.NodeSandboxID}, time.Second)
				oldDone <- ack
			}()
			command, err := routesync.ReadMsg(old.body)
			if err != nil || command.Cmd == nil || command.Cmd.CmdID != "same-command" {
				t.Fatalf("old command: %v", err)
			}
			cancelOld()
			if ack := <-oldDone; ack != nil {
				t.Fatal("undelivered old reply completed")
			}

			fresh := openNativeFenceStream(t, r, 2)
			freshConn, live := r.node("n1")
			if !live || freshConn == oldConn {
				t.Fatal("NodeLink reconnect did not replace connection")
			}
			current, err := r.stores.nodes.ReadNodeProfile(t.Context(), "n1")
			if err != nil || current.Registration == selected.GroupAdmission.Registration {
				t.Fatal("reconnect reused registration")
			}
			result := make(chan *routesync.CmdAck, 1)
			go func() {
				ack, _ := r.sendAndWait(t.Context(), freshConn, &routesync.Command{CmdID: "same-command", Kind: routesync.CmdCreate, SID: route.NodeSandboxID}, 3*time.Second)
				result <- ack
			}()
			command, err = routesync.ReadMsg(fresh.body)
			if err != nil || command.Cmd == nil || command.Cmd.CmdID != "same-command" {
				t.Fatalf("new command: %v", err)
			}
			// Reuse the exact CmdID deliberately: source connection and original
			// registration, rather than UUID randomness, must reject this reply.
			old.write(t, &routesync.Msg{Type: routesync.TypeCmdAck, Ack: &routesync.CmdAck{CmdID: "same-command", Status: routesync.AckAccepted}})
			late := testE2BRoute(route.SandboxID, routesync.StateRunning)
			late.SandboxID = route.NodeSandboxID
			late.TemplateID = route.TemplateID
			old.write(t, &routesync.Msg{Type: routesync.TypeUpsert, Route: &late})
			// EOF follows processing the ordered ACK and rejecting the stale
			// report. This is a transport barrier, not a timing-based sleep.
			if _, err := io.Copy(io.Discard, old.body); err != nil {
				t.Fatal(err)
			}
			select {
			case ack := <-result:
				t.Fatalf("old stream completed new waiter: %v", ack != nil)
			default:
			}
			stored, gotRevision, found, err := r.stores.GetSandbox(t.Context(), route.Group, route.RouteKey)
			if err != nil || !found || gotRevision != revision || stored.State != StateReserved {
				t.Fatal("old stream changed the current route")
			}
			owned, ownerErr := r.stores.ownership.ReadSandboxOwner(t.Context(), "n1", route.NodeSandboxID)
			if ownerErr != nil || !owned.Exists || owned.Value != metadata.SandboxRef(*route) {
				t.Fatal("old stream changed exact cleanup ownership")
			}
			selection, err := r.stores.groups.ReadGroupSupport(t.Context(), "/g", "n1")
			if err != nil || !selection.Support.Exists || selection.Support.Value.GroupVersion != 2 || selection.Node.Registration != current.Registration {
				t.Fatal("old stream changed replacement Group support")
			}
			fresh.write(t, &routesync.Msg{Type: routesync.TypeCmdAck, Ack: &routesync.CmdAck{CmdID: "same-command", Status: routesync.AckAccepted}})
			if ack := <-result; ack == nil || ack.Status != routesync.AckAccepted {
				t.Fatal("current stream did not complete its own waiter")
			}
			fresh.write(t, &routesync.Msg{Type: routesync.TypeUpsert, Route: &late})
			deadline := time.Now().Add(3 * time.Second)
			for {
				stored, _, found, err = r.stores.GetSandbox(t.Context(), route.Group, route.RouteKey)
				if err != nil {
					t.Fatal(err)
				}
				if found && stored.State == StateReady {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("current stream route did not converge")
				}
				time.Sleep(5 * time.Millisecond)
			}
			t.Log("F05: two real h2c streams; repeated CmdID; stale ACK/report rejected; fresh directory, exact ownership and current report preserved")
		})
	}
}
