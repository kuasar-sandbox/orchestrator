package configsock

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

type retirementTestProvider struct {
	stubProvider
	mu       sync.Mutex
	event    RunRetirementEvent
	features []string
	ack      bool
}

type retirementTestSession struct {
	stubRunSession
	events chan RunRetirementEvent
}

func (s *retirementTestSession) Retirements() <-chan RunRetirementEvent { return s.events }
func (p *retirementTestProvider) RegisterRunSessionFeatures(_ context.Context, kind, runID string, features []string) (RunSessionRegistration, bool, error) {
	p.mu.Lock()
	p.features = append([]string(nil), features...)
	p.mu.Unlock()
	session := &retirementTestSession{events: make(chan RunRetirementEvent, 1)}
	// This is available before the initial response is flushed, exercising the
	// shared JSON decoder's prefetched second response as well as streaming.
	session.events <- p.event
	return session, kind == "sandbox" && runID == p.event.RunID, nil
}
func (p *retirementTestProvider) RunRetirementRole(_ context.Context, kind, runID string, event *RunRetirementEvent) (RunRoleResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if kind != "sandbox" || runID != p.event.RunID {
		return RunRoleResponse{}, errors.New("wrong owner")
	}
	if event != nil {
		if *event != p.event {
			return RunRoleResponse{}, errors.New("wrong epoch")
		}
		p.ack = true
	}
	copy := p.event
	return RunRoleResponse{Pending: true, Retirement: &copy, Acknowledged: p.ack}, nil
}

func TestRunSessionRetirementStreamAndExactAck(t *testing.T) {
	pf := filepath.Join(t.TempDir(), "run.pid")
	mustWrite(t, pf, strconv.Itoa(os.Getpid()))
	provider := &retirementTestProvider{stubProvider: stubProvider{pidFile: pf}, event: RunRetirementEvent{
		OperationID: "operation", SandboxID: "sid", InstanceID: "instance", RunID: "sr-test", CheckpointID: "checkpoint", MemoryID: "memory", Epoch: 7}}
	sock, _ := startTestServer(t, Deps{Provider: provider})
	keeper, err := OpenRunSessionKeeperWithFeatures(context.Background(), sock, "sandbox", "sr-test", []string{FeatureCheckpointHolderRetirementV1})
	if err != nil {
		t.Fatal(err)
	}
	defer keeper.Close()
	var event RunRetirementEvent
	select {
	case event = <-keeper.Retirements():
	case <-time.After(time.Second):
		t.Fatal("retirement response was lost after initial session JSON")
	}
	if event != provider.event {
		t.Fatalf("event = %+v", event)
	}
	stale := event
	stale.Epoch++
	if err := AcknowledgeRunRetirement(context.Background(), sock, "sandbox", stale); err == nil || IsRetryableError(err) {
		t.Fatalf("stale ACK = %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := AcknowledgeRunRetirement(context.Background(), sock, "sandbox", event); err != nil {
			t.Fatal(err)
		}
	}
	role, err := ReadRunRole(context.Background(), sock, "sandbox", "sr-test")
	if err != nil || !role.Acknowledged || role.Retirement == nil || *role.Retirement != event {
		t.Fatalf("durable role = %+v %v", role, err)
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if len(provider.features) != 1 || provider.features[0] != FeatureCheckpointHolderRetirementV1 {
		t.Fatalf("parent did not explicitly advertise support: %v", provider.features)
	}
}

func TestRunSessionLegacyProviderDoesNotPromiseRetirement(t *testing.T) {
	pf := filepath.Join(t.TempDir(), "run.pid")
	mustWrite(t, pf, strconv.Itoa(os.Getpid()))
	sock, _ := startTestServer(t, Deps{Provider: stubProvider{pidFile: pf}})
	if _, err := ReadRunRole(context.Background(), sock, "sandbox", "sr-test"); !errors.Is(err, ErrRunRetirementUnsupported) {
		t.Fatalf("legacy role = %v", err)
	}
}
