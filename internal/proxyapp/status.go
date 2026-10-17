package proxyapp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/proxydrain"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

type RestartID string

var (
	ErrNotReady          = errors.New("proxy is not ready")
	ErrStopped           = errors.New("proxy service is stopped")
	ErrRestartInProgress = errors.New("proxy InplaceRestart is already in progress")
	ErrIncompatible      = errors.New("proxy InplaceRestart is incompatible")
	ErrForcedClose       = proxydrain.ErrForcedClose
)

type WorkerStatus struct {
	Ready    int `json:"ready"`
	Expected int `json:"expected"`
}

// InstanceStatus is a snapshot of business readiness, never a traffic probe.
type InstanceStatus struct {
	Ready          bool         `json:"ready"`
	InstanceID     string       `json:"instance_id"`
	BootID         string       `json:"boot_id"`
	PID            int          `json:"pid"`
	Phase          string       `json:"phase"`
	RoutesSynced   bool         `json:"routes_synced"`
	Workers        WorkerStatus `json:"workers"`
	Data           string       `json:"data"`
	MMDS           string       `json:"mmds"`
	Reasons        []string     `json:"reasons"`
	StatsSocket    string       `json:"stats_socket"`
	MetricsAddress string       `json:"metrics_address"`
}

type RestartStatus struct {
	ID     RestartID `json:"id"`
	Phase  string    `json:"phase"`
	Target string    `json:"target"`
	// Attempts includes instance preparations, failed exec attempts, and
	// retries of retained candidate reclamation after its first drain.
	Attempts         int        `json:"attempts"`
	LastError        string     `json:"last_error,omitempty"`
	NextAttempt      *time.Time `json:"next_attempt,omitempty"`
	Result           string     `json:"result,omitempty"`
	ForcedCloseCount int        `json:"forced_close_count"`
}

// Status separates the queried instance from the entire supervised service.
type Status struct {
	InstanceStatus
	// CoordinatorPID remains the original master PID across exec. A custom
	// entry can use it to keep signal triggers out of B without interpreting
	// registration IDs as process roles.
	CoordinatorPID int              `json:"coordinator_pid"`
	ServiceReady   bool             `json:"service_ready"`
	Instances      []InstanceStatus `json:"instances"`
	InplaceRestart *RestartStatus   `json:"inplace_restart"`
}

type workerState struct {
	pid                                                              int
	epoch                                                            uint64
	peer                                                             *controlPeer
	alive, prepared, ready, dataDone, mmdsDone, stopped, canShutdown bool
}

// Instance owns one generation's control state. The service coordinator owns
// cross-generation actions; stopping this instance never stops its replacement.
type Instance struct {
	mu                sync.Mutex
	identity          controlIdentity
	status            InstanceStatus
	workers           []workerState
	changed           chan struct{}
	stop              chan struct{}
	done              chan struct{}
	stopping          bool
	deadline          time.Time
	err               error
	initialized       bool
	everSynced        bool
	capabilities      []string
	capabilitiesKnown bool
	statusProvider    func() Status
	// A full-service stop may hold MMDS until every instance has drained data.
}

func newBootID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(value[:])
}

func NewInstance(id, bootID string, operation RestartID, workers int) *Instance {
	if bootID == "" {
		bootID = newBootID()
	}
	return &Instance{
		identity: controlIdentity{BootID: bootID, RestartID: operation},
		status:   InstanceStatus{InstanceID: id, BootID: bootID, PID: os.Getpid(), Phase: "starting", Data: "starting", MMDS: "starting", Workers: WorkerStatus{Expected: workers}, Reasons: []string{"initial_sync_pending", "worker_not_ready"}},
		workers:  make([]workerState, workers), changed: make(chan struct{}), stop: make(chan struct{}), done: make(chan struct{}),
	}
}

func (i *Instance) notifyLocked() { close(i.changed); i.changed = make(chan struct{}) }

func (i *Instance) recalculateLocked() {
	i.status.Workers.Ready = 0
	prepared := true
	for _, w := range i.workers {
		prepared = prepared && w.alive && w.prepared
		if w.alive && w.ready {
			i.status.Workers.Ready++
		}
	}
	i.status.Ready = false
	i.status.Reasons = []string{}
	if i.stopping {
		i.status.Reasons = append(i.status.Reasons, "draining")
	}
	if !i.status.RoutesSynced {
		reason := "initial_sync_pending"
		if i.everSynced {
			reason = "route_state_unavailable"
		}
		i.status.Reasons = append(i.status.Reasons, reason)
	}
	if i.status.Workers.Ready != len(i.workers) {
		i.status.Reasons = append(i.status.Reasons, "worker_not_ready")
	}
	if i.status.Phase == "stopped" || i.status.Phase == "failed" {
		i.status.Reasons = append(i.status.Reasons, i.status.Phase)
		return
	}
	i.status.Ready = i.initialized && len(i.status.Reasons) == 0
	if i.stopping {
		i.status.Phase = "draining"
		dataDone, mmdsDone := true, true
		for _, w := range i.workers {
			dataDone = dataDone && (!w.alive || w.dataDone)
			mmdsDone = mmdsDone && (!w.alive || w.mmdsDone)
		}
		if dataDone {
			i.status.Data = "stopped"
		}
		if mmdsDone && i.status.MMDS != "disabled" {
			i.status.MMDS = "stopped"
		}
		return
	}
	if i.initialized && prepared && i.status.Phase == "starting" {
		i.status.Phase = "prepared"
	}
	if i.status.Ready {
		i.status.Phase = "serving"
		i.status.Data = "serving"
		if i.status.MMDS != "disabled" {
			i.status.MMDS = "serving"
		}
	}
}

func (i *Instance) Snapshot() InstanceStatus {
	i.mu.Lock()
	defer i.mu.Unlock()
	s := i.status
	s.Reasons = append([]string{}, s.Reasons...)
	return s
}

func (i *Instance) Status() Status {
	i.mu.Lock()
	provider := i.statusProvider
	i.mu.Unlock()
	if provider != nil {
		return provider()
	}
	s := i.Snapshot()
	return Status{InstanceStatus: s, ServiceReady: s.Ready, Instances: []InstanceStatus{s}}
}

func (i *Instance) WaitReady(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("proxy wait context is nil")
	}
	for {
		i.mu.Lock()
		s, changed, err := i.status, i.changed, i.err
		i.mu.Unlock()
		if s.Ready {
			return nil
		}
		if s.Phase == "stopped" || s.Phase == "failed" || s.Phase == "draining" {
			return errors.Join(ErrStopped, err)
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// requestStop records the first absolute budget, without borrowing caller
// cancellation. It is also valid before initialization has completed.
func (i *Instance) requestStop(deadline time.Time) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.stopping {
		return
	}
	i.stopping = true
	i.deadline = deadline
	i.status.Data = "draining"
	if i.status.MMDS != "disabled" {
		i.status.MMDS = "draining"
	}
	i.recalculateLocked()
	close(i.stop)
	i.notifyLocked()
}

func (i *Instance) finish(err error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.err = errors.Join(i.err, err)
	i.status.Phase = "stopped"
	if err != nil && !i.stopping {
		i.status.Phase = "failed"
	}
	i.status.Data = "stopped"
	if i.status.MMDS != "disabled" {
		i.status.MMDS = "stopped"
	}
	i.recalculateLocked()
	close(i.done)
	i.notifyLocked()
}

func (i *Instance) management(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/readyz" && r.URL.Path != "/status" {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			w.WriteHeader(http.StatusMethodNotAllowed)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "method_not_allowed"})
			return
		}
		if r.URL.Path == "/status" {
			_ = json.NewEncoder(w).Encode(i.Status())
			return
		}
		s := i.Snapshot()
		if !s.Ready {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(s)
	})
}

type readinessSink struct {
	routesync.Sink
	instance *Instance
}

func (s *readinessSink) syncState(ready bool) {
	s.instance.mu.Lock()
	defer s.instance.mu.Unlock()
	s.instance.status.RoutesSynced = ready
	s.instance.everSynced = s.instance.everSynced || ready
	s.instance.recalculateLocked()
	s.instance.notifyLocked()
}
func (s *readinessSink) BeginSync() { s.Sink.BeginSync(); s.syncState(false) }
func (s *readinessSink) Bookmark()  { s.Sink.Bookmark(); s.syncState(true) }
func (s *readinessSink) InvalidateSync() {
	if sink, ok := s.Sink.(routesync.InvalidatableSink); ok {
		sink.InvalidateSync()
	}
	s.syncState(false)
}

func (s *readinessSink) SetProxyCapabilities(capabilities []string) {
	s.instance.mu.Lock()
	defer s.instance.mu.Unlock()
	s.instance.capabilities = append([]string(nil), capabilities...)
	s.instance.capabilitiesKnown = true
	s.instance.notifyLocked()
}
