package proxyapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync"
	"time"

	proxyextension "github.com/kuasar-sandbox/orchestrator/app/proxy/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

// Startup transfers process-local runtime ownership to the service. Release is
// called after the instance and its workers have ended, including failed starts.
type Startup struct {
	Effective *EffectiveConfig
	Runtime   *Runtime
	Release   func()
}

type PrepareService func(context.Context, ServiceInput) (*Startup, error)
type MasterRunner func(context.Context, *EffectiveConfig, *Runtime) error

// Service coordinates one original-PID instance and at most one child. All
// public actions, management responses and private control use this same state.
type Service struct {
	mu sync.Mutex
	// actionMu orders stop acceptance against process creation and exec. No
	// application callback or drain runs while it is held.
	actionMu          sync.Mutex
	input             ServiceInput
	baseline          *EffectiveConfig
	prepare           PrepareService
	prepareCancel     context.CancelFunc
	runner            MasterRunner
	local             *Instance
	localDone         chan error
	localEnded        bool
	localResult       error
	localExtension    any
	resources         *retainedResources
	child             *serviceChild
	operation         *RestartStatus
	target            *restartTarget
	stopRecord        *stopRecord
	stopDeadline      time.Time
	stopRequested     bool
	stop              chan struct{}
	stopOnce          sync.Once
	stopStarted       bool
	done              chan struct{}
	changed           chan struct{}
	requests          chan struct{}
	preparing         bool
	running           bool
	started           bool
	entryPending      bool
	entryReady        chan struct{}
	entryFinished     sync.Once
	entryErr          error
	earlyStopAt       time.Time
	earlyAutomatic    bool
	closing           bool
	finished          sync.Once
	knownService      *Status
	candidate         bool
	candidateDeadline time.Time // instance drain received before business starts
	parent            *serviceWire
	candidateDialMu   sync.Mutex
	candidateDialID   uint64
	configReplies     chan serviceMessage
	err               error
	stopErr           error
	timeout           time.Duration
	logger            *slog.Logger
	coordinatorPID    int
}

// NewService performs no I/O. Runtime resources are acquired by Run.
func NewService() *Service {
	// Observe only the private marker; descriptor ownership still starts in
	// ReceiveContinuation. An early stop must await restoration of B/A-prime's
	// role and supervision instead of treating it as an unused ordinary App.
	return &Service{entryPending: ContinuationPresent(), entryReady: make(chan struct{}), stop: make(chan struct{}), done: make(chan struct{}), changed: make(chan struct{}), requests: make(chan struct{}, 1), coordinatorPID: os.Getpid()}
}

func (s *Service) notifyLocked() { close(s.changed); s.changed = make(chan struct{}) }

func (s *Service) Status() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := Status{InstanceStatus: InstanceStatus{Phase: "starting", Reasons: []string{"not_started"}}, CoordinatorPID: s.coordinatorPID, Instances: []InstanceStatus{}}
	if s.local != nil {
		result.InstanceStatus = s.local.Snapshot()
		result.Instances = append(result.Instances, result.InstanceStatus)
	}
	if s.child != nil {
		result.Instances = append(result.Instances, s.child.snapshot())
	}
	for _, instance := range result.Instances {
		result.ServiceReady = result.ServiceReady || instance.Ready
	}
	if s.stopping() {
		result.ServiceReady = false
	}
	if s.candidate && s.knownService != nil {
		local := result.InstanceStatus
		result = copyStatus(*s.knownService)
		result.InstanceStatus = local
		for index := range result.Instances {
			if result.Instances[index].BootID == local.BootID {
				result.Instances[index] = local
			}
		}
		ready := false
		for _, instance := range result.Instances {
			ready = ready || instance.Ready
		}
		result.ServiceReady = result.ServiceReady && ready && !s.stopping()
		return result
	}
	if s.operation != nil {
		operation := *s.operation
		if operation.NextAttempt != nil {
			stamp := *operation.NextAttempt
			operation.NextAttempt = &stamp
		}
		result.InplaceRestart = &operation
	}
	return result
}

// WaitReady waits for this process's business readiness. Cancellation affects
// only this wait. It never sends synthetic requests or starts a stopped service.
func (s *Service) WaitReady(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("proxy wait context is nil")
	}
	for {
		s.mu.Lock()
		instance, changed := s.local, s.changed
		s.mu.Unlock()
		if instance != nil {
			return instance.WaitReady(ctx)
		}
		select {
		case <-changed:
		case <-s.done:
			return errors.Join(ErrStopped, s.result())
		case <-s.stop:
			return ErrStopped
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Service) result() error { s.mu.Lock(); defer s.mu.Unlock(); return s.err }

// Shutdown requests graceful stop of the entire service, including B. A positive
// configured timeout bounds the drain; zero leaves this explicit drain unbounded.
// The first request fixes the deadline; caller cancellation only ends this wait.
// Handlers should request shutdown asynchronously rather than wait for themselves.
func (s *Service) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("proxy shutdown context is nil")
	}
	select {
	case <-s.done:
		return s.result()
	default:
	}
	// Initiation belongs to the service as well: a candidate's control write
	// or a launch/exec critical section must not hold the caller past its
	// context. Cancellation leaves this request and the fixed drain running.
	requested := make(chan error, 1)
	go func() { requested <- s.requestShutdown() }()
	select {
	case err := <-requested:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-s.done:
		return s.result()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// FinishEntry wakes lifecycle waiters when an entry fails before Run can take
// ownership, for example when receiving a component bootstrap fails.
func (s *Service) FinishEntry(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return
	}
	s.started = true
	s.err = err
	s.entryErr = err
	s.entryPending = false
	s.entryFinished.Do(func() { close(s.entryReady) })
	s.finished.Do(func() { close(s.done) })
	s.notifyLocked()
}

func (s *Service) requestShutdown() error {
	return s.requestShutdownAt(time.Time{})
}

func (s *Service) requestAutomaticShutdown() error {
	return s.requestShutdownRequest(time.Time{}, true)
}

func (s *Service) requestShutdownAt(requestedDeadline time.Time) error {
	return s.requestShutdownRequest(requestedDeadline, false)
}

func (s *Service) requestShutdownRequest(requestedDeadline time.Time, automatic bool) error {
	s.mu.Lock()
	if s.entryPending {
		if s.earlyStopAt.IsZero() {
			s.earlyStopAt = time.Now()
			s.earlyAutomatic = automatic
			s.notifyLocked()
		}
		ready := s.entryReady
		s.mu.Unlock()
		<-ready
		return s.requestShutdownRequest(requestedDeadline, automatic)
	}
	if s.entryErr != nil {
		err := s.entryErr
		s.mu.Unlock()
		return err
	}
	if s.closing {
		s.mu.Unlock()
		return nil
	}
	if !s.started {
		s.stopOnce.Do(func() { close(s.stop) })
		s.finished.Do(func() { close(s.done) })
		s.notifyLocked()
		s.mu.Unlock()
		return nil
	}
	record, timeout, parent, candidate := s.stopRecord, s.timeout, s.parent, s.candidate
	deadline := s.stopDeadline
	if !s.stopRequested {
		deadline = requestedDeadline
		if deadline.IsZero() && (automatic || timeout > 0) {
			deadline = time.Now().Add(timeout)
		}
	}
	if candidate {
		s.stopRequested = true
		s.stopDeadline = deadline
		if s.localDone == nil {
			s.stopUnstartedLocalLocked()
		}
		s.mu.Unlock()
		if parent == nil {
			return ErrNotReady
		}
		return parent.send(serviceMessage{Kind: "shutdown", Deadline: deadline})
	}
	if record != nil {
		var err error
		deadline, err = record.request(deadline)
		if err != nil {
			s.mu.Unlock()
			return err
		}
	}
	// Publish before waiting for actionMu. If exec has already passed its
	// final check, the successor still receives this same durable record.
	// Keeping mu until the write also excludes releaseService's file close.
	s.stopDeadline = deadline
	s.stopRequested = true
	s.stopOnce.Do(func() { close(s.stop) })
	s.mu.Unlock()
	s.beginShutdown(deadline)
	return nil
}

// Stop both endpoints in each owned instance. Each drains its own accepted
// work; there is no cross-endpoint or cross-worker request-sequence guarantee.
func (s *Service) beginShutdown(deadline time.Time) {
	s.actionMu.Lock()
	defer s.actionMu.Unlock()
	s.mu.Lock()
	if s.closing || s.stopStarted {
		s.mu.Unlock()
		return
	}
	s.stopStarted = true
	if s.prepareCancel != nil {
		s.prepareCancel()
	}
	instance, child, done := s.local, s.child, s.localDone
	s.mu.Unlock()
	if instance != nil && done != nil {
		instance.requestStop(deadline)
	}
	if child != nil {
		child.mu.Lock()
		child.draining = true
		child.mu.Unlock()
		_ = child.wire.send(serviceMessage{Kind: "drain", Deadline: deadline})
	}
}

func (s *Service) stopping() bool {
	select {
	case <-s.stop:
		return true
	default:
		return false
	}
}

// InplaceRestart returns after acceptance, not after exec. Cancellation before
// acceptance aborts preparation. Afterwards the engine owns the operation and
// reports its outcome through Status; the original Go stack cannot survive exec.
func (s *Service) InplaceRestart(ctx context.Context) (RestartID, error) {
	if ctx == nil {
		return "", fmt.Errorf("proxy restart context is nil")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.mu.Lock()
	if s.stopping() {
		s.mu.Unlock()
		return "", ErrStopped
	}
	if s.candidate {
		s.mu.Unlock()
		return "", fmt.Errorf("%w: candidate cannot start a nested operation", ErrIncompatible)
	}
	if s.preparing || (s.operation != nil && s.operation.Result == "") {
		s.mu.Unlock()
		return "", ErrRestartInProgress
	}
	if !s.running || s.local == nil || s.resources == nil || !s.local.Snapshot().Ready {
		s.mu.Unlock()
		return "", ErrNotReady
	}
	if s.timeout <= 0 {
		s.mu.Unlock()
		return "", fmt.Errorf("%w: InplaceRestart requires a positive shutdown_timeout", ErrIncompatible)
	}
	local, extension, input := s.local, s.localExtension, s.input
	local.mu.Lock()
	supported := slices.Contains(local.capabilities, routesync.InplaceRestartV1)
	local.mu.Unlock()
	if !supported || !local.extensionCompatible() {
		s.mu.Unlock()
		return "", fmt.Errorf("%w: conductor or worker shutdown contract", ErrIncompatible)
	}
	if extension != nil {
		if _, ok := extension.(proxyextension.Shutdown); !ok {
			s.mu.Unlock()
			return "", fmt.Errorf("%w: master extension has no Shutdown", ErrIncompatible)
		}
	}
	if _, err := candidateMetricsAddress(s.baseline.config.MetricsListen); err != nil {
		s.mu.Unlock()
		return "", err
	}
	if s.baseline.config.MetricsListen != "" && s.resources.Metrics == nil {
		s.mu.Unlock()
		return "", fmt.Errorf("%w: configured metrics listener is unavailable", ErrIncompatible)
	}
	s.preparing = true
	s.mu.Unlock()
	target, err := openRestartTarget(ctx, input.Executable)
	s.mu.Lock()
	var logOperation func()
	defer func() {
		s.mu.Unlock()
		if logOperation != nil {
			logOperation()
		}
	}()
	s.preparing = false
	if err != nil {
		return "", err
	}
	if err = ctx.Err(); err != nil {
		target.file.Close()
		return "", err
	}
	if s.stopping() {
		target.file.Close()
		return "", ErrStopped
	}
	if !local.Snapshot().Ready {
		target.file.Close()
		return "", ErrNotReady
	}
	id := RestartID(newBootID())
	s.target = target
	s.operation = &RestartStatus{ID: id, Phase: "preparing_b", Target: target.description()}
	logOperation = s.operationLogLocked()
	s.notifyLocked()
	s.requests <- struct{}{}
	return id, nil
}

// Run consumes a continuation before invoking any application preparation. In
// resume mode child supervision and the persisted stop deadline are restored
// before Configure, TLS resolution, extension startup or listener acquisition.
func (s *Service) Run(ctx context.Context, input ServiceInput, continuation *Continuation, prepare PrepareService, runner MasterRunner) (returnErr error) {
	if ctx == nil || prepare == nil || runner == nil {
		return fmt.Errorf("proxy service has unresolved startup")
	}
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return fmt.Errorf("proxy service already running")
	}
	if s.stopping() {
		s.mu.Unlock()
		return nil
	}
	s.started = true
	s.running = true
	s.input = input
	s.prepare = prepare
	s.runner = runner
	// Publish startup only after inherited role, control and child supervision
	// have been restored under this same lock. No stop can observe a temporary
	// default coordinator role or freeze an incomplete participant set.
	var restoreErr error
	if continuation != nil {
		restoreErr = s.restoreLocked(continuation)
	} else if s.entryPending {
		restoreErr = fmt.Errorf("proxy continuation was not received")
	} else {
		var record *stopRecord
		record, restoreErr = newStopRecord()
		if restoreErr == nil {
			restoreErr = s.installStopRecordLocked(record)
			if restoreErr != nil {
				record.file.Close()
			}
		}
	}
	if restoreErr == nil && !s.earlyStopAt.IsZero() {
		s.stopRequested = true
		if s.earlyAutomatic || s.timeout > 0 {
			s.stopDeadline = s.earlyStopAt.Add(s.timeout)
		}
		if !s.candidate {
			s.stopDeadline, restoreErr = s.stopRecord.request(s.stopDeadline)
		}
	}
	if restoreErr == nil && !s.candidate {
		var deadline time.Time
		var requested bool
		deadline, requested, restoreErr = s.stopRecord.read()
		if requested {
			s.stopRequested = true
			s.stopDeadline = deadline
			s.stopOnce.Do(func() { close(s.stop) })
		}
	}
	s.entryErr = restoreErr
	s.entryPending = false
	s.entryFinished.Do(func() { close(s.entryReady) })
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.err = returnErr
		s.running = false
		s.notifyLocked()
		s.finished.Do(func() { close(s.done) })
		s.mu.Unlock()
	}()
	defer s.releaseService()
	if continuation != nil {
		defer continuation.Close()
	}
	if restoreErr != nil {
		// A malformed inherited contract is fatal, unlike retryable application
		// preparation. Finish any restored child without abandoning supervision.
		return errors.Join(restoreErr, s.reapChild(s.child, time.Now().Add(s.timeout)))
	}
	if s.candidate {
		return s.runCandidate(ctx)
	}
	if s.stopping() {
		return s.shutdownAll()
	}
	watchDone := make(chan struct{})
	defer close(watchDone)
	go s.publishService(watchDone)
	go func() {
		select {
		case <-ctx.Done():
			_ = s.requestAutomaticShutdown()
		case <-watchDone:
		}
	}()
	if continuation != nil {
		if err := s.resumeOperation(); err != nil {
			return err
		}
	} else if err := s.startLocal(ctx, "", routesync.ProxyPluginID, nil); err != nil {
		if s.stopping() {
			return s.shutdownAll()
		}
		return err
	}
	for {
		if s.stopping() {
			return s.shutdownAll()
		}
		s.mu.Lock()
		localDone := s.localDone
		s.mu.Unlock()
		select {
		case <-s.stop:
			return s.shutdownAll()
		case <-s.requests:
			if err := s.restart(); err != nil {
				return err
			}
		case err := <-localDone:
			s.mu.Lock()
			s.localEnded, s.localResult = true, err
			s.mu.Unlock()
			return err
		}
	}
}

func (s *Service) startLocal(ctx context.Context, boot, id string, resources *retainedResources) error {
	s.mu.Lock()
	if err := s.checkLocalStartLocked(); err != nil {
		s.mu.Unlock()
		return err
	}
	if s.localDone != nil {
		s.mu.Unlock()
		return fmt.Errorf("previous local instance has not ended")
	}
	ctx, cancel := context.WithCancel(ctx)
	s.prepareCancel = cancel
	s.mu.Unlock()
	defer func() {
		cancel()
		s.mu.Lock()
		s.prepareCancel = nil
		s.mu.Unlock()
	}()
	input := s.input
	input.Config = append(json.RawMessage(nil), input.Config...)
	startup, err := s.prepare(ctx, input)
	if err != nil {
		s.mu.Lock()
		if s.stopping() {
			s.mu.Unlock()
			if errors.Is(err, context.Canceled) {
				return errors.Join(ErrStopped, err)
			}
			return err
		}
		if s.baseline != nil {
			operation := RestartID("")
			if s.operation != nil {
				operation = s.operation.ID
			}
			instance := NewInstance(id, boot, operation, s.baseline.config.Workers)
			instance.finish(err)
			s.local = instance
			s.localDone = nil
			s.notifyLocked()
		}
		s.mu.Unlock()
		return err
	}
	if startup == nil || startup.Effective == nil || startup.Runtime == nil {
		if startup != nil && startup.Release != nil {
			startup.Release()
		}
		return fmt.Errorf("proxy startup is incomplete")
	}
	release := startup.Release
	failed := true
	defer func() {
		if failed && release != nil {
			release()
		}
	}()
	s.mu.Lock()
	if err := s.checkLocalStartLocked(); err != nil {
		s.mu.Unlock()
		return err
	}
	if s.baseline == nil {
		s.baseline = startup.Effective
	} else if !compatibleTakeover(s.baseline.config, startup.Effective.config) {
		s.mu.Unlock()
		return fmt.Errorf("%w: Configure changed a required takeover setting", ErrIncompatible)
	}
	config := startup.Effective.Config()
	if s.candidate {
		config.StatsSocket = resources.StatsPath
		config.ShmPath = resources.ShmPath
		config.MetricsListen = resources.MetricsExpression
	}
	effective, err := FreezeConfig(config)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	operation := RestartID("")
	if s.operation != nil {
		operation = s.operation.ID
	}
	instance := NewInstance(id, boot, operation, config.Workers)
	s.localEnded, s.localResult = false, nil
	instance.statusProvider = s.Status
	startup.Runtime.Instance = instance
	startup.Runtime.inplaceRestart = s.restarting
	if s.candidate {
		startup.Runtime.candidateDial = s.dialCandidateConfig
	}
	startup.Runtime.inherited = resources
	startup.Runtime.retain = func(retained *retainedResources) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.resources == nil {
			s.resources = retained
		} else {
			retained.close()
		}
		return nil
	}
	s.local = instance
	s.timeout = config.ShutdownTimeoutDur()
	s.localExtension = startup.Runtime.MasterExtension
	s.logger = startup.Runtime.Logger
	done := make(chan error, 1)
	s.localDone = done
	s.notifyLocked()
	s.mu.Unlock()
	failed = false
	go func() {
		err := s.runner(context.WithoutCancel(ctx), effective, startup.Runtime)
		if release != nil {
			release()
		}
		done <- err
	}()
	return nil
}

// An unstarted candidate has no data/MMDS work to protect. Cancel its
// preparation; an already-running B drains its own accepted work.
func (s *Service) stopUnstartedLocalLocked() {
	s.stopOnce.Do(func() { close(s.stop) })
	if s.prepareCancel != nil {
		s.prepareCancel()
	}
}

func (s *Service) checkLocalStartLocked() error {
	if s.stopping() {
		return ErrStopped
	}
	if s.candidate {
		_, requested, err := s.stopRecord.read()
		if err != nil {
			return err
		}
		if s.stopRequested || requested {
			s.stopUnstartedLocalLocked()
			return ErrStopped
		}
	}
	return nil
}

func (s *Service) phase(phase string, err error) {
	s.mu.Lock()
	s.operation.Phase = phase
	s.operation.NextAttempt = nil
	if err != nil {
		s.operation.LastError = err.Error()
	}
	logOperation := s.operationLogLocked()
	s.notifyLocked()
	s.mu.Unlock()
	logOperation()
}

func (s *Service) operationComplete(result string, err error) {
	s.phase("completed", err)
	s.mu.Lock()
	s.operation.Result = result
	logOperation := s.operationLogLocked()
	target := s.target
	s.target = nil
	s.notifyLocked()
	s.mu.Unlock()
	logOperation()
	if target != nil {
		target.file.Close()
	}
}

func (s *Service) releaseService() {
	s.actionMu.Lock()
	defer s.actionMu.Unlock()
	s.mu.Lock()
	s.closing = true
	resources, target, record := s.resources, s.target, s.stopRecord
	s.mu.Unlock()
	if resources != nil {
		resources.close()
		if !s.candidate {
			_ = resources.removePaths()
		}
	}
	if target != nil {
		target.file.Close()
	}
	if record != nil {
		record.file.Close()
	}
	if s.parent != nil {
		s.parent.conn.Close()
	}
}

// The retry delay is independent of preparation and drain budgets.
func (s *Service) backoff(attempt int, err error) bool {
	delays := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second}
	index := attempt - 1
	if index < 0 {
		index = 0
	}
	if index >= len(delays) {
		index = len(delays) - 1
	}
	next := time.Now().Add(delays[index])
	s.mu.Lock()
	s.operation.Phase = "recovering"
	s.operation.LastError = err.Error()
	s.operation.NextAttempt = &next
	logOperation := s.operationLogLocked()
	s.notifyLocked()
	s.mu.Unlock()
	logOperation()
	timer := time.NewTimer(time.Until(next))
	defer timer.Stop()
	select {
	case <-timer.C:
		return !s.stopping()
	case <-s.stop:
		return false
	}
}

// Copy under the state lock, then invoke the application-supplied logger after
// unlocking. A logger may itself inspect the public status snapshot.
func (s *Service) operationLogLocked() func() {
	logger := s.logger
	if logger == nil {
		logger = slog.Default()
	}
	op := *s.operation
	return func() {
		logger.Info("proxy InplaceRestart", "id", op.ID, "phase", op.Phase,
			"target", op.Target, "attempts", op.Attempts, "last_error", op.LastError,
			"next_attempt", op.NextAttempt, "result", op.Result,
			"forced_close_count", op.ForcedCloseCount)
	}
}

// B is always transitional. A and A-prime expose the accepted operation until
// its cleanup has finished; this does not promise complete cross-instance stats.
func (s *Service) restarting() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.candidate || (s.operation != nil && s.operation.Result == "")
}
