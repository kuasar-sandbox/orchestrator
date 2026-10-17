package proxyapp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	proxyextension "github.com/kuasar-sandbox/orchestrator/app/proxy/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/componentexec"
	"github.com/kuasar-sandbox/orchestrator/internal/listenresource"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"golang.org/x/sys/unix"
)

// Run holds mu across role restoration and publication to lifecycle callers.
func (s *Service) restoreLocked(c *Continuation) error {
	e := c.envelope
	s.input = e.Input
	s.operation = &e.Operation
	baseline, err := effectiveConfigFromRaw(e.Baseline, sha256.Sum256(e.Baseline))
	if err != nil {
		return err
	}
	s.baseline = baseline
	s.timeout = baseline.config.ShutdownTimeoutDur()
	s.candidate = e.Mode == "candidate"
	if s.candidate {
		s.coordinatorPID = os.Getppid()
	}
	control := c.take(e.Files.Control)
	if control == nil {
		return fmt.Errorf("proxy continuation control missing")
	}
	defer control.Close()
	boot := e.BootID
	if !s.candidate {
		boot = e.CandidateBootID
	}
	wire, err := serviceConnection(control, boot, e.Operation.ID)
	if err != nil {
		return err
	}
	if s.candidate {
		s.parent = wire
	} else {
		if e.CandidatePID <= 1 || e.CandidatePID == os.Getpid() {
			wire.conn.Close()
			return fmt.Errorf("proxy continuation child identity invalid")
		}
		child := &serviceChild{pid: e.CandidatePID, boot: boot, wire: wire, resources: &retainedResources{StatsPath: e.CandidateStatsPath, ShmPath: e.CandidateShmPath, StatsIdentity: e.CandidateStatsIdentity, ShmIdentity: e.CandidateShmIdentity, Metrics: c.take(e.Files.CandidateMetrics), MetricsExpression: e.CandidateMetricsExpression}, status: InstanceStatus{PID: e.CandidatePID, BootID: boot, Phase: "starting"}, done: make(chan struct{}), changed: make(chan struct{}), detached: make(chan struct{})}
		s.child = child
		// Restore supervision before validating or preparing business resources.
		s.watchChild(child)
		if err := wire.send(serviceMessage{Kind: "resume"}); err != nil {
			child.mu.Lock()
			child.err = err
			child.mu.Unlock()
		}
	}
	recordFile := c.take(e.Files.Stop)
	if recordFile == nil {
		return fmt.Errorf("proxy continuation stop record missing")
	}
	record, err := receiveStopRecord(recordFile)
	if err != nil {
		recordFile.Close()
		return err
	}
	if err := s.installStopRecordLocked(record); err != nil {
		recordFile.Close()
		return err
	}
	s.resources = &retainedResources{Data: c.take(e.Files.Data), MMDS: c.take(e.Files.MMDS), Stats: c.take(e.Files.Stats), Shm: c.take(e.Files.Shm), Metrics: c.take(e.Files.Metrics), DataExpression: e.DataExpression, MMDSExpression: e.MMDSExpression, StatsPath: e.StatsPath, ShmPath: e.ShmPath, StatsIdentity: e.StatsIdentity, ShmIdentity: e.ShmIdentity, MetricsExpression: e.MetricsExpression}
	target := c.take(e.Files.Target)
	if target == nil {
		return fmt.Errorf("proxy continuation target missing")
	}
	s.target = &restartTarget{file: target, path: e.Input.Executable, identity: e.TargetIdentity, digest: e.TargetDigest}
	if err := componentexec.VerifyCurrentExecutable(e.TargetIdentity); err != nil {
		return err
	}
	stat, err := target.Stat()
	if err != nil {
		return err
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, io.NewSectionReader(target, 0, stat.Size())); err != nil {
		return err
	}
	if hex.EncodeToString(hash.Sum(nil)) != e.TargetDigest {
		return fmt.Errorf("%w: target contents changed after acceptance", ErrIncompatible)
	}
	if _, err := listenresource.TCP(s.resources.Data); err != nil {
		return err
	}
	if s.resources.MMDS != nil {
		if _, err := listenresource.TCP(s.resources.MMDS); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) runCandidate(ctx context.Context) error {
	defer s.parent.conn.Close()
	deadline, requested, err := s.stopRecord.read()
	if err != nil {
		return err
	}
	if requested {
		s.mu.Lock()
		s.stopRequested = true
		s.stopDeadline = deadline
		s.mu.Unlock()
	}
	boot := s.parent.boot
	messages := make(chan serviceMessage, 1)
	connectionError := make(chan error, 1)
	readerCtx, cancelReader := context.WithCancel(context.Background())
	s.configReplies = make(chan serviceMessage, 1)
	readerDone := make(chan struct{})
	defer func() {
		cancelReader()
		s.parent.conn.Close()
		<-readerDone
		for {
			select {
			case reply := <-s.configReplies:
				closeFile(reply.file)
			default:
				return
			}
		}
	}()
	go func() {
		defer close(readerDone)
		for {
			message, err := s.parent.read()
			if err != nil {
				s.mu.Lock()
				if s.localDone == nil {
					s.stopUnstartedLocalLocked()
				}
				s.mu.Unlock()
				connectionError <- err
				return
			}
			if message.Kind == "config_connected" {
				select {
				case s.configReplies <- message:
				default:
					closeFile(message.file)
				}
				continue
			}
			// Read-only service snapshots need no business-loop action. Consume
			// them here so they cannot delay a stop while Configure is running.
			if message.Kind == "service" && message.Service != nil {
				s.mu.Lock()
				s.knownService = message.Service
				s.mu.Unlock()
				continue
			}
			if message.Kind == "drain" {
				s.mu.Lock()
				if s.candidateDeadline.IsZero() || message.Deadline.Before(s.candidateDeadline) {
					s.candidateDeadline = message.Deadline
				}
				if s.localDone == nil {
					s.stopUnstartedLocalLocked()
				}
				s.mu.Unlock()
			}
			select {
			case messages <- message:
			case <-readerCtx.Done():
				return
			}
		}
	}()
	s.mu.Lock()
	earlyStop := s.stopRequested
	s.mu.Unlock()
	var preparationErr error
	if earlyStop {
		// B has no business to drain, but it still forwards a whole-service
		// request and remains under A's control until A acknowledges by drain.
		if err := s.requestShutdown(); err != nil {
			return err
		}
	} else {
		preparationErr = s.startLocal(ctx, boot, "proxy-"+boot, s.resources)
		if preparationErr != nil {
			if !s.stopping() {
				return s.finishCandidateError(preparationErr, messages, connectionError)
			}
			if errors.Is(preparationErr, ErrStopped) || errors.Is(preparationErr, context.Canceled) {
				preparationErr = nil
			} else {
				return s.finishCandidateError(preparationErr, messages, connectionError)
			}
		}
	}
	// local may contain a failed snapshot with no runner. Only a published
	// completion channel denotes business which must actually be joined.
	hasBusiness := s.localDone != nil
	paused := false
	sendSnapshot := func() error {
		if !hasBusiness {
			status := InstanceStatus{PID: os.Getpid(), BootID: boot, Phase: "stopping", Data: "stopped", MMDS: "stopped"}
			return s.parent.send(serviceMessage{Kind: "snapshot", Status: &status, CanShutdown: true})
		}
		status := s.local.Snapshot()
		compatible := s.local.extensionCompatible()
		if s.localExtension != nil {
			_, ok := s.localExtension.(proxyextension.Shutdown)
			compatible = compatible && ok
		}
		return s.parent.send(serviceMessage{Kind: "snapshot", Status: &status, CanShutdown: compatible})
	}
	ctxDone := ctx.Done()
	for {
		var changed <-chan struct{}
		if hasBusiness {
			s.local.mu.Lock()
			changed = s.local.changed
			s.local.mu.Unlock()
		}
		if paused {
			changed = nil
		} else if err := sendSnapshot(); err != nil {
			if !hasBusiness {
				return errors.Join(preparationErr, err)
			}
			s.local.requestStop(time.Now().Add(s.timeout))
		}
		select {
		case <-changed:
		case err := <-s.localDone:
			_ = sendSnapshot()
			if err != nil {
				return s.finishCandidateError(err, messages, connectionError)
			}
			return err
		case err := <-connectionError:
			if !hasBusiness {
				return errors.Join(preparationErr, err)
			}
			s.local.requestStop(time.Now().Add(s.timeout))
			return errors.Join(err, <-s.localDone)
		case <-ctxDone:
			ctxDone = nil
			_ = s.requestAutomaticShutdown()
		case message := <-messages:
			switch message.Kind {
			case "drain":
				if !hasBusiness {
					return preparationErr
				}
				s.local.requestStop(message.Deadline)
			case "detach":
				paused = true
				if err := s.parent.send(serviceMessage{Kind: "detached"}); err != nil {
					if !hasBusiness {
						return errors.Join(preparationErr, err)
					}
					s.local.requestStop(time.Now().Add(s.timeout))
				}
			case "resume":
				paused = false
				if err := sendSnapshot(); err != nil {
					if !hasBusiness {
						return errors.Join(preparationErr, err)
					}
					s.local.requestStop(time.Now().Add(s.timeout))
				}
			case "service": // Coordinator updates preserve whole-service /status on B.
				if message.Service != nil {
					snapshot := *message.Service
					s.mu.Lock()
					s.knownService = &snapshot
					s.mu.Unlock()
				}
			default:
				if !hasBusiness {
					return errors.Join(preparationErr, fmt.Errorf("unexpected service message %q", message.Kind))
				}
				s.local.requestStop(time.Now().Add(s.timeout))
			}
		}
	}
}

// All business has ended or failed to start before this call. Closing a Unix
// packet socket with unread inbound packets discards even its queued outbound
// error. Keep consuming control until A confirms it recorded the terminal error.
// This shares one bounded control budget and never extends an existing drain.
func (s *Service) finishCandidateError(cause error, messages <-chan serviceMessage, connectionError <-chan error) error {
	deadline := time.Now().Add(2 * time.Second)
	if drain := s.drainDeadline(); !drain.IsZero() && drain.Before(deadline) {
		deadline = drain
	}
	s.mu.Lock()
	local := s.local
	if !s.candidateDeadline.IsZero() && s.candidateDeadline.Before(deadline) {
		deadline = s.candidateDeadline
	}
	s.mu.Unlock()
	if local != nil {
		local.mu.Lock()
		if !local.deadline.IsZero() && local.deadline.Before(deadline) {
			deadline = local.deadline
		}
		local.mu.Unlock()
	}
	if err := s.parent.send(serviceMessage{Kind: "error", Error: cause.Error(), Forced: errors.Is(cause, ErrForcedClose), Deadline: deadline}); err != nil {
		return errors.Join(cause, err)
	}
	timer := time.NewTimer(max(time.Until(deadline), 0))
	defer timer.Stop()
	for {
		select {
		case message := <-messages:
			if message.Kind == "error_ack" && message.Error == cause.Error() {
				return cause
			}
			if message.Kind == "drain" && !message.Deadline.IsZero() && message.Deadline.Before(deadline) {
				deadline = message.Deadline
				timer.Reset(max(time.Until(deadline), 0))
			}
			// Drain/detach cannot restart ended business. Consuming
			// them also prevents the single reader from blocking ahead of ACK.
		case err := <-connectionError:
			return errors.Join(cause, err)
		case <-timer.C:
			return errors.Join(cause, fmt.Errorf("candidate terminal error acknowledgement timed out"))
		}
	}
}

func (s *Service) execTarget() error {
	s.mu.Lock()
	child, r, target, record := s.child, s.resources, s.target, s.stopRecord
	s.mu.Unlock()
	if child == nil {
		return fmt.Errorf("proxy exec requires a supervised replacement")
	}
	if s.stopping() {
		return ErrStopped
	}
	child.detached = make(chan struct{})
	if err := child.wire.send(serviceMessage{Kind: "detach"}); err != nil {
		return err
	}
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-child.detached:
		<-child.readerCompletion()
	case <-child.done:
		return errors.Join(ErrNotReady, child.failed())
	case <-s.stop:
		return ErrStopped
	case <-timer.C:
		return fmt.Errorf("proxy candidate detach timed out")
	}
	// From here B sends no notifications until the new image restores control.
	defer func() { s.readChild(child); _ = child.wire.send(serviceMessage{Kind: "resume"}) }()
	envelope := s.envelope("resume", newBootID(), routesync.ProxyPluginID, r)
	envelope.CandidatePID = child.pid
	envelope.CandidateBootID = child.boot
	envelope.CandidateStatsPath = child.resources.StatsPath
	envelope.CandidateShmPath = child.resources.ShmPath
	envelope.CandidateStatsIdentity = child.resources.StatsIdentity
	envelope.CandidateShmIdentity = child.resources.ShmIdentity
	envelope.CandidateMetricsExpression = child.resources.MetricsExpression
	control, err := listenresource.Dup(child.wire.conn, "proxy-service-control")
	if err != nil {
		return err
	}
	defer control.Close()
	// Keep current descriptor numbers and toggle CLOEXEC on private copies only;
	// no bootstrap, log descriptor or other runtime-owned file is overwritten.
	var inherited []*os.File
	defer func() { closeFiles(inherited) }()
	var duplicateErr error
	add := func(file *os.File) int {
		if file == nil {
			return -1
		}
		copy, err := listenresource.Dup(file, file.Name())
		if err != nil {
			duplicateErr = errors.Join(duplicateErr, err)
			return -1
		}
		inherited = append(inherited, copy)
		return int(copy.Fd())
	}
	envelope.Files = continuationFiles{Data: add(r.Data), MMDS: add(r.MMDS), Stats: add(r.Stats), Shm: add(r.Shm), Metrics: add(r.Metrics), Stop: add(record.file), Target: add(target.file), Control: add(control), CandidateMetrics: add(child.resources.Metrics)}
	if duplicateErr != nil {
		return duplicateErr
	}
	bootstrap, err := encodeContinuation(envelope)
	if err != nil {
		return err
	}
	inherited = append(inherited, bootstrap)
	for _, file := range inherited {
		if _, err := unix.FcntlInt(file.Fd(), unix.F_SETFD, 0); err != nil {
			return err
		}
	}
	s.actionMu.Lock()
	defer s.actionMu.Unlock()
	_, requested, err := record.read()
	if err != nil {
		return err
	}
	if requested {
		return ErrStopped
	}
	releaseSignal, err := holdRestartSignal(s.input.Signals && s.baseline.config.InplaceRestart)
	if err != nil {
		return err
	}
	defer releaseSignal()
	err = unix.Exec("/proc/self/fd/"+strconv.Itoa(envelope.Files.Target), []string{s.input.Executable}, append(cleanContinuationEnvironment(), continuationEnvironment+"="+strconv.Itoa(int(bootstrap.Fd()))))
	// exec errors keep the old coordinator responsible for B and for retrying.
	return err
}
