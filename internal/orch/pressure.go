package orch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
	"golang.org/x/sys/unix"
)

type nodePressureController struct {
	state     *nodectl.State
	admission *nodectl.AdmissionController
	policy    nodectl.PressurePolicy
	resultMu  sync.Mutex
	recent    []nodectl.PressureOperationResult
	blocked   string
}

// ConfigureMemoryPressure runs before the resource socket serves or API/run
// pools start. Durable rows own Q; the controller owns numbers and zone only.
func (o *Orchestrator) ConfigureMemoryPressure(ctx context.Context, state *nodectl.State, admission *nodectl.AdmissionController, policy nodectl.PressurePolicy) error {
	record, err := o.st.LoadNodePressure(ctx)
	if err != nil {
		return err
	}
	if err := state.ConfigurePressure(policy, nodectl.PressureRecord{Zone: nodectl.Zone(record.Zone), Version: record.Version, Reason: record.Reason, SinceUnix: record.SinceUnix}); err != nil {
		return err
	}
	o.memoryPressure.Store(&nodePressureController{state: state, admission: admission, policy: policy})
	var rows []*types.Sandbox
	if err := o.st.RangeSandboxes(ctx, func(sb *types.Sandbox) error { rows = append(rows, sb); return nil }); err != nil {
		return err
	}
	for _, row := range rows {
		if err := func() error {
			unlock := o.lifecycle.Lock(row.ID)
			defer unlock()
			sb, err := o.st.Get(ctx, row.ID)
			if err != nil {
				return err
			}
			if sb == nil {
				return nil
			}

			if sb.ResourceObligation && (sb.PauseReason != types.PauseReasonResource || ((sb.State == types.StatePaused || sb.State == types.StateStarting) && (sb.ResumeSource.Kind != types.ResumeSourceSnapshot || !sb.ResumeSource.Valid()))) {
				return fmt.Errorf("sandbox %s has inconsistent pressure obligation", sb.ID)
			}
			if sb.State == types.StateRunning && sb.RunningSinceUnixNano == 0 {
				if err := o.st.InitializeRunningInterval(ctx, sb.ID, sb.RunID); err != nil {
					return err
				}
				sb, err = o.st.Get(ctx, sb.ID)
				if err != nil {
					return err
				}
				o.cache(sb)
			}
			// A surviving running VM with an interrupted pre-capture intent is still
			// running. Cancel only that exact intent, never a committed paused source.
			if sb.State == types.StateRunning && sb.ResourceObligation {
				if _, err := o.st.CancelResourcePause(ctx, sb); err != nil {
					return err
				}
				sb, err = o.st.Get(ctx, sb.ID)
				if err != nil {
					return err
				}
				o.cache(sb)
			}
			o.observePressureSandbox(sb)
			return nil
		}(); err != nil {
			return err
		}
	}

	admission.LookupLaunch = o.ResourceLaunchAdmission
	return nil
}

func (o *Orchestrator) PersistMemoryPressure(ctx context.Context) error {
	if o.memoryPressure.Load() == nil {
		return nil
	}
	p := o.memoryPressure.Load().state.PressureSnapshot()
	return o.st.SaveNodePressure(ctx, store.NodePressure{Zone: string(p.Zone), Version: p.Version, Reason: p.Reason, SinceUnix: p.SinceUnix})
}

func launchAdmission(sb *types.Sandbox) nodectl.LaunchAdmission {
	a := nodectl.LaunchAdmission{Identity: sb.RunID + ":" + strconv.FormatUint(sb.PressureVersion, 10)}
	if sb.ResumeSource.Valid() {
		a.Operation = nodectl.OperationResume
		a.SavedSource = sb.LaunchMode == types.LaunchMemory && sb.ResumeSource.Kind == types.ResumeSourceSnapshot
		if sb.ResourceObligation && sb.PauseReason == types.PauseReasonResource && a.SavedSource {
			a.Operation = nodectl.OperationRecovery
		}
	}
	return a
}

func (o *Orchestrator) checkResourceLaunch(sb *types.Sandbox) error {
	if o.memoryPressure.Load() == nil {
		return nil
	}
	if o.memoryPressure.Load().admission.IsDrained() {
		return api.ErrResourceUnavailable
	}
	if err := o.memoryPressure.Load().state.CheckOperation(launchAdmission(sb).Operation); err != nil {
		return fmt.Errorf("%w: %v", api.ErrResourceUnavailable, err)
	}
	return nil
}

// ResourceLaunchAdmission is called only after SO_PEERCRED has been captured.
// The current sandbox pidfile and its live POSIX lock bind the authenticated
// peer to this launch. No resource RPC is allowed to choose its operation or exemption.
func (o *Orchestrator) ResourceLaunchAdmission(sid string, peerPID int) (nodectl.LaunchAdmission, error) {
	sb, err := o.st.Get(context.Background(), sid)
	if err != nil {
		return nodectl.LaunchAdmission{}, err
	}
	if sb == nil {
		// Build VM phases have no Sandbox Pause lifecycle and remain ordinary
		// creates. Their existing resource/lease authentication is unchanged.
		return nodectl.LaunchAdmission{}, nil
	}
	if peerPID <= 0 || sb.RunID == "" || (sb.State != types.StateStarting && sb.State != types.StateRunning) {
		return nodectl.LaunchAdmission{}, errors.New("resource launch is not owned by a current runner")
	}
	fd, err := unix.Open(sb.PidFile(), unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nodectl.LaunchAdmission{}, errors.New("resource launch pid identity unavailable")
	}
	f := os.NewFile(uintptr(fd), "resource-launch-identity")
	defer f.Close()
	owner, locked, err := resource.LockOwnerFD(f.Fd())
	if err != nil || !locked || owner != peerPID {
		return nodectl.LaunchAdmission{}, errors.New("resource launch peer does not own the live runner")
	}
	body, err := io.ReadAll(io.LimitReader(f, 64))
	if err != nil {
		return nodectl.LaunchAdmission{}, errors.New("resource launch pid identity unavailable")
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(body)))
	if err != nil || pid != peerPID {
		return nodectl.LaunchAdmission{}, errors.New("resource launch peer identity mismatch")
	}
	a := launchAdmission(sb)
	a.Accepted = true
	return a, nil
}

func (o *Orchestrator) observePressureSandbox(sb *types.Sandbox) {
	if o.memoryPressure.Load() == nil || sb == nil {
		return
	}
	phase := ""
	if sb.ResourceObligation && sb.PauseReason == types.PauseReasonResource {
		switch sb.State {
		case types.StateRunning:
			phase = "capturing"
		case types.StatePaused:
			phase = "paused"
		case types.StateStarting:
			phase = "starting"
		}
	}
	accountCharge := sb.PressureSinceUnixNano != 0 && (sb.State == types.StatePaused || sb.State == types.StateDeleting || sb.State == types.StateDead)
	owner := accountCharge && (sb.RunID != "" || sb.VswitchPort != "" || sb.FloatingIP != "" || sb.InnerIP != "" || sb.PortMAC != "" || sb.RunDir != "" || sb.EnvdUDS != "" || sb.CiUDS != "")
	if sb.PressureVersion == 0 && sb.PressureSinceUnixNano == 0 && !sb.ResourceObligation {
		o.memoryPressure.Load().state.ResetPressureIdentity(sb.ID)
	}
	o.memoryPressure.Load().state.ObserveResourceObligation(sb.ID, sb.PressureVersion, phase, owner, accountCharge)
	o.memoryPressure.Load().admission.PushWake()
}

// A terminal observation follows confirmed physical cleanup and hard deletion.
// The old owner tuple is retained in the callback for observers, but must not
// re-register cleanup. Any resource charge still requires its own proven Release.
func (o *Orchestrator) observeDeletedPressureSandbox(sb *types.Sandbox) {
	if o.memoryPressure.Load() == nil || sb == nil {
		return
	}
	o.memoryPressure.Load().state.ObserveResourceObligation(sb.ID, sb.PressureVersion, "", false, sb.PressureSinceUnixNano != 0)
	o.memoryPressure.Load().state.RetirePressureIdentity(sb.ID)
	o.memoryPressure.Load().admission.PushWake()
}

func (o *Orchestrator) adoptResourcePauseLocked(ctx context.Context, sb *types.Sandbox, request sandboxcfg.CaptureRequest) error {
	if !sb.ResourceObligation || sb.PauseReason != types.PauseReasonResource {
		return api.ErrAlreadyPaused
	}
	if err := validateCaptureRequest(request); err != nil {
		return err
	}
	// Explicit capture actions cannot be certified from the retained source.
	// Default Snapshot adoption reuses it without applying capture defaults.
	if request.Kind != types.CaptureSnapshot || !request.SnapshotPolicy.Empty() || sb.ResumeSource.Kind != types.ResumeSourceSnapshot {
		return fmt.Errorf("%w: requested capture cannot be satisfied by the saved snapshot", api.ErrBadRequest)
	}
	changed, err := o.st.AdoptResourcePause(ctx, sb)
	if err != nil {
		return err
	}
	if !changed {
		return api.ErrSandboxChanged
	}
	adopted := cloneSandbox(sb)
	adopted.ResourceObligation = false
	adopted.PauseReason = types.PauseReasonExplicit
	adopted.PressureVersion++
	o.observePressureSandbox(adopted) // invalidate unused authorization before publication
	o.cache(adopted)
	o.publishUpsert(adopted)
	o.observeSandboxUpsert(adopted)
	o.recordPressureResult(sb.ID, "adopt", nil)
	o.log.Info("resource pause adopted", "sid", sb.ID, "pressure_version", adopted.PressureVersion)
	return nil
}

func (o *Orchestrator) pressureSandboxes() []*types.Sandbox {
	o.mu.Lock()
	defer o.mu.Unlock()
	rows := make([]*types.Sandbox, 0, len(o.reg))
	for _, sb := range o.reg {
		rows = append(rows, sb)
	} // immutable cache snapshots
	return rows
}

func (o *Orchestrator) pauseForPressure(ctx context.Context, candidate *types.Sandbox) error {
	unlock, err := o.lockLifecycleMutation(ctx, candidate.ID)
	if err != nil {
		return err
	}
	defer unlock()
	sb, err := o.st.Get(ctx, candidate.ID)
	if err != nil {
		return err
	}
	if sb == nil || sb.State != types.StateRunning || sb.RunID != candidate.RunID || sb.ResourceObligation {
		return api.ErrSandboxChanged
	}
	if o.memoryPressure.Load().admission.IsDrained() {
		return api.ErrResourceUnavailable
	}
	var fs unix.Statfs_t
	storageErr := unix.Statfs(filepath.Join(sb.BaseDir, "checkpoint"), &fs)
	if errors.Is(storageErr, unix.ENOENT) {
		storageErr = unix.Statfs(sb.BaseDir, &fs)
	}
	if storageErr != nil {
		return fmt.Errorf("checkpoint storage: %w", storageErr)
	}
	if fs.Type == unix.TMPFS_MAGIC || fs.Type == unix.RAMFS_MAGIC {
		return errors.New("resource pause requires disk-backed checkpoint storage")
	}
	request := sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot}
	request.SnapshotPolicy, err = o.resolveSnapshotPolicy(sb.Metadata, sandboxcfg.SnapshotPolicy{})
	if err != nil {
		return err
	}
	if !o.memoryPressure.Load().state.BeginPressurePause(sb.ID, sb.PressureVersion+1) {
		return api.ErrResourceUnavailable
	}
	changed, err := o.st.BeginResourcePause(ctx, sb)
	if err != nil || !changed {
		o.memoryPressure.Load().state.ObserveObligation(sb.ID, sb.PressureVersion+1, "", false)
		if err == nil {
			err = api.ErrSandboxChanged
		}
		return err
	}
	sb, err = o.st.Get(ctx, sb.ID)
	if err != nil {
		return err
	} // persisted obligation remains for restart
	o.cache(sb)
	o.observePressureSandbox(sb)
	if err = o.pauseSandboxLocked(ctx, sb, request); err != nil {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		changed, cancelErr := o.st.CancelResourcePause(cleanupCtx, sb)
		if cancelErr != nil {
			return errors.Join(err, cancelErr)
		}
		if changed {
			current, getErr := o.st.Get(cleanupCtx, sb.ID)
			if getErr != nil {
				return errors.Join(err, getErr)
			}
			o.cache(current)
			o.publishUpsert(current)
			o.observeSandboxUpsert(current)
		}
	}
	return err
}

// RunMemoryPressure has one capture and one recovery in flight. Selection,
// SQLite and capture I/O happen outside the node resource mutex. Each completed
// capture requires new rounds before another; failed candidates back off.
func (o *Orchestrator) RunMemoryPressure(ctx context.Context) {
	p := o.memoryPressure.Load()
	if p == nil {
		return
	}
	ticker := time.NewTicker(p.policy.Interval)
	defer ticker.Stop()
	type result struct {
		sid   string
		pause bool
		err   error
	}
	done := make(chan result, 2)
	pauseBusy, resumeBusy := false, false
	// Pace our own recovery work, not every unrelated reservation update.
	// A busy node may never have a globally quiet interval even with ample
	// free capacity; complete-budget admission remains the resource gate.
	resumeAfter := time.Now().Add(p.policy.Interval)
	backoff := map[string]time.Time{}
	var persisted uint64 = ^uint64(0)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-p.state.PressureWake():
		case r := <-done:
			kind := "resume"
			if r.pause {
				kind = "pause"
			}
			o.recordPressureResult(r.sid, kind, r.err)
			resumeAfter = time.Now().Add(p.policy.Interval)
			if r.pause {
				pauseBusy = false
			} else {
				resumeBusy = false
			}
			if r.err != nil {
				backoff[r.sid] = time.Now().Add(max(p.policy.Interval, 2*time.Second))
				o.log.Warn("node pressure operation blocked", "sid", r.sid, "pause", r.pause, "err", r.err)
			}
		}
		rows := o.pressureSandboxes()
		snapshot := p.state.PressureSnapshot()
		if snapshot.Version != persisted {
			if err := o.PersistMemoryPressure(ctx); err != nil {
				o.log.Error("persist node pressure", "err", err)
				continue
			}
			persisted = snapshot.Version
			o.log.Info("node pressure state", "zone", snapshot.Zone, "raw_zone", snapshot.RawZone, "reason", snapshot.Reason, "pending_recoveries", snapshot.Pending, "version", snapshot.Version)
		}
		p.admission.PushWake()
		now := time.Now()
		for sid, until := range backoff {
			if !now.Before(until) {
				delete(backoff, sid)
			}
		}
		workerBlocked := ""
		if !pauseBusy && snapshot.PauseEligible {
			if sb := pressurePauseCandidate(rows, snapshot, now, backoff, p.policy.MinimumRunTime, p.state.CanPressurePause); sb != nil {
				pauseBusy = true
				go func(sb *types.Sandbox) {
					err := o.pauseForPressure(ctx, sb)
					select {
					case done <- result{sid: sb.ID, pause: true, err: err}:
					case <-ctx.Done():
					}
				}(sb)
			} else {
				workerBlocked = "no_eligible_running_sandbox"
			}
		}
		p.resultMu.Lock()
		p.blocked = workerBlocked
		p.resultMu.Unlock()
		if !resumeBusy && !now.Before(resumeAfter) && !p.admission.IsDrained() && !p.state.HasPendingExecutableMemoryDemand() {
			// Age order gives unattended recoveries eventual service after
			// currently runnable work has consumed the headroom that pressure
			// coordination just created. A real Wake still shares the normal
			// admission/launch claim and is not gated by this coordinator hint.
			sort.Slice(rows, func(i, j int) bool {
				if rows[i].PressureSinceUnixNano == rows[j].PressureSinceUnixNano {
					return rows[i].ID < rows[j].ID
				}
				return rows[i].PressureSinceUnixNano < rows[j].PressureSinceUnixNano
			})
			for _, sb := range rows {
				if sb.State != types.StatePaused || !sb.ResourceObligation || now.Before(backoff[sb.ID]) {
					continue
				}
				if _, charged := p.state.SnapshotSandboxResource(sb.ID); charged {
					continue
				}
				resumeBusy = true
				go func(saved *types.Sandbox) {
					deadline := saved.DeadlineUnix
					_, attempt, err := o.ensureResumeAccepted(ctx, saved.ID, &deadline, types.ResumeRequest{Trigger: types.ResumeTriggerWake, Mode: types.ResumeMemory}, func(current *types.Sandbox) error {
						if !current.ResourceObligation || current.PauseReason != types.PauseReasonResource || current.PressureVersion != saved.PressureVersion || current.ResumeSource != saved.ResumeSource {
							return api.ErrSandboxChanged
						}
						return nil
					})
					if err == nil && attempt != nil {
						err = attempt.wait(ctx)
					}
					select {
					case done <- result{sid: saved.ID, err: err}:
					case <-ctx.Done():
					}
				}(sb)
				break
			}
		}
	}
}

// ResourcePressureStatus projects lifecycle facts and one atomic resource
// snapshot. The diagnostic history is bounded and is never admission authority.
func (o *Orchestrator) ResourcePressureStatus(ctx context.Context) (nodectl.PressureStatus, error) {
	if err := ctx.Err(); err != nil {
		return nodectl.PressureStatus{}, err
	}
	p := o.memoryPressure.Load()
	if p == nil {
		return nodectl.PressureStatus{}, nil
	}
	out := nodectl.PressureStatus{Enabled: true, PressureSnapshot: p.state.PressureSnapshot()}
	for _, sb := range o.pressureSandboxes() {
		out.Sandboxes = append(out.Sandboxes, nodectl.SandboxPressureStatus{ID: sb.ID, State: string(sb.State), PauseReason: sb.PauseReason, RecoveryObligation: sb.ResourceObligation, Version: sb.PressureVersion, RunningSinceUnixNano: sb.RunningSinceUnixNano, WaitingSinceUnixNano: sb.PressureSinceUnixNano})
	}
	sort.Slice(out.Sandboxes, func(i, j int) bool { return out.Sandboxes[i].ID < out.Sandboxes[j].ID })
	p.resultMu.Lock()
	out.Recent = append([]nodectl.PressureOperationResult(nil), p.recent...)
	out.WorkerBlocked = p.blocked
	p.resultMu.Unlock()
	return out, nil
}

func (o *Orchestrator) recordPressureResult(sid, operation string, err error) {
	p := o.memoryPressure.Load()
	if p == nil {
		return
	}
	result := "completed"
	if err != nil {
		result = err.Error()
	}
	p.resultMu.Lock()
	defer p.resultMu.Unlock()
	if len(p.recent) == 64 {
		copy(p.recent, p.recent[1:])
		p.recent = p.recent[:63]
	}
	p.recent = append(p.recent, nodectl.PressureOperationResult{SandboxID: sid, Operation: operation, Result: result, AtUnixNano: time.Now().UnixNano()})
}

// Selection uses only current continuous-running age, authoritative lifecycle
// facts and confirmed reservations. The final fence rechecks eligibility.
func pressurePauseCandidate(rows []*types.Sandbox, snapshot nodectl.PressureSnapshot, now time.Time, backoff map[string]time.Time, minimum time.Duration, eligible func(string) bool) *types.Sandbox {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].RunningSinceUnixNano == rows[j].RunningSinceUnixNano {
			return rows[i].ID < rows[j].ID
		}
		return rows[i].RunningSinceUnixNano < rows[j].RunningSinceUnixNano
	})
	for _, sb := range rows {
		if sb.State != types.StateRunning || sb.ResourceObligation || sb.ID == snapshot.Beneficiary || now.Before(backoff[sb.ID]) || !eligible(sb.ID) {
			continue
		}
		if sb.RunningSinceUnixNano == 0 || now.Sub(time.Unix(0, sb.RunningSinceUnixNano)) < minimum {
			continue
		}
		return sb
	}
	return nil
}
