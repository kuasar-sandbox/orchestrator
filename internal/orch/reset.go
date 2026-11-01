package orch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type sandboxResetWork struct {
	done   chan struct{}
	err    error
	manual atomic.Bool
}

// Reset returns success only after E-only paused is durable. Request cancellation
// stops waiting, not an accepted operation's responsibility. Every retry joins
// the same persisted ID until success or a separately committed explicit failure.
func (o *Orchestrator) Reset(ctx context.Context, id, apiKey string) error {
	// Admission does not need the disk grant: a manual request must be able to
	// join a preparing automatic operation while that operation owns the source.
	unlock := o.lifecycle.Lock(id)
	sb, err := o.st.Get(ctx, id)
	if err != nil {
		unlock()
		return err
	}
	if !ownsSandbox(sb, apiKey) {
		unlock()
		return api.ErrNotFound
	}
	if sb.State == types.StatePaused && sb.ResumeSource.Kind == types.ResumeSourceSandbox {
		unlock()
		return nil
	}
	if sb.State != types.StateDead {
		unlock()
		return api.ErrSandboxChanged
	}
	changed, err := o.st.BeginSandboxReset(ctx, sb)
	if err != nil || !changed {
		unlock()
		if err != nil {
			return err
		}
		return api.ErrSandboxChanged
	}
	sb, err = o.st.Get(ctx, id)
	if err != nil {
		unlock()
		return err
	}
	work := o.startSandboxReset(sb, true)
	unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-work.done:
		return work.err
	}
}

func (o *Orchestrator) startSandboxReset(sb *types.Sandbox, manual bool) *sandboxResetWork {
	work := &sandboxResetWork{done: make(chan struct{})}
	if sb == nil || sb.State != types.StateDead || sb.ResetState != "pending" || sb.ResetID == "" {
		work.err = api.ErrSandboxChanged
		close(work.done)
		return work
	}
	key := sb.InstanceID + "/" + sb.ResetID
	o.resetMu.Lock()
	if old := o.resetActive[key]; old != nil {
		if manual {
			old.manual.Store(true)
		}
		o.resetMu.Unlock()
		return old
	}
	work.manual.Store(manual)
	life := o.launchContext()
	finish, err := o.acceptedOps.Begin(life)
	if err != nil {
		o.resetMu.Unlock()
		work.err = err
		close(work.done)
		return work
	}
	if o.resetActive == nil {
		o.resetActive = make(map[string]*sandboxResetWork)
	}
	o.resetActive[key] = work
	o.resetMu.Unlock()
	expected := cloneSandbox(sb)
	go func() {
		defer finish()
		defer func() {
			o.resetMu.Lock()
			delete(o.resetActive, key)
			close(work.done)
			o.resetMu.Unlock()
		}()
		delay := launchCleanupRetryMin
		for {
			if life.Err() != nil {
				work.err = life.Err()
				return
			}
			retry, err := o.completeSandboxResetOnce(life, expected, work.manual.Load)
			if !retry {
				work.err = err
				return
			}
			o.log.Warn("Reset remains pending", "sid", sb.ID, "operation", sb.ResetID, "err", err)
			if !waitSandboxCleanupRetry(life, delay) {
				work.err = life.Err()
				return
			}
			delay = nextLaunchCleanupRetry(delay)
		}
	}()
	return work
}

func (o *Orchestrator) resetTime() time.Time {
	if o.resetNow != nil {
		return o.resetNow()
	}
	return time.Now()
}

func (o *Orchestrator) completeSandboxResetOnce(ctx context.Context, expected *types.Sandbox, manual func() bool) (bool, error) {
	unlock := o.lifecycle.Lock(expected.ID)
	locked := true
	defer func() {
		if locked {
			unlock()
		}
	}()
	sb, err := o.st.Get(ctx, expected.ID)
	if err != nil {
		return true, err
	}
	if sb == nil || sb.InstanceID != expected.InstanceID {
		return false, api.ErrSandboxChanged
	}
	if sb.ResetState == "succeeded" && sb.ResetID == expected.ResetID {
		if sb.State == types.StateDeleting {
			o.uncache(sb.ID)
		} else {
			o.cache(sb)
		}
		o.publishUpsert(sb)
		o.observeSandboxUpsert(sb)
		return false, nil
	}
	if sb.State != types.StateDead || sb.DeadToken != expected.DeadToken || sb.ResetID != expected.ResetID || sb.ResetState != "pending" {
		return false, api.ErrSandboxChanged
	}
	// Manual admission sets this flag under the same lifecycle fence. Read it
	// here, after waiting for that fence, so an admitted join cannot be lost.
	if !manual() && sb.ResetAutomatic && sb.DeadUnix > 0 && !o.resetTime().Before(time.Unix(sb.DeadUnix, 0).Add(o.cfg.Sandbox.DeadTTLDur())) {
		// Expired terminal data belongs to the deleting finalizer, not a late
		// automatic preparation. Acceptance uses the exact current incarnation.
		_, err := o.acceptSandboxDeleteLocked(ctx, sb)
		if err != nil {
			return true, err
		}
		return false, api.ErrSandboxChanged
	}
	ctx, release, err := o.trySourceOperation(ctx, sb)
	if err != nil {
		return true, err
	}
	defer release()
	// Persisted pending state prohibits Resume; the inherited source grant
	// fences Delete/export and survives a conductor crash. Let request admission
	// observe pending while the SDK does potentially long storage work.
	unlock()
	locked = false
	prepare := o.prepareReset
	if prepare == nil {
		prepare = o.prepareCurrentDisks
	}
	var source types.ResumeSource
	if sb.BaseDir == "" || sb.DeadRunID == "" {
		err = errors.New("current disks unavailable for this terminated run")
	} else if err = o.validateSandboxCleanupPaths(sb); err == nil {
		source, err = prepare(ctx, sb)
	}
	unlock = o.lifecycle.Lock(expected.ID)
	locked = true
	if err != nil {
		var exited *exec.ExitError
		// sandbox-ctl reset preserves handled SIGTERM/SIGINT and lost result
		// delivery as EX_TEMPFAIL (75); signal.NotifyContext otherwise exits 1.
		if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &exited) && (exited.ExitCode() < 0 || exited.ExitCode() == 75)) {
			return true, err // no durable outcome; reopen the same operation
		}
		changed, commitErr := o.st.FailSandboxReset(ctx, sb, err.Error())
		if commitErr != nil {
			return true, commitErr
		}
		if !changed {
			return false, api.ErrSandboxChanged
		}
		if updated, getErr := o.st.Get(ctx, sb.ID); getErr == nil && updated != nil {
			o.publishUpsert(updated)
			o.observeSandboxUpsert(updated)
		}
		return false, fmt.Errorf("%w: %v", api.ErrResetFailed, err)
	}
	changed, err := o.st.CommitSandboxReset(ctx, sb, source)
	if err != nil {
		return true, err
	}
	if !changed {
		return false, api.ErrSandboxChanged
	}
	updated, err := o.st.Get(ctx, sb.ID)
	if err != nil {
		return true, err
	}
	o.cache(updated)
	o.publishUpsert(updated)
	o.observeSandboxUpsert(updated)
	return false, nil
}

// The configured SDK-backed CLI owns disk algorithms and tenant storage access.
// Its inherited source lock remains live if conductor exits while the child is
// preparing, so a new process cannot launch, export or delete its source early.
func (o *Orchestrator) prepareCurrentDisks(ctx context.Context, sb *types.Sandbox) (types.ResumeSource, error) {
	args := []string{"reset", "--checkpoint-dir", filepath.Join(sb.BaseDir, "checkpoint"), "--sandbox-id", sb.ID,
		"--instance-id", sb.InstanceID, "--run-id", sb.DeadRunID, "--operation-id", sb.ResetID, "--json", "--manifest-config", o.cfg.ManifestConfig}
	cmd := exec.CommandContext(ctx, o.executables.SandboxCtl(), args...)
	inheritSourceOperation(ctx, cmd)
	cmd.Env = append(os.Environ(), "MANIFEST_KEY="+sb.ManifestKey)
	var out, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &stderr
	if err := cmd.Run(); err != nil {
		return types.ResumeSource{}, fmt.Errorf("prepare current disks: %w: %s", err, stderr.String())
	}
	var result struct {
		SandboxRef string `json:"sandbox_ref"`
	}
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		return types.ResumeSource{}, err
	}
	source := types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: result.SandboxRef}
	if !source.Valid() {
		return types.ResumeSource{}, errors.New("invalid Reset E result")
	}
	if types.IsPortableRef(source.Ref) {
		return types.ResumeSource{}, errors.New("Reset must return a parent-owned local E")
	}
	if _, err := o.ownedLocalArtifactDir(sb, source); err != nil {
		return types.ResumeSource{}, err
	}
	return source, nil
}
