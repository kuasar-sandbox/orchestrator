package orch

import (
	"context"
	"fmt"
	"time"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/execadmission"
	"github.com/kuasar-sandbox/orchestrator/internal/execsession"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type unixClock func() int64

func wallUnix() int64 { return time.Now().Unix() }

// ExecSession synchronously authenticates and, when requested, imports the
// target. Token preparation runs under the resume-admission lifecycle fence,
// after any previous cleanup owner but before a new paused-to-starting mutation.
func (o *Orchestrator) ExecSession(
	ctx context.Context,
	id, apiKey, migrationToken string,
	ttlSeconds int64,
	conditions []string,
) (string, error) {
	return o.execSession(ctx, id, apiKey, migrationToken, ttlSeconds, conditions, wallUnix)
}

func (o *Orchestrator) execSession(
	ctx context.Context,
	id, apiKey, migrationToken string,
	ttlSeconds int64,
	conditions []string,
	now unixClock,
) (string, error) {
	if _, err := execSessionExpiry(now(), ttlSeconds); err != nil {
		return "", err
	}
	if err := o.preflightExecSessionCaller(ctx, id, apiKey, migrationToken); err != nil {
		return "", err
	}
	compiler, err := execadmission.Default()
	if err != nil {
		return "", fmt.Errorf("exec session: initialize admission: %w", err)
	}
	if _, err := compiler.Compile(conditions); err != nil {
		return "", fmt.Errorf("exec session: invalid conditions: %w", api.ErrBadRequest)
	}
	if _, err := o.prepareStandaloneTarget(ctx, id, apiKey, migrationToken); err != nil {
		return "", err
	}
	var token string
	request := types.ResumeRequest{Trigger: types.ResumeTriggerExecSession, Mode: types.ResumeAuto}
	_, _, err = o.ensureResumeAcceptedPreparedFrom(ctx, id, nil, request, conductorextension.SandboxOriginExec, func(current *types.Sandbox) error {
		if !ownsSandbox(current, apiKey) {
			return api.ErrNotFound
		}
		return nil
	}, func(current *types.Sandbox) error {
		var err error
		// Sample signing time only after synchronous import, lifecycle contention,
		// and any previous launch cleanup have completed. The hook still runs
		// before BeginResume, so token generation failure has no launch side effect.
		token, err = mintExecSessionToken(current, ttlSeconds, conditions, now())
		return err
	})
	if err != nil {
		return "", err
	}
	return token, nil
}

// preflightExecSessionCaller authenticates caller-controlled CEL input without
// importing or resuming a sandbox. Existing targets use their resource-bound
// APISecret; an absent migration target requires an allowlisted credential pair
// before compilation. prepareStandaloneTarget repeats the authoritative checks
// after compilation to close races before any import or lifecycle mutation.
func (o *Orchestrator) preflightExecSessionCaller(
	ctx context.Context,
	id, apiKey, migrationToken string,
) error {
	existing, err := o.st.Get(ctx, id)
	if err != nil {
		return err
	}
	if existing != nil {
		if !ownsSandbox(existing, apiKey) {
			return api.ErrNotFound
		}
		return nil
	}
	if migrationToken == "" {
		return api.ErrNotFound
	}
	pair, err := o.resolveAllowed(ctx, apiKey)
	if err != nil {
		return err
	}
	if pair.APISecret == "" {
		return fmt.Errorf("exec session: credential pair is not installed: %w", api.ErrNotAllowed)
	}
	return nil
}

func mintExecSessionToken(sb *types.Sandbox, ttlSeconds int64, conditions []string, nowUnix int64) (string, error) {
	if err := validateExecSessionSandbox(sb); err != nil {
		return "", err
	}
	expiresUnix, err := execSessionExpiry(nowUnix, ttlSeconds)
	if err != nil {
		return "", err
	}
	token, err := keys.MintExecAccessTokenWithConditions(
		sb.ServiceSecret, sb.StableID(), expiresUnix, conditions,
	)
	if err != nil {
		return "", fmt.Errorf("exec session: mint access token: %w", err)
	}
	return token, nil
}

func validateExecSessionSandbox(sb *types.Sandbox) error {
	if sb == nil {
		return fmt.Errorf("exec session: sandbox is required")
	}
	if sb.State != types.StateStarting && sb.State != types.StateRunning && sb.State != types.StatePaused {
		return fmt.Errorf("exec session: sandbox is unavailable: %w", api.ErrNotFound)
	}
	template, err := types.ParseTemplateID(sb.TemplateID)
	if err != nil || !sb.Profile.Valid() || template.Profile != sb.Profile {
		return fmt.Errorf("exec session: sandbox template and profile are inconsistent")
	}
	return nil
}

func execSessionExpiry(nowUnix, ttlSeconds int64) (int64, error) {
	expiresUnix, err := execsession.ExpiryUnix(nowUnix, ttlSeconds)
	if err != nil {
		return 0, fmt.Errorf("exec session: invalid ttlSeconds: %w", api.ErrBadRequest)
	}
	return expiresUnix, nil
}
