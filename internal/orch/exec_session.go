package orch

import (
	"context"
	"fmt"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
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
) (string, error) {
	return o.execSession(ctx, id, apiKey, migrationToken, ttlSeconds, wallUnix)
}

func (o *Orchestrator) execSession(
	ctx context.Context,
	id, apiKey, migrationToken string,
	ttlSeconds int64,
	now unixClock,
) (string, error) {
	if _, err := execSessionExpiry(now(), ttlSeconds); err != nil {
		return "", err
	}
	if _, err := o.prepareStandaloneTarget(ctx, id, apiKey, migrationToken); err != nil {
		return "", err
	}
	var token string
	_, _, err := o.ensureResumeAcceptedPrepared(ctx, id, nil, func(current *types.Sandbox) error {
		if !ownsSandbox(current, apiKey) {
			return api.ErrNotFound
		}
		return nil
	}, func(current *types.Sandbox) error {
		var err error
		// Sample signing time only after synchronous import, lifecycle contention,
		// and any previous launch cleanup have completed. The hook still runs
		// before BeginResume, so token generation failure has no launch side effect.
		token, err = mintExecSessionToken(current, ttlSeconds, now())
		return err
	})
	if err != nil {
		return "", err
	}
	return token, nil
}

func mintExecSessionToken(sb *types.Sandbox, ttlSeconds, nowUnix int64) (string, error) {
	if err := validateExecSessionSandbox(sb); err != nil {
		return "", err
	}
	expiresUnix, err := execSessionExpiry(nowUnix, ttlSeconds)
	if err != nil {
		return "", err
	}
	token, err := keys.MintExecAccessToken(sb.ServiceSecret, sb.AuthSandboxID(), expiresUnix)
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
