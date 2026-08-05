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
// target before minting a node-signed exec capability. A paused target is
// resumed asynchronously only after token preparation succeeds.
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
	sb, err := o.prepareStandaloneTarget(ctx, id, apiKey, migrationToken)
	if err != nil {
		return "", err
	}
	// Signing time is intentionally sampled after synchronous target import and
	// validation so migration latency never consumes the requested token TTL.
	token, err := mintExecSessionToken(sb, ttlSeconds, now())
	if err != nil {
		return "", err
	}
	if sb.State == types.StatePaused {
		o.scheduleResume(sb.ID)
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
