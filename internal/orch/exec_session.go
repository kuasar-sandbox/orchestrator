package orch

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// ExecSession synchronously authenticates and, when requested, imports the
// target before minting a node-signed exec capability. A paused target is
// resumed asynchronously only after token preparation succeeds.
func (o *Orchestrator) ExecSession(
	ctx context.Context,
	id, apiKey, migrationToken string,
	ttlSeconds int64,
) (string, error) {
	sb, err := o.prepareStandaloneTarget(ctx, id, apiKey, migrationToken)
	if err != nil {
		return "", err
	}
	token, err := mintExecSessionToken(sb, ttlSeconds, time.Now().Unix())
	if err != nil {
		return "", err
	}
	if sb.State == types.StatePaused {
		o.scheduleResume(sb.ID)
	}
	return token, nil
}

func mintExecSessionToken(sb *types.Sandbox, ttlSeconds, nowUnix int64) (string, error) {
	if sb == nil {
		return "", fmt.Errorf("exec session: sandbox is required")
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

func execSessionExpiry(nowUnix, ttlSeconds int64) (int64, error) {
	if ttlSeconds < 0 || nowUnix <= 0 || ttlSeconds > math.MaxInt64-nowUnix {
		return 0, fmt.Errorf("exec session: invalid ttlSeconds: %w", api.ErrBadRequest)
	}
	if ttlSeconds == 0 {
		return 0, nil
	}
	return nowUnix + ttlSeconds, nil
}
