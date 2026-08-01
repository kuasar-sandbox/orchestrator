package orch

import (
	"context"
	"fmt"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

// PutMMDSSecret installs or overwrites one named secret value for sid,
// encrypted at rest. name must already be specified in sid's
// kuasar-sandbox.mmds secrets[] -- an admin-side typo safety net mirroring
// Create-time validation; the value itself is never accepted from the
// tenant's Create request, only through this admin API.
func (o *Orchestrator) PutMMDSSecret(ctx context.Context, sid, name string, body []byte, contentType string, expiresUnix int64) (int64, error) {
	unlock := o.lifecycle.Lock(sid)
	defer unlock()

	sb, err := o.st.Get(ctx, sid)
	if err != nil {
		return 0, err
	}
	if sb == nil {
		return 0, fmt.Errorf("mmds secret: sandbox %s: %w", sid, api.ErrNotFound)
	}
	if max := o.cfg.MMDS.Routes.Secret.MaxValueBytes; max > 0 && len(body) > max {
		return 0, fmt.Errorf("mmds secret: value exceeds %d bytes: %w", max, api.ErrBadRequest)
	}
	if !sandboxcfg.MMDSSpecifiesSecretName(sb.Metadata, name) {
		return 0, fmt.Errorf("mmds secret: %q is not specified for sandbox %s: %w", name, sid, api.ErrBadRequest)
	}
	digest := sandboxcfg.MMDSConfigDigest(sb.Metadata)
	rev, err := o.st.SetMMDSSecret(ctx, sid, digest, name, body, contentType, expiresUnix)
	if err != nil {
		return 0, err
	}
	o.secretWait.Notify(sid, name)
	o.publishUpsert(sb)
	return rev, nil
}

// DeleteMMDSSecret clears one named secret value for sid. Deleting a name
// that was never configured (or was already deleted) is an idempotent
// success; deleting a name that was never specified in secrets[] at all is a
// bad request, same validation as PutMMDSSecret.
func (o *Orchestrator) DeleteMMDSSecret(ctx context.Context, sid, name string) (int64, error) {
	unlock := o.lifecycle.Lock(sid)
	defer unlock()

	sb, err := o.st.Get(ctx, sid)
	if err != nil {
		return 0, err
	}
	if sb == nil {
		return 0, fmt.Errorf("mmds secret: sandbox %s: %w", sid, api.ErrNotFound)
	}
	if !sandboxcfg.MMDSSpecifiesSecretName(sb.Metadata, name) {
		return 0, fmt.Errorf("mmds secret: %q is not specified for sandbox %s: %w", name, sid, api.ErrBadRequest)
	}
	digest := sandboxcfg.MMDSConfigDigest(sb.Metadata)
	rev, err := o.st.ClearMMDSSecret(ctx, sid, digest, name)
	if err != nil {
		return 0, err
	}
	o.secretWait.Notify(sid, name)
	o.publishUpsert(sb)
	return rev, nil
}
