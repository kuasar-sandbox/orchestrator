package orch

import (
	"context"
	"errors"

	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
)

// Only this pre-admission error has nonexecution meaning for this delivery.
// Generic conflicts, missing objects and transport errors do not carry it.
type groupAdmissionConflict struct{ error }

func (e *groupAdmissionConflict) Unwrap() error { return e.error }

func (o *Orchestrator) admitClusterGroup(ctx context.Context, cmd *routesync.Command, group string) (nodegroup.Detail, store.KeyPair, error) {
	detail, pair, err := o.st.GroupAdmit(ctx, cmd.GroupAPIKey, group, cmd.ExpectedGroupVersion)
	if errors.Is(err, nodegroup.ErrConflict) {
		return detail, pair, &groupAdmissionConflict{err}
	}
	if err != nil {
		return detail, pair, err
	}
	fingerprint, err := store.APISecretHash(pair.APISecret)
	if err != nil {
		return detail, pair, err
	}
	if fingerprint != cmd.APISecretFingerprint {
		return detail, store.KeyPair{}, nodegroup.ErrForbidden
	}
	return detail, pair, nil
}
