package orch

import (
	"context"
	"fmt"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
)

func (o *Orchestrator) PutMMDSRouteSecretValue(ctx context.Context, sandboxID, name string, value []byte) error {
	return o.mutateMMDSRouteSecretValue(ctx, sandboxID, name, value, false)
}

func (o *Orchestrator) DeleteMMDSRouteSecretValue(ctx context.Context, sandboxID, name string) error {
	return o.mutateMMDSRouteSecretValue(ctx, sandboxID, name, nil, true)
}

func (o *Orchestrator) mutateMMDSRouteSecretValue(ctx context.Context, sandboxID, name string, value []byte, deleteValue bool) error {
	unlock, err := o.lockLifecycleMutation(ctx, sandboxID)
	if err != nil {
		return err
	}
	defer unlock()

	sb, err := o.st.Get(ctx, sandboxID)
	if err != nil {
		return err
	}
	if sb == nil {
		return fmt.Errorf("sandbox %s: %w", sandboxID, api.ErrNotFound)
	}
	raw, present := sb.Metadata[sandboxcfg.NsMMDS]
	if !present {
		return fmt.Errorf("secret name %q is not declared: %w", name, api.ErrBadRequest)
	}
	routes, err := sandboxcfg.DecodePersistedMMDSRoutes(raw)
	if err != nil {
		return fmt.Errorf("decode sandbox MMDS routes: %w", err)
	}
	if !sandboxcfg.MMDSReferencesSecret(routes, name) {
		return fmt.Errorf("secret name %q is not declared: %w", name, api.ErrBadRequest)
	}
	if !deleteValue && len(value) > o.cfg.MMDS.Routes.MaxSecretValueBytes {
		return fmt.Errorf("secret value exceeds %d bytes: %w", o.cfg.MMDS.Routes.MaxSecretValueBytes, api.ErrBadRequest)
	}
	digest := sandboxcfg.MMDSRoutesDigest(raw)
	current, _, _, err := o.st.GetMMDSRouteSecretValues(ctx, store.MMDSRouteSecretOwnerSandbox, sandboxID, digest)
	if err != nil {
		return err
	}
	proposed := make(store.MMDSRouteSecretValues, len(current)+1)
	for currentName, currentValue := range current {
		proposed[currentName] = append([]byte(nil), currentValue...)
	}
	if deleteValue {
		delete(proposed, name)
	} else {
		proposed[name] = append([]byte(nil), value...)
	}
	if err := validateMMDSRouteEntry(sb, proposed); err != nil {
		return fmt.Errorf("secret value set cannot be synchronized: %w: %v", api.ErrBadRequest, err)
	}
	if deleteValue {
		_, err = o.st.DeleteMMDSRouteSecretValue(ctx, store.MMDSRouteSecretOwnerSandbox, sandboxID, digest, name)
	} else {
		_, err = o.st.PutMMDSRouteSecretValue(ctx, store.MMDSRouteSecretOwnerSandbox, sandboxID, digest, name, value)
	}
	if err != nil {
		return err
	}
	// The store commit is complete before publishing; no subscriber can observe
	// a success that did not durably happen.
	o.cache(sb)
	o.publishUpsert(sb)
	return nil
}
