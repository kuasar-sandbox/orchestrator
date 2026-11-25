package registry

import (
	"context"
	"encoding/hex"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	requestplacement "github.com/kuasar-sandbox/orchestrator/internal/placement"
)

func (r *Registry) VerifyGroupAPIKey(ctx context.Context, group, key string) (bool, error) {
	parsed, err := apikey.Parse(key)
	if err != nil {
		return false, nil
	}
	prefix := hex.EncodeToString(parsed.CandidateFingerprint)
	for cursor := metadata.Cursor(""); ; {
		page, err := r.stores.groups.ListGroupAuthCandidates(ctx, group, prefix, cursor, metadata.MaxGroupSupportBatchRows)
		if err != nil {
			return false, err
		}
		for _, auth := range page.Items {
			if !strings.HasPrefix(auth.Value.AuthRef, prefix) {
				continue
			}
			raw, err := hex.DecodeString(auth.Value.APISecret)
			valid := err == nil && len(raw) == 32 && apikey.Verify(parsed, raw)
			clear(raw)
			if valid {
				return true, nil
			}
		}
		if page.Next == "" {
			return false, nil
		}
		cursor = page.Next
	}
}

func (r *Registry) authenticateSandboxAPIKey(ctx context.Context, rec *SandboxRecord, key string) error {
	if key == "" {
		return ErrReserveUnauthorized
	}
	if rec == nil {
		return ErrSandboxNotFound
	}
	if rec.APISecret != "" {
		return authenticateRecordAPIKey(rec, key)
	}
	auth, err := r.stores.groups.ReadGroupAuth(ctx, rec.Group, rec.APISecretFingerprint)
	if err != nil {
		return err
	}
	if !auth.Exists {
		return ErrReserveForbidden
	}
	copy := *rec
	copy.APISecret = auth.Value.APISecret
	return authenticateRecordAPIKey(&copy, key)
}

// Every waiter checks its own APIKey and constraints against the final exact
// object. A single-flight result is shared work, never shared authorization.
func (r *Registry) validateCreateResult(ctx context.Context, req SandboxReserveRequest, result *ReserveResult) error {
	if result == nil {
		return ErrSandboxNotFound
	}
	current, _, found, err := r.getSandboxForReserve(ctx, req.Group, req.RouteKey)
	if err != nil {
		return err
	}
	if !found || current.SandboxID != result.Route.SandboxID || current.NodeSandboxID != result.Route.NodeSandboxID || current.NodeID != result.Route.NodeID {
		return ErrReserveConflict
	}
	if err := r.authenticateSandboxAPIKey(ctx, current, req.APIKey); err != nil {
		return err
	}
	return r.validateExistingPlacement(ctx, current, req.Constraints)
}

func (r *Registry) validateExistingPlacement(ctx context.Context, rec *SandboxRecord, constraints requestplacement.Request) error {
	if len(constraints.NodeSelectors) == 0 {
		return nil
	}
	node, found, err := r.stores.GetNodeProfile(ctx, rec.NodeID)
	if err != nil {
		return err
	}
	if !found || !constraints.Matches(node.Labels) {
		return ErrReserveConflict
	}
	return nil
}
