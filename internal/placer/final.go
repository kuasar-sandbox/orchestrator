package placer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/maglev"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

const (
	FinalPlanPath      = "/internal/placement/plan"
	FinalVerifyKeyPath = "/internal/provider/verify-key"
	FinalKeyLeasePath  = "/internal/provider/key-lease"
)

type VerifyKeyRequest struct {
	Group  string `json:"group"`
	APIKey string `json:"api_key"`
}

type VerifyKeyResponse struct {
	Authorized bool `json:"authorized"`
}

type KeyLeaseRequest struct {
	Group                  string `json:"group"`
	AuthKeyFingerprint     string `json:"auth_key_fingerprint"`
	ManifestKeyFingerprint string `json:"manifest_key_fingerprint"`
}

type KeyLeaseResponse struct {
	Lease *routesync.NodeKeyLeaseV1 `json:"lease,omitempty"`
	Error string                    `json:"error,omitempty"`
}

type PlanKind string

const (
	PlanSandbox PlanKind = "sandbox"
	PlanBuild   PlanKind = "build"
)

type SandboxPlanInput struct {
	SandboxID      string                             `json:"sandbox_id"`
	TemplateRef    string                             `json:"template_ref,omitempty"`
	Config         map[string]string                  `json:"config,omitempty"`
	TimeoutSeconds int                                `json:"timeout_seconds,omitempty"`
	Demand         placement.SandboxDemand            `json:"demand"`
	Request        clusterstate.NodeRequestEnvelopeV1 `json:"request"`
}

type BuildPlanInput struct {
	BuildID    string                             `json:"build_id"`
	TemplateID string                             `json:"template_id"`
	Profile    types.Profile                      `json:"profile"`
	Names      []string                           `json:"names,omitempty"`
	Aliases    []string                           `json:"aliases,omitempty"`
	Metadata   map[string]string                  `json:"metadata,omitempty"`
	CPUCount   int                                `json:"cpu_count"`
	MemoryMB   int                                `json:"memory_mb"`
	Demand     placement.BuildDemand              `json:"demand"`
	Request    clusterstate.NodeRequestEnvelopeV1 `json:"request"`
}

type PlanRequest struct {
	Kind                PlanKind                `json:"kind"`
	Group               string                  `json:"group"`
	RouteKey            string                  `json:"route_key,omitempty"`
	Nodes               []placement.CatalogNode `json:"nodes"`
	ExcludedNodeIDs     []string                `json:"excluded_node_ids,omitempty"`
	TargetRuntimeDigest string                  `json:"target_runtime_digest,omitempty"`
	Sandbox             *SandboxPlanInput       `json:"sandbox,omitempty"`
	Build               *BuildPlanInput         `json:"build,omitempty"`
}

type PlanResponse struct {
	Candidates            []clusterstate.PlacementCandidate `json:"candidates,omitempty"`
	NormalizedDemand      []byte                            `json:"normalized_demand,omitempty"`
	DispatchSpec          []byte                            `json:"dispatch_spec,omitempty"`
	ProviderPolicyVersion string                            `json:"provider_policy_version,omitempty"`
	Error                 string                            `json:"error,omitempty"`
}

type FinalService struct {
	provider clusterstate.SandboxGroupProvider
	config   clustercfg.PlacementConfig
}

func NewFinalService(provider clusterstate.SandboxGroupProvider, config clustercfg.PlacementConfig) (*FinalService, error) {
	if provider == nil {
		return nil, errors.New("placer: final service requires a group Provider")
	}
	if config.Candidates == 0 {
		config.Candidates = placement.DefaultCandidateCount
	}
	if config.Candidates != placement.DefaultCandidateCount {
		return nil, fmt.Errorf("placer: initial generation requires exactly %d candidates", placement.DefaultCandidateCount)
	}
	return &FinalService{provider: provider, config: config}, nil
}

func (s *FinalService) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.NotFound(w, request)
		return
	}
	if request.URL.Path == FinalVerifyKeyPath {
		s.serveVerifyKey(w, request)
		return
	}
	if request.URL.Path == FinalKeyLeasePath {
		s.serveKeyLease(w, request)
		return
	}
	if request.URL.Path != FinalPlanPath {
		http.NotFound(w, request)
		return
	}
	var input PlanRequest
	if err := decodeFinalRequest(request, 4<<20, &input); err != nil {
		http.Error(w, "invalid placement plan request", http.StatusBadRequest)
		return
	}
	response, err := s.Plan(request.Context(), input)
	if err != nil {
		response.Error = err.Error()
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func (s *FinalService) serveKeyLease(w http.ResponseWriter, request *http.Request) {
	var input KeyLeaseRequest
	if err := decodeFinalRequest(request, 1<<20, &input); err != nil ||
		input.Group == "" || input.AuthKeyFingerprint == "" || input.ManifestKeyFingerprint == "" {
		http.Error(w, "invalid Provider key lease request", http.StatusBadRequest)
		return
	}
	lease, err := ResolveNodeKeyLease(
		request.Context(), s.provider, input.Group, time.Now().Add(DefaultNodeKeyLeaseTTL).Unix(),
	)
	if err == nil && (lease.AuthKey.Fingerprint != input.AuthKeyFingerprint ||
		lease.ManifestKey.Fingerprint != input.ManifestKeyFingerprint) {
		err = errors.New("placer: requested key lease fingerprints are no longer active")
	}
	response := KeyLeaseResponse{}
	if err != nil {
		response.Error = err.Error()
	} else {
		response.Lease = &lease
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func (s *FinalService) serveVerifyKey(w http.ResponseWriter, request *http.Request) {
	var input VerifyKeyRequest
	if err := decodeFinalRequest(request, 1<<20, &input); err != nil || input.Group == "" || input.APIKey == "" {
		http.Error(w, "invalid Provider authorization request", http.StatusBadRequest)
		return
	}
	secret, found, err := s.provider.GetAuthKey(request.Context(), input.Group)
	if err != nil {
		http.Error(w, "Provider unavailable", http.StatusServiceUnavailable)
		return
	}
	authKey := ""
	if found {
		authKey, err = inlineSecret("auth_key", secret)
	}
	if err != nil {
		http.Error(w, "Provider unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(VerifyKeyResponse{Authorized: found && verifyAPIKey(authKey, input.APIKey)})
}

func decodeFinalRequest(request *http.Request, limit int64, target any) error {
	decoder := json.NewDecoder(io.LimitReader(request.Body, limit+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("placer: request must contain exactly one JSON value")
	}
	return nil
}

func (s *FinalService) Plan(ctx context.Context, request PlanRequest) (PlanResponse, error) {
	if request.Group == "" || len(request.Nodes) == 0 {
		return PlanResponse{}, errors.New("placer: group and Node Catalog are required")
	}
	group, found, err := s.provider.Get(ctx, request.Group)
	if err != nil {
		return PlanResponse{}, err
	}
	if !found {
		return PlanResponse{}, errors.New("placer: group is not present in Provider")
	}
	hint, _, err := s.provider.GetPlacementHint(ctx, request.Group)
	if err != nil {
		return PlanResponse{}, err
	}
	keyLease, err := ResolveNodeKeyLease(ctx, s.provider, request.Group, time.Now().Add(DefaultNodeKeyLeaseTTL).Unix())
	if err != nil {
		return PlanResponse{}, err
	}
	policy := placement.StaticPolicy{
		Selectors: hint.NodeSelectors, ExcludedNodeIDs: stringSet(request.ExcludedNodeIDs),
		TargetRuntimeDigest: request.TargetRuntimeDigest,
	}
	if selectors, ok := effectiveCatalogSelectors(request.Group, request.Nodes, hint.NodeSelectors, s.config.ShuffleSharding); ok {
		policy.Selectors = selectors
	}
	version, err := providerPolicyDigest(
		group, hint, keyLease.AuthKey.Fingerprint, keyLease.ManifestKey.Fingerprint, s.config,
	)
	if err != nil {
		return PlanResponse{}, err
	}

	switch request.Kind {
	case PlanSandbox:
		if request.Sandbox == nil || request.Build != nil || request.RouteKey == "" || request.Sandbox.SandboxID == "" {
			return PlanResponse{}, errors.New("placer: incomplete Sandbox plan request")
		}
		demand, err := placement.NormalizeSandboxDemand(request.Sandbox.Demand)
		if err != nil {
			return PlanResponse{}, err
		}
		candidates, err := placement.PlaceSandboxN(
			request.Nodes, request.Sandbox.Demand, policy, s.config.Candidates, nil,
		)
		if err != nil {
			return PlanResponse{}, err
		}
		if keyLease.AuthKey.Type != routesync.KeyMaterialInline {
			return PlanResponse{}, errors.New("placer: referenced AuthKey requires Provider access-token derivation support")
		}
		templateRef, err := clusterstate.ResolveTemplateRef(group, request.Sandbox.TemplateRef)
		if err != nil {
			return PlanResponse{}, err
		}
		requestedConfig := clusterstate.WithoutSystemMetadata(request.Sandbox.Config)
		effectiveConfig := clusterstate.WithoutSystemMetadata(mergeConfig(group.Config, requestedConfig))
		nodeRequest, err := api.RewriteSandboxCreateEnvelope(
			request.Sandbox.Request, templateRef, request.Sandbox.TimeoutSeconds, effectiveConfig,
		)
		if err != nil {
			return PlanResponse{}, err
		}
		accessToken, err := clusterstate.DeriveAccessToken(keyLease.AuthKey.Value, request.Sandbox.SandboxID)
		if err != nil {
			return PlanResponse{}, err
		}
		spec, err := clusterstate.MarshalSandboxDispatchSpec(clusterstate.SandboxDispatchSpecV1{
			Version: clusterstate.DispatchSpecVersionV1, TemplateRef: templateRef,
			AuthKeyFingerprint:     keyLease.AuthKey.Fingerprint,
			ManifestKeyFingerprint: keyLease.ManifestKey.Fingerprint,
			TargetRuntimeDigest:    request.TargetRuntimeDigest,
			RequestedConfig:        requestedConfig,
			Config:                 effectiveConfig,
			AccessToken:            accessToken, TargetPort: group.TargetPort, TimeoutSeconds: request.Sandbox.TimeoutSeconds,
			Request: nodeRequest,
		})
		if err != nil {
			return PlanResponse{}, err
		}
		return PlanResponse{Candidates: candidates, NormalizedDemand: demand, DispatchSpec: spec, ProviderPolicyVersion: version}, nil

	case PlanBuild:
		if request.Build == nil || request.Sandbox != nil || request.Build.BuildID == "" || request.Build.TemplateID == "" ||
			request.Build.CPUCount <= 0 || request.Build.MemoryMB <= 0 {
			return PlanResponse{}, errors.New("placer: incomplete Build plan request")
		}
		demand, err := placement.NormalizeBuildDemand(request.Build.Demand)
		if err != nil {
			return PlanResponse{}, err
		}
		candidates, err := placement.PlaceBuildN(request.Nodes, request.Build.Demand, policy, s.config.Candidates, nil)
		if err != nil {
			return PlanResponse{}, err
		}
		nodeRequest, err := api.RewriteBuildRegisterEnvelope(request.Build.Request, api.RegisterSpec{
			Name: firstString(request.Build.Names), Tags: append([]string(nil), request.Build.Aliases...),
			Profile: request.Build.Profile, CPUCount: request.Build.CPUCount, MemoryMB: request.Build.MemoryMB,
			Metadata: clusterstate.WithoutSystemMetadata(request.Build.Metadata),
		})
		if err != nil {
			return PlanResponse{}, err
		}
		spec, err := clusterstate.MarshalBuildDispatchSpec(clusterstate.BuildDispatchSpecV1{
			Version: clusterstate.DispatchSpecVersionV1, TemplateID: request.Build.TemplateID,
			AuthKeyFingerprint:     keyLease.AuthKey.Fingerprint,
			ManifestKeyFingerprint: keyLease.ManifestKey.Fingerprint,
			TargetRuntimeDigest:    request.TargetRuntimeDigest,
			Profile:                request.Build.Profile,
			CPUCount:               request.Build.CPUCount, MemoryMB: request.Build.MemoryMB,
			Names: append([]string(nil), request.Build.Names...), Aliases: append([]string(nil), request.Build.Aliases...),
			Metadata: clusterstate.WithoutSystemMetadata(request.Build.Metadata), Request: nodeRequest,
		})
		if err != nil {
			return PlanResponse{}, err
		}
		return PlanResponse{Candidates: candidates, NormalizedDemand: demand, DispatchSpec: spec, ProviderPolicyVersion: version}, nil
	default:
		return PlanResponse{}, errors.New("placer: unsupported plan kind")
	}
}

func providerPolicyDigest(
	group clusterstate.SandboxGroup,
	hint clusterstate.PlacementHint,
	authFingerprint string,
	manifestFingerprint string,
	config clustercfg.PlacementConfig,
) (string, error) {
	value := struct {
		Version             uint16                     `json:"version"`
		Group               clusterstate.SandboxGroup  `json:"group"`
		Hint                clusterstate.PlacementHint `json:"hint"`
		AuthFingerprint     string                     `json:"auth_key_fingerprint"`
		ManifestFingerprint string                     `json:"manifest_key_fingerprint"`
		Candidates          int                        `json:"candidates"`
		Shuffle             []clustercfg.ShuffleRule   `json:"shuffle_sharding,omitempty"`
	}{
		Version: 1, Group: group, Hint: hint,
		AuthFingerprint: authFingerprint, ManifestFingerprint: manifestFingerprint,
		Candidates: config.Candidates, Shuffle: config.ShuffleSharding,
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return "provider-policy-v1:" + hex.EncodeToString(digest[:]), nil
}

func firstString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func stringSet(values []string) map[string]struct{} {
	if len(values) == 0 {
		return nil
	}
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value != "" {
			result[value] = struct{}{}
		}
	}
	return result
}

func effectiveCatalogSelectors(
	group string,
	nodes []placement.CatalogNode,
	selectors []map[string]string,
	rules []clustercfg.ShuffleRule,
) ([]map[string]string, bool) {
	for _, rule := range rules {
		if rule.ShardBy == "" || rule.N <= 0 {
			continue
		}
		seen := make(map[string]struct{})
		values := make([]string, 0)
		for _, node := range nodes {
			if !catalogLabelsContain(node.Labels, rule.Selector) {
				continue
			}
			value := node.Labels[rule.ShardBy]
			if value == "" {
				continue
			}
			if _, exists := seen[value]; exists {
				continue
			}
			seen[value] = struct{}{}
			values = append(values, value)
		}
		if len(values) == 0 {
			continue
		}
		sort.Strings(values)
		count := min(rule.N, len(values))
		slots, err := maglev.LocateN([]byte(group), values, count)
		if err != nil {
			continue
		}
		base := selectors
		if len(base) == 0 {
			base = []map[string]string{{}}
		}
		result := make([]map[string]string, 0, len(slots)*len(base))
		for _, slot := range slots {
			for _, selector := range base {
				combined := make(map[string]string, len(selector)+1)
				for key, value := range selector {
					combined[key] = value
				}
				combined[rule.ShardBy] = slot
				result = append(result, combined)
			}
		}
		return result, true
	}
	return nil, false
}

func catalogLabelsContain(labels, wanted map[string]string) bool {
	for key, value := range wanted {
		if labels[key] != value {
			return false
		}
	}
	return true
}
