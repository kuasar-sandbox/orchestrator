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
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
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
	Kind                PlanKind                   `json:"kind"`
	Group               string                     `json:"group"`
	RouteKey            string                     `json:"route_key,omitempty"`
	Catalog             placement.CatalogReference `json:"catalog"`
	ExcludedNodeIDs     []string                   `json:"excluded_node_ids,omitempty"`
	TargetRuntimeDigest string                     `json:"target_runtime_digest,omitempty"`
	Sandbox             *SandboxPlanInput          `json:"sandbox,omitempty"`
	Build               *BuildPlanInput            `json:"build,omitempty"`
}

type PlanResponse struct {
	Candidates            []clusterstate.PlacementCandidate `json:"candidates,omitempty"`
	NormalizedDemand      []byte                            `json:"normalized_demand,omitempty"`
	DispatchSpec          []byte                            `json:"dispatch_spec,omitempty"`
	ProviderPolicyVersion string                            `json:"provider_policy_version,omitempty"`
	ErrorCode             string                            `json:"error_code,omitempty"`
	Error                 string                            `json:"error,omitempty"`
}

type FinalService struct {
	provider clusterstate.SandboxGroupProvider
	config   clustercfg.PlacementConfig
	catalogs *catalogCache
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
	return &FinalService{provider: provider, config: config, catalogs: newCatalogCache()}, nil
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
	if request.URL.Path == FinalCatalogSyncPath {
		s.serveCatalogSync(w, request)
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
		if errors.Is(err, ErrCatalogUnavailable) {
			response.ErrorCode = PlanErrorCatalogUnavailable
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func (s *FinalService) serveCatalogSync(w http.ResponseWriter, request *http.Request) {
	var input CatalogSyncRequest
	if err := decodeFinalRequest(request, MaximumCatalogSyncRequestBytes, &input); err != nil {
		http.Error(w, "invalid Node Catalog sync request", http.StatusBadRequest)
		return
	}
	response, err := s.catalogs.install(input)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	if err := response.ValidateFor(input); err != nil {
		http.Error(w, "invalid Node Catalog sync response", http.StatusInternalServerError)
		return
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
	authorized, err := verifyProviderAPIKey(request.Context(), s.provider, input.Group, input.APIKey)
	if err != nil {
		http.Error(w, "Provider unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(VerifyKeyResponse{Authorized: authorized})
}

func verifyProviderAPIKey(
	ctx context.Context,
	provider clusterstate.SandboxGroupProvider,
	group string,
	apiKey string,
) (bool, error) {
	if authorizer, ok := provider.(clusterstate.SandboxGroupAuthorizer); ok {
		return authorizer.VerifyAPIKey(ctx, group, apiKey)
	}
	secret, found, err := provider.GetAuthKey(ctx, group)
	if err != nil || !found {
		return false, err
	}
	authKey, err := inlineSecret("auth_key", secret)
	if err != nil {
		return false, errors.New("placer: referenced AuthKey Provider must implement SandboxGroupAuthorizer")
	}
	return verifyAPIKey(authKey, apiKey), nil
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
	if request.Group == "" {
		return PlanResponse{}, errors.New("placer: group is required")
	}
	nodes, err := s.catalogs.nodes(request.Catalog)
	if err != nil {
		return PlanResponse{}, catalogSyncError(request.Catalog, err)
	}
	if len(nodes) == 0 {
		return PlanResponse{}, errors.New("placer: Node Catalog is empty")
	}
	record, found, err := s.provider.GetRecord(ctx, request.Group)
	if err != nil {
		return PlanResponse{}, err
	}
	if !found || record.Group != request.Group {
		return PlanResponse{}, errors.New("placer: group is not present in Provider")
	}
	group := groupRecordToGroup(record)
	hint := clusterstate.PlacementHint{
		NodeSelectors: cloneSelectors(record.NodeSelectors), ShuffleLabels: cloneStringMap(record.ShuffleLabels),
	}
	keyLease, err := nodeKeyLeaseFromRecord(record, time.Now().Add(DefaultNodeKeyLeaseTTL).Unix())
	if err != nil {
		return PlanResponse{}, err
	}
	policy := placement.StaticPolicy{
		Selectors: hint.NodeSelectors, ExcludedNodeIDs: stringSet(request.ExcludedNodeIDs),
		TargetRuntimeDigest: request.TargetRuntimeDigest,
	}
	switch request.Kind {
	case PlanSandbox:
		policy.RequiredCapabilities = []string{"sandbox"}
	case PlanBuild:
		policy.RequiredCapabilities = []string{"build"}
	}
	if selectors, ok := effectiveCatalogSelectors(request.Group, nodes, hint.NodeSelectors, s.config.ShuffleSharding); ok {
		if len(selectors) == 0 {
			return PlanResponse{}, errors.New("placer: applicable shuffle-sharding rule has no eligible shard values")
		}
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
		templateRef, err := clusterstate.ResolveTemplateRef(group, request.Sandbox.TemplateRef)
		if err != nil {
			return PlanResponse{}, err
		}
		template, err := types.ParseTemplateID(templateRef)
		if err != nil {
			return PlanResponse{}, err
		}
		requestedConfig := clusterstate.WithoutSystemMetadata(request.Sandbox.Config)
		effectiveConfig := clusterstate.WithoutSystemMetadata(mergeConfig(group.Config, requestedConfig))
		var resources sandboxcfg.ResolvedResources
		if template.Kind == types.KindSnp {
			resources, err = sandboxcfg.ResolveRestoreResources(effectiveConfig)
		} else {
			resources, err = sandboxcfg.ResolveResources(effectiveConfig)
		}
		if err != nil {
			return PlanResponse{}, err
		}
		sandboxDemand, err := mergeSandboxDemand(request.Sandbox.Demand, resources)
		if err != nil {
			return PlanResponse{}, err
		}
		demand, err := placement.NormalizeSandboxDemand(sandboxDemand)
		if err != nil {
			return PlanResponse{}, err
		}
		candidates, err := placement.PlaceSandboxN(
			nodes, sandboxDemand, policy, s.config.Candidates, nil,
		)
		if err != nil {
			return PlanResponse{}, err
		}
		nodeRequest, err := api.RewriteSandboxCreateEnvelope(
			request.Sandbox.Request, templateRef, request.Sandbox.TimeoutSeconds, effectiveConfig,
		)
		if err != nil {
			return PlanResponse{}, err
		}
		accessToken, err := keys.MintToken()
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
		candidates, err := placement.PlaceBuildN(nodes, request.Build.Demand, policy, s.config.Candidates, nil)
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

func mergeSandboxDemand(
	demand placement.SandboxDemand,
	resources sandboxcfg.ResolvedResources,
) (placement.SandboxDemand, error) {
	if resources.FloorMemoryBytes > 0 {
		if demand.FloorMemory > 0 && demand.FloorMemory != resources.FloorMemoryBytes {
			return placement.SandboxDemand{}, errors.New("placer: Sandbox floor memory conflicts with effective config")
		}
		demand.FloorMemory = resources.FloorMemoryBytes
	}
	if resources.StartupMemoryBytes > 0 {
		if demand.StartupBudgetMemory > 0 && demand.StartupBudgetMemory != resources.StartupMemoryBytes {
			return placement.SandboxDemand{}, errors.New("placer: Sandbox startup memory conflicts with effective config")
		}
		demand.StartupBudgetMemory = resources.StartupMemoryBytes
	}
	return demand, nil
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
	base := selectors
	if len(base) == 0 {
		base = []map[string]string{{}}
	}
	for _, rule := range rules {
		if rule.ShardBy == "" || rule.N <= 0 {
			continue
		}
		result := make([]map[string]string, 0)
		applicable := false
		for baseIndex, selector := range base {
			predicate, compatible := mergeCatalogSelector(selector, rule.Selector)
			if !compatible {
				continue
			}
			applicable = true
			seen := make(map[string]struct{})
			values := make([]string, 0)
			for _, node := range nodes {
				if !catalogLabelsContain(node.Labels, predicate) {
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
			key := []byte(fmt.Sprintf("%s\x00%d", group, baseIndex))
			slots, err := maglev.LocateN(key, values, count)
			if err != nil {
				continue
			}
			for _, slot := range slots {
				combined := cloneSelector(predicate)
				combined[rule.ShardBy] = slot
				result = append(result, combined)
			}
		}
		if len(result) != 0 {
			return result, true
		}
		if applicable {
			return nil, true
		}
	}
	return nil, false
}

func mergeCatalogSelector(left, right map[string]string) (map[string]string, bool) {
	merged := cloneSelector(left)
	for key, value := range right {
		if current, found := merged[key]; found && current != value {
			return nil, false
		}
		merged[key] = value
	}
	return merged, true
}

func cloneSelector(source map[string]string) map[string]string {
	result := make(map[string]string, len(source)+1)
	for key, value := range source {
		result[key] = value
	}
	return result
}

func catalogLabelsContain(labels, wanted map[string]string) bool {
	for key, value := range wanted {
		if labels[key] != value {
			return false
		}
	}
	return true
}
