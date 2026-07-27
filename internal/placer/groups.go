package placer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
)

const defaultGroupPageLimit = 1024

type ImportSource struct {
	SourceID string
	Importer clusterstate.SandboxGroupImporter
}

type ConfiguredGroupInputs struct {
	Provider clusterstate.SandboxGroupProvider
	Sources  []ImportSource
}

func NewConfiguredGroupInputs(sources []clustercfg.GroupSourceConfig) (ConfiguredGroupInputs, error) {
	if len(sources) == 0 {
		return ConfiguredGroupInputs{Provider: emptyGroupProvider{}}, nil
	}
	out := make([]*fileGroupSource, 0, len(sources))
	imports := make([]ImportSource, 0, len(sources))
	for _, cfg := range sources {
		switch cfg.SourceType {
		case "file":
			src, err := NewFileGroupSource(cfg.SourceID, cfg.Path)
			if err != nil {
				return ConfiguredGroupInputs{}, err
			}
			out = append(out, src)
			imports = append(imports, ImportSource{SourceID: cfg.SourceID, Importer: src})
		default:
			return ConfiguredGroupInputs{}, fmt.Errorf("placer: unsupported group source type %q", cfg.SourceType)
		}
	}
	return ConfiguredGroupInputs{Provider: multiGroupProvider{sources: out}, Sources: imports}, nil
}

func NewFileGroupSource(sourceID, dir string) (*fileGroupSource, error) {
	if sourceID == "" {
		return nil, fmt.Errorf("placer: file group source id is required")
	}
	if dir == "" {
		return nil, fmt.Errorf("placer: file group source %q path is required", sourceID)
	}
	info, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("placer: file group source %q stat %s: %w", sourceID, dir, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("placer: file group source %q path %s is not a directory", sourceID, dir)
	}
	return &fileGroupSource{sourceID: sourceID, dir: dir}, nil
}

type emptyGroupProvider struct{}

func (emptyGroupProvider) Get(context.Context, string) (clusterstate.SandboxGroup, bool, error) {
	return clusterstate.SandboxGroup{}, false, nil
}

func (emptyGroupProvider) GetPlacementHint(context.Context, string) (clusterstate.PlacementHint, bool, error) {
	return clusterstate.PlacementHint{}, false, nil
}

func (emptyGroupProvider) GetKey(context.Context, string) (clusterstate.Secret, bool, error) {
	return clusterstate.Secret{}, false, nil
}

func (emptyGroupProvider) GetAPISecret(context.Context, string) (clusterstate.Secret, bool, error) {
	return clusterstate.Secret{}, false, nil
}

type multiGroupProvider struct {
	sources []*fileGroupSource
}

func (m multiGroupProvider) Get(ctx context.Context, group string) (clusterstate.SandboxGroup, bool, error) {
	var out clusterstate.SandboxGroup
	foundOne := ""
	for _, source := range m.sources {
		g, found, err := source.Get(ctx, group)
		if err != nil {
			return clusterstate.SandboxGroup{}, false, err
		}
		if !found {
			continue
		}
		if foundOne != "" {
			return clusterstate.SandboxGroup{}, false, duplicateGroupError(group, foundOne, source.sourceID)
		}
		foundOne = source.sourceID
		out = g
	}
	return out, foundOne != "", nil
}

func (m multiGroupProvider) GetPlacementHint(ctx context.Context, group string) (clusterstate.PlacementHint, bool, error) {
	var out clusterstate.PlacementHint
	foundOne := ""
	for _, source := range m.sources {
		hint, found, err := source.GetPlacementHint(ctx, group)
		if err != nil {
			return clusterstate.PlacementHint{}, false, err
		}
		if !found {
			continue
		}
		if foundOne != "" {
			return clusterstate.PlacementHint{}, false, duplicateGroupError(group, foundOne, source.sourceID)
		}
		foundOne = source.sourceID
		out = hint
	}
	return out, foundOne != "", nil
}

func (m multiGroupProvider) GetKey(ctx context.Context, group string) (clusterstate.Secret, bool, error) {
	var out clusterstate.Secret
	foundOne := ""
	for _, source := range m.sources {
		key, found, err := source.GetKey(ctx, group)
		if err != nil {
			return clusterstate.Secret{}, false, err
		}
		if !found {
			continue
		}
		if foundOne != "" {
			return clusterstate.Secret{}, false, duplicateGroupError(group, foundOne, source.sourceID)
		}
		foundOne = source.sourceID
		out = key
	}
	return out, foundOne != "", nil
}

func (m multiGroupProvider) GetAPISecret(ctx context.Context, group string) (clusterstate.Secret, bool, error) {
	var out clusterstate.Secret
	foundOne := ""
	for _, source := range m.sources {
		key, found, err := source.GetAPISecret(ctx, group)
		if err != nil {
			return clusterstate.Secret{}, false, err
		}
		if !found {
			continue
		}
		if foundOne != "" {
			return clusterstate.Secret{}, false, duplicateGroupError(group, foundOne, source.sourceID)
		}
		foundOne = source.sourceID
		out = key
	}
	return out, foundOne != "", nil
}

func duplicateGroupError(group, firstSource, secondSource string) error {
	return fmt.Errorf("placer: group %q is defined by multiple sources (%s, %s)", group, firstSource, secondSource)
}

type fileGroupSource struct {
	sourceID string
	dir      string
}

func (s *fileGroupSource) Get(ctx context.Context, group string) (clusterstate.SandboxGroup, bool, error) {
	rec, found, err := s.find(ctx, group)
	if err != nil || !found {
		return clusterstate.SandboxGroup{}, false, err
	}
	return groupRecordToGroup(rec), true, nil
}

func (s *fileGroupSource) GetPlacementHint(ctx context.Context, group string) (clusterstate.PlacementHint, bool, error) {
	rec, found, err := s.find(ctx, group)
	if err != nil || !found {
		return clusterstate.PlacementHint{}, false, err
	}
	return clusterstate.PlacementHint{NodeSelectors: cloneSelectors(rec.NodeSelectors), ShuffleLabels: cloneStringMap(rec.ShuffleLabels)}, true, nil
}

func (s *fileGroupSource) GetKey(ctx context.Context, group string) (clusterstate.Secret, bool, error) {
	rec, found, err := s.find(ctx, group)
	if err != nil || !found {
		return clusterstate.Secret{}, false, err
	}
	key, err := normalizeManifestKey(rec.ManifestKey)
	if err != nil {
		return clusterstate.Secret{}, false, fmt.Errorf("placer: group %q: %w", group, err)
	}
	return key, true, nil
}

func (s *fileGroupSource) GetAPISecret(ctx context.Context, group string) (clusterstate.Secret, bool, error) {
	rec, found, err := s.find(ctx, group)
	if err != nil || !found {
		return clusterstate.Secret{}, false, err
	}
	apiSecret, err := materializeAPISecret(rec.ManifestKey, rec.APISecret)
	if err != nil {
		return clusterstate.Secret{}, false, fmt.Errorf("placer: group %q: %w", group, err)
	}
	return apiSecret, true, nil
}

func (s *fileGroupSource) Range(ctx context.Context, cursor string, limit int) (clusterstate.GroupPage, error) {
	if limit <= 0 {
		limit = defaultGroupPageLimit
	}
	records, err := s.records(ctx)
	if err != nil {
		return clusterstate.GroupPage{}, err
	}
	groups := make([]string, 0, len(records))
	for _, rec := range records {
		groups = append(groups, rec.Group)
	}
	sort.Strings(groups)
	start, err := parseGroupCursor(cursor)
	if err != nil {
		return clusterstate.GroupPage{}, err
	}
	if start >= len(groups) {
		return clusterstate.GroupPage{}, nil
	}
	end := start + limit
	if end > len(groups) {
		end = len(groups)
	}
	next := ""
	if end < len(groups) {
		next = strconv.Itoa(end)
	}
	return clusterstate.GroupPage{Groups: groups[start:end], NextCursor: next}, nil
}

func (s *fileGroupSource) find(ctx context.Context, group string) (clusterstate.SandboxGroupRecord, bool, error) {
	if group == "" {
		return clusterstate.SandboxGroupRecord{}, false, nil
	}
	records, err := s.records(ctx)
	if err != nil {
		return clusterstate.SandboxGroupRecord{}, false, err
	}
	for _, rec := range records {
		if rec.Group == group {
			return rec, true, nil
		}
	}
	return clusterstate.SandboxGroupRecord{}, false, nil
}

func (s *fileGroupSource) records(ctx context.Context) ([]clusterstate.SandboxGroupRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("placer: file group source %q read %s: %w", s.sourceID, s.dir, err)
	}
	records := make([]clusterstate.SandboxGroupRecord, 0, len(entries))
	seen := map[string]string{}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		rec, err := readGroupRecord(filepath.Join(s.dir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("placer: file group source %q: %w", s.sourceID, err)
		}
		if !groupRecordActive(rec) {
			continue
		}
		if prev := seen[rec.Group]; prev != "" {
			return nil, fmt.Errorf("duplicate group %q in %s and %s", rec.Group, prev, filepath.Join(s.dir, entry.Name()))
		}
		seen[rec.Group] = filepath.Join(s.dir, entry.Name())
		records = append(records, cloneGroupRecord(rec))
	}
	return records, nil
}

func readGroupRecord(path string) (clusterstate.SandboxGroupRecord, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return clusterstate.SandboxGroupRecord{}, err
	}
	var rec clusterstate.SandboxGroupRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return clusterstate.SandboxGroupRecord{}, fmt.Errorf("parse %s: %w", path, err)
	}
	if rec.Group == "" {
		return clusterstate.SandboxGroupRecord{}, fmt.Errorf("parse %s: group is required", path)
	}
	return rec, nil
}

func groupRecordActive(rec clusterstate.SandboxGroupRecord) bool {
	return rec.Group != ""
}

func parseGroupCursor(cursor string) (int, error) {
	if cursor == "" {
		return 0, nil
	}
	start, err := strconv.Atoi(cursor)
	if err != nil || start < 0 {
		return 0, fmt.Errorf("placer: invalid group cursor %q", cursor)
	}
	return start, nil
}

func groupRecordToGroup(rec clusterstate.SandboxGroupRecord) clusterstate.SandboxGroup {
	return clusterstate.SandboxGroup{
		Group:        rec.Group,
		Config:       cloneStringMap(rec.Config),
		ImageRepo:    rec.ImageRepo,
		RegistryAuth: rec.RegistryAuth,
		TemplateRef:  rec.TemplateRef,
		TargetPort:   rec.TargetPort,
		Metadata:     cloneStringMap(rec.Metadata),
	}
}

func inlineSecret(kind string, s clusterstate.Secret) (string, error) {
	if s.Type == "" || s.Value == "" {
		return "", nil
	}
	if s.Type != clusterstate.SecretInline {
		return "", fmt.Errorf("placer: %s secret type %q requires an out-of-band resolver", kind, s.Type)
	}
	return s.Value, nil
}

func secretAbsent(secret clusterstate.Secret) bool {
	return secret.Type == "" && secret.Value == "" && secret.Fingerprint == ""
}

func decodeRoot(kind, value string) ([]byte, error) {
	if len(value) != 64 {
		return nil, fmt.Errorf("%s must be 64 lowercase hexadecimal characters", kind)
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return nil, fmt.Errorf("%s must be 64 lowercase hexadecimal characters", kind)
		}
	}
	raw, err := hex.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("%s must be 64 lowercase hexadecimal characters", kind)
	}
	return raw, nil
}

func normalizeManifestKey(key clusterstate.Secret) (clusterstate.Secret, error) {
	if secretAbsent(key) {
		return clusterstate.Secret{}, nil
	}
	keyType := key.Type
	if keyType == "" {
		keyType = clusterstate.SecretInline
	}
	switch keyType {
	case clusterstate.SecretInline:
		if key.Value == "" {
			return clusterstate.Secret{}, fmt.Errorf("manifest_key inline value is required")
		}
		if _, err := decodeRoot("manifest_key", key.Value); err != nil {
			return clusterstate.Secret{}, err
		}
		return clusterstate.Secret{Type: keyType, Value: key.Value}, nil
	case clusterstate.SecretRef:
		if key.Value == "" {
			return clusterstate.Secret{}, fmt.Errorf("manifest_key ref value is required")
		}
		if !isLowerHex64(key.Fingerprint) {
			return clusterstate.Secret{}, fmt.Errorf("manifest_key ref requires a 64-character lowercase hexadecimal fingerprint")
		}
		return clusterstate.Secret{Type: keyType, Value: key.Value, Fingerprint: key.Fingerprint}, nil
	default:
		return clusterstate.Secret{}, fmt.Errorf("unknown manifest_key type %q", keyType)
	}
}

func normalizeAPISecret(apiSecret clusterstate.Secret) (clusterstate.Secret, error) {
	if secretAbsent(apiSecret) {
		return clusterstate.Secret{}, nil
	}
	secretType := apiSecret.Type
	if secretType == "" {
		secretType = clusterstate.SecretInline
	}
	switch secretType {
	case clusterstate.SecretInline:
		if apiSecret.Value == "" {
			return clusterstate.Secret{}, fmt.Errorf("api_secret inline value is required")
		}
		if _, err := decodeRoot("api_secret", apiSecret.Value); err != nil {
			return clusterstate.Secret{}, err
		}
		return clusterstate.Secret{Type: secretType, Value: apiSecret.Value}, nil
	case clusterstate.SecretRef:
		if apiSecret.Value == "" {
			return clusterstate.Secret{}, fmt.Errorf("api_secret ref value is required")
		}
		if !isLowerHex64(apiSecret.Fingerprint) {
			return clusterstate.Secret{}, fmt.Errorf("api_secret ref requires a 64-character lowercase hexadecimal fingerprint")
		}
		return clusterstate.Secret{Type: secretType, Value: apiSecret.Value, Fingerprint: apiSecret.Fingerprint}, nil
	default:
		return clusterstate.Secret{}, fmt.Errorf("unknown api_secret type %q", secretType)
	}
}

func materializeAPISecret(manifestKey, apiSecret clusterstate.Secret) (clusterstate.Secret, error) {
	if !secretAbsent(apiSecret) {
		return normalizeAPISecret(apiSecret)
	}
	if secretAbsent(manifestKey) {
		return clusterstate.Secret{}, nil
	}
	manifestKey, err := normalizeManifestKey(manifestKey)
	if err != nil {
		return clusterstate.Secret{}, err
	}
	if manifestKey.Type != clusterstate.SecretInline {
		return clusterstate.Secret{}, fmt.Errorf("api_secret is required when manifest_key is not inline")
	}
	raw, err := hex.DecodeString(manifestKey.Value)
	if err != nil {
		return clusterstate.Secret{}, fmt.Errorf("manifest_key must be 64 lowercase hexadecimal characters")
	}
	return clusterstate.Secret{
		Type:  clusterstate.SecretInline,
		Value: hex.EncodeToString(apikey.DeriveAPISecret(raw)),
	}, nil
}

func manifestKeyPatch(group string, key clusterstate.Secret) (fp, keyType, keyValue, keyRef string, err error) {
	key, err = normalizeManifestKey(key)
	if err != nil {
		return "", "", "", "", fmt.Errorf("placer: group %q: %w", group, err)
	}
	if key.Value == "" {
		return "", "", "", "", nil
	}
	keyType = key.Type
	switch keyType {
	case clusterstate.SecretInline:
		fp, err = manifestKeyFingerprint(key.Value)
		if err != nil {
			return "", "", "", "", fmt.Errorf("placer: group %q: %w", group, err)
		}
		return fp, keyType, key.Value, "", nil
	case clusterstate.SecretRef:
		return key.Fingerprint, keyType, "", key.Value, nil
	default:
		return "", "", "", "", fmt.Errorf("placer: group %q: unknown manifest_key type %q", group, keyType)
	}
}

func apiSecretPatch(group string, apiSecret clusterstate.Secret) (fp, secretType, secretValue, secretRef string, err error) {
	apiSecret, err = normalizeAPISecret(apiSecret)
	if err != nil {
		return "", "", "", "", fmt.Errorf("placer: group %q: %w", group, err)
	}
	if apiSecret.Value == "" {
		return "", "", "", "", nil
	}
	secretType = apiSecret.Type
	switch secretType {
	case clusterstate.SecretInline:
		fp, err = apiSecretFingerprint(apiSecret.Value)
		if err != nil {
			return "", "", "", "", fmt.Errorf("placer: group %q: %w", group, err)
		}
		return fp, secretType, apiSecret.Value, "", nil
	case clusterstate.SecretRef:
		return apiSecret.Fingerprint, secretType, "", apiSecret.Value, nil
	default:
		return "", "", "", "", fmt.Errorf("placer: group %q: unknown api_secret type %q", group, secretType)
	}
}

func credentialPairPatch(group string, manifestKey, apiSecret clusterstate.Secret) (
	apiFP, apiType, apiValue, apiRef,
	manifestFP, manifestType, manifestValue, manifestRef string,
	err error,
) {
	apiSecret, err = materializeAPISecret(manifestKey, apiSecret)
	if err != nil {
		return "", "", "", "", "", "", "", "", fmt.Errorf("placer: group %q: %w", group, err)
	}
	manifestFP, manifestType, manifestValue, manifestRef, err = manifestKeyPatch(group, manifestKey)
	if err != nil {
		return "", "", "", "", "", "", "", "", err
	}
	apiFP, apiType, apiValue, apiRef, err = apiSecretPatch(group, apiSecret)
	if err != nil {
		return "", "", "", "", "", "", "", "", err
	}
	if (apiFP == "") != (manifestFP == "") {
		return "", "", "", "", "", "", "", "", fmt.Errorf("placer: group %q: api_secret and manifest_key must be configured as one pair", group)
	}
	return apiFP, apiType, apiValue, apiRef,
		manifestFP, manifestType, manifestValue, manifestRef, nil
}

func manifestKeyFingerprint(manifestKeyHex string) (string, error) {
	raw, err := decodeRoot("manifest_key", manifestKeyHex)
	if err != nil {
		return "", err
	}
	fp := sha256.Sum256(raw)
	return hex.EncodeToString(fp[:]), nil
}

func apiSecretFingerprint(apiSecretHex string) (string, error) {
	raw, err := decodeRoot("api_secret", apiSecretHex)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(apikey.FullFingerprint(raw)), nil
}

func isLowerHex64(value string) bool {
	_, err := decodeRoot("fingerprint", value)
	return err == nil
}

func verifyAPIKey(apiSecretHex, encoded string) bool {
	p, err := apikey.Parse(encoded)
	if err != nil {
		return false
	}
	raw, err := decodeRoot("api_secret", apiSecretHex)
	if err != nil {
		return false
	}
	return apikey.Verify(p, raw)
}

func mergeConfig(group, create map[string]string) map[string]string {
	if len(group) == 0 {
		return cloneStringMap(create)
	}
	merged := make(map[string]string, len(group)+len(create))
	for k, v := range group {
		merged[k] = v
	}
	for k, v := range create {
		merged[k] = v
	}
	return merged
}

func cloneGroupRecord(in clusterstate.SandboxGroupRecord) clusterstate.SandboxGroupRecord {
	out := in
	out.Config = cloneStringMap(in.Config)
	out.Metadata = cloneStringMap(in.Metadata)
	out.ShuffleLabels = cloneStringMap(in.ShuffleLabels)
	out.NodeSelectors = cloneSelectors(in.NodeSelectors)
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneSelectors(in []map[string]string) []map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make([]map[string]string, len(in))
	for i := range in {
		out[i] = cloneStringMap(in[i])
	}
	return out
}
