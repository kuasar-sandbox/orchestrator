package placer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
)

const defaultGroupPageLimit = 1024

type ImportSource struct {
	SourceID string
	Importer clusterstate.SandboxGroupImporter
}

type ConfiguredGroupInputs struct {
	Sources []ImportSource
}

func NewConfiguredGroupInputs(sources []clustercfg.GroupSourceConfig) (ConfiguredGroupInputs, error) {
	if len(sources) == 0 {
		return ConfiguredGroupInputs{}, nil
	}
	imports := make([]ImportSource, 0, len(sources))
	for _, cfg := range sources {
		switch cfg.SourceType {
		case "file":
			src, err := NewFileGroupSource(cfg.SourceID, cfg.Path)
			if err != nil {
				return ConfiguredGroupInputs{}, err
			}
			imports = append(imports, ImportSource{SourceID: cfg.SourceID, Importer: src})
		default:
			return ConfiguredGroupInputs{}, fmt.Errorf("placer: unsupported group source type %q", cfg.SourceType)
		}
	}
	return ConfiguredGroupInputs{Sources: imports}, nil
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

type fileGroupSource struct {
	sourceID string
	dir      string
}

// Read returns one complete record. A maintenance operation must derive its
// context, static target selection and credentials from this single read.
func (s *fileGroupSource) Read(ctx context.Context, group string) (clusterstate.SandboxGroupRecord, bool, error) {
	return s.find(ctx, group)
}

func (s *fileGroupSource) Range(ctx context.Context, cursor string, limit int) (clusterstate.GroupPage, error) {
	records, err := s.records(ctx)
	if err != nil {
		return clusterstate.GroupPage{}, err
	}
	groups := make([]string, 0, len(records))
	for _, rec := range records {
		groups = append(groups, rec.Group)
	}
	sort.Strings(groups)
	return groupPage(groups, cursor, limit)
}

func groupPage(groups []string, cursor string, limit int) (clusterstate.GroupPage, error) {
	if limit <= 0 {
		limit = defaultGroupPageLimit
	}
	start, err := parseGroupCursor(cursor)
	if err != nil {
		return clusterstate.GroupPage{}, err
	}
	if start >= len(groups) {
		return clusterstate.GroupPage{}, nil
	}
	end := start + min(limit, len(groups)-start)
	next := ""
	if end < len(groups) {
		next = strconv.Itoa(end)
	}
	return clusterstate.GroupPage{Groups: append([]string(nil), groups[start:end]...), NextCursor: next}, nil
}

// snapshot validates the file source once for an import pass. Source changes
// become visible on the next pass; pages and records are detached so callers
// cannot change another target's input within the current pass.
func (s *fileGroupSource) snapshot(ctx context.Context) (*fileGroupSnapshot, error) {
	records, err := s.records(ctx)
	if err != nil {
		return nil, err
	}
	snapshot := &fileGroupSnapshot{
		records: make(map[string]clusterstate.SandboxGroupRecord, len(records)),
		groups:  make([]string, 0, len(records)),
	}
	for _, record := range records {
		snapshot.records[record.Group] = record
		snapshot.groups = append(snapshot.groups, record.Group)
	}
	sort.Strings(snapshot.groups)
	return snapshot, nil
}

type fileGroupSnapshot struct {
	records map[string]clusterstate.SandboxGroupRecord
	groups  []string
}

func (s *fileGroupSnapshot) Range(ctx context.Context, cursor string, limit int) (clusterstate.GroupPage, error) {
	if err := ctx.Err(); err != nil {
		return clusterstate.GroupPage{}, err
	}
	return groupPage(s.groups, cursor, limit)
}

func (s *fileGroupSnapshot) Read(ctx context.Context, group string) (clusterstate.SandboxGroupRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return clusterstate.SandboxGroupRecord{}, false, err
	}
	record, found := s.records[group]
	return cloneGroupRecord(record), found, nil
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
	file, err := os.Open(path)
	if err != nil {
		return clusterstate.SandboxGroupRecord{}, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if err != nil {
		return clusterstate.SandboxGroupRecord{}, err
	}
	if len(raw) > 1<<20 {
		return clusterstate.SandboxGroupRecord{}, fmt.Errorf("parse %s: import record exceeds 1 MiB", path)
	}
	var rec clusterstate.SandboxGroupRecord
	if err := strictjson.DecodeAllowNull(raw, &rec); err != nil {
		return clusterstate.SandboxGroupRecord{}, fmt.Errorf("parse %s: %w", path, err)
	}
	if rec.Group == "" {
		return clusterstate.SandboxGroupRecord{}, fmt.Errorf("parse %s: group is required", path)
	}
	if len(rec.ShuffleLabels) != 0 {
		return clusterstate.SandboxGroupRecord{}, fmt.Errorf("parse %s: shuffle_labels has no consumer; remove it", path)
	}
	if _, _, err := importContext(rec); err != nil {
		return clusterstate.SandboxGroupRecord{}, fmt.Errorf("parse %s: %w", path, err)
	}
	rec.Config, err = sandboxcfg.NormalizeResourceMetadata(rec.Config)
	if err != nil {
		return clusterstate.SandboxGroupRecord{}, fmt.Errorf("parse %s: sandbox_config: %w", path, err)
	}
	rec.Config, err = sandboxcfg.NormalizeTrafficMetadata(rec.Config)
	if err != nil {
		return clusterstate.SandboxGroupRecord{}, fmt.Errorf("parse %s: sandbox_config: %w", path, err)
	}
	return rec, nil
}

func importContext(rec clusterstate.SandboxGroupRecord) (nodegroup.Context, []byte, error) {
	if err := nodegroup.ValidateName(rec.Group); err != nil {
		return nodegroup.Context{}, nil, err
	}
	c := nodegroup.Context{
		SandboxConfig: rec.Config, ImageRepo: rec.ImageRepo, TargetPort: rec.TargetPort,
		ProjectID: rec.ProjectID, Metadata: rec.Metadata,
	}
	if rec.TemplateRef != "" {
		c.TemplateID = &rec.TemplateRef
	}
	if !secretAbsent(rec.RegistryAuth) {
		c.RegistryAuth = &rec.RegistryAuth
	}
	return nodegroup.Normalize(c)
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
