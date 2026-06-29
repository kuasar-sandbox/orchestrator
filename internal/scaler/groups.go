package scaler

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/apikey"
	clusterstate "github.com/kuasar-sandbox/sandbox-orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
)

const defaultGroupPageLimit = 1024

type GroupSource interface {
	clusterstate.SandboxGroupProvider
	clusterstate.SandboxGroupImporter
}

type ImportTask struct {
	ID       string
	Importer clusterstate.SandboxGroupImporter
}

type ImportTaskSource interface {
	ImportTasks() []ImportTask
}

func NewConfiguredGroupSource(sources []clustercfg.GroupSourceConfig) (GroupSource, error) {
	if len(sources) == 0 {
		return emptyGroupSource{}, nil
	}
	out := make([]GroupSource, 0, len(sources))
	for _, cfg := range sources {
		switch cfg.SourceType {
		case "file":
			src, err := NewFileGroupSource(cfg.SourceID, cfg.Path)
			if err != nil {
				return nil, err
			}
			out = append(out, src)
		default:
			return nil, fmt.Errorf("scaler: unsupported group source type %q", cfg.SourceType)
		}
	}
	return multiGroupSource{sources: out}, nil
}

func NewFileGroupSource(sourceID, dir string) (GroupSource, error) {
	if sourceID == "" {
		return nil, fmt.Errorf("scaler: file group source id is required")
	}
	if dir == "" {
		return nil, fmt.Errorf("scaler: file group source %q path is required", sourceID)
	}
	info, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("scaler: file group source %q stat %s: %w", sourceID, dir, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("scaler: file group source %q path %s is not a directory", sourceID, dir)
	}
	return &fileGroupSource{sourceID: sourceID, dir: dir}, nil
}

type emptyGroupSource struct{}

func (emptyGroupSource) Get(context.Context, string) (clusterstate.SandboxGroup, bool, error) {
	return clusterstate.SandboxGroup{}, false, nil
}

func (emptyGroupSource) GetPlacementHint(context.Context, string) (clusterstate.PlacementHint, bool, error) {
	return clusterstate.PlacementHint{}, false, nil
}

func (emptyGroupSource) GetKey(context.Context, string) (clusterstate.Secret, bool, error) {
	return clusterstate.Secret{}, false, nil
}

func (emptyGroupSource) GetAuthKey(context.Context, string) (clusterstate.Secret, bool, error) {
	return clusterstate.Secret{}, false, nil
}

func (emptyGroupSource) Range(context.Context, string, int) (clusterstate.GroupPage, error) {
	return clusterstate.GroupPage{}, nil
}

func (emptyGroupSource) ImportTasks() []ImportTask { return nil }

type multiGroupSource struct {
	sources []GroupSource
}

func (m multiGroupSource) Get(ctx context.Context, group string) (clusterstate.SandboxGroup, bool, error) {
	for _, source := range m.sources {
		g, found, err := source.Get(ctx, group)
		if err != nil || found {
			return g, found, err
		}
	}
	return clusterstate.SandboxGroup{}, false, nil
}

func (m multiGroupSource) GetPlacementHint(ctx context.Context, group string) (clusterstate.PlacementHint, bool, error) {
	for _, source := range m.sources {
		hint, found, err := source.GetPlacementHint(ctx, group)
		if err != nil || found {
			return hint, found, err
		}
	}
	return clusterstate.PlacementHint{}, false, nil
}

func (m multiGroupSource) GetKey(ctx context.Context, group string) (clusterstate.Secret, bool, error) {
	for _, source := range m.sources {
		key, found, err := source.GetKey(ctx, group)
		if err != nil || found {
			return key, found, err
		}
	}
	return clusterstate.Secret{}, false, nil
}

func (m multiGroupSource) GetAuthKey(ctx context.Context, group string) (clusterstate.Secret, bool, error) {
	for _, source := range m.sources {
		key, found, err := source.GetAuthKey(ctx, group)
		if err != nil || found {
			return key, found, err
		}
	}
	return clusterstate.Secret{}, false, nil
}

func (m multiGroupSource) Range(ctx context.Context, cursor string, limit int) (clusterstate.GroupPage, error) {
	if limit <= 0 {
		limit = defaultGroupPageLimit
	}
	seen := map[string]bool{}
	for _, source := range m.sources {
		next := ""
		for {
			page, err := source.Range(ctx, next, defaultGroupPageLimit)
			if err != nil {
				return clusterstate.GroupPage{}, err
			}
			for _, group := range page.Groups {
				seen[group] = true
			}
			if page.NextCursor == "" {
				break
			}
			next = page.NextCursor
		}
	}
	groups := make([]string, 0, len(seen))
	for group := range seen {
		groups = append(groups, group)
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

func (m multiGroupSource) ImportTasks() []ImportTask {
	var out []ImportTask
	for _, source := range m.sources {
		if tasks, ok := source.(ImportTaskSource); ok {
			out = append(out, tasks.ImportTasks()...)
			continue
		}
		out = append(out, ImportTask{ID: "default", Importer: source})
	}
	return out
}

type fileGroupSource struct {
	sourceID string
	dir      string
}

func (s *fileGroupSource) ImportTasks() []ImportTask {
	return []ImportTask{{ID: s.sourceID, Importer: s}}
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
	return rec.ManifestKey, true, nil
}

func (s *fileGroupSource) GetAuthKey(ctx context.Context, group string) (clusterstate.Secret, bool, error) {
	rec, found, err := s.find(ctx, group)
	if err != nil || !found {
		return clusterstate.Secret{}, false, err
	}
	return rec.AuthKey, true, nil
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
		return nil, fmt.Errorf("scaler: file group source %q read %s: %w", s.sourceID, s.dir, err)
	}
	records := make([]clusterstate.SandboxGroupRecord, 0, len(entries))
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		rec, err := readGroupRecord(filepath.Join(s.dir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("scaler: file group source %q: %w", s.sourceID, err)
		}
		if !groupRecordActive(rec) {
			continue
		}
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
		return 0, fmt.Errorf("scaler: invalid group cursor %q", cursor)
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
		Metadata:     cloneStringMap(rec.Metadata),
	}
}

func inlineSecret(kind string, s clusterstate.Secret) (string, error) {
	if s.Type == "" || s.Value == "" {
		return "", nil
	}
	if s.Type != clusterstate.SecretInline {
		return "", fmt.Errorf("scaler: %s secret type %q requires an out-of-band resolver", kind, s.Type)
	}
	return s.Value, nil
}

func manifestKeyPatch(group string, key clusterstate.Secret) (fp, keyType, keyValue, keyRef string, err error) {
	if key.Value == "" {
		return "", "", "", "", nil
	}
	keyType = key.Type
	if keyType == "" {
		keyType = clusterstate.SecretInline
	}
	switch keyType {
	case clusterstate.SecretInline:
		fp = manifestKeyFingerprint(key.Value)
		if fp == "" {
			return "", "", "", "", fmt.Errorf("scaler: invalid manifest_key for group %q", group)
		}
		return fp, keyType, key.Value, "", nil
	case clusterstate.SecretRef:
		if key.Fingerprint == "" {
			return "", "", "", "", fmt.Errorf("scaler: manifest_key ref for group %q missing fingerprint", group)
		}
		return key.Fingerprint, keyType, "", key.Value, nil
	default:
		return "", "", "", "", fmt.Errorf("scaler: unknown manifest_key type %q", keyType)
	}
}

func manifestKeyFingerprint(manifestKeyHex string) string {
	raw, err := hex.DecodeString(manifestKeyHex)
	if err != nil {
		return ""
	}
	return hex.EncodeToString(apikey.Fingerprint(raw))
}

func verifyAPIKey(authKeyHex, encoded string) bool {
	p, err := apikey.Parse(encoded)
	if err != nil {
		return false
	}
	raw, err := hex.DecodeString(authKeyHex)
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
