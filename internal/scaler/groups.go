package scaler

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/apikey"
	clusterstate "github.com/kuasar-sandbox/sandbox-orchestrator/internal/cluster"
)

const GroupImportPath = "/scale-link/import-groups"

const SnapshotKindGroup = "group"

type GroupImportRecord struct {
	Type  string                           `json:"type"`
	Group *clusterstate.SandboxGroupRecord `json:"group,omitempty"`
}

type GroupImportSummary struct {
	Groups int `json:"groups"`
}

type groupStore struct {
	mu     sync.RWMutex
	groups map[string]clusterstate.SandboxGroupRecord
}

func newGroupStore() *groupStore {
	return &groupStore{groups: map[string]clusterstate.SandboxGroupRecord{}}
}

func (s *groupStore) get(group string) (clusterstate.SandboxGroupRecord, bool) {
	s.pruneExpired(time.Now().Unix())
	s.mu.RLock()
	defer s.mu.RUnlock()
	g, ok := s.groups[group]
	if !ok || g.Deleted {
		return clusterstate.SandboxGroupRecord{}, false
	}
	return cloneGroupRecord(g), true
}

func (s *groupStore) values() []clusterstate.SandboxGroupRecord {
	s.pruneExpired(time.Now().Unix())
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]clusterstate.SandboxGroupRecord, 0, len(s.groups))
	for _, g := range s.groups {
		if g.Deleted {
			continue
		}
		out = append(out, cloneGroupRecord(g))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Group < out[j].Group })
	return out
}

func (s *groupStore) upsert(groups []clusterstate.SandboxGroupRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, g := range groups {
		if g.Group == "" {
			continue
		}
		if g.Deleted {
			delete(s.groups, g.Group)
			continue
		}
		if g.ExpiresUnix > 0 && g.ExpiresUnix <= time.Now().Unix() {
			delete(s.groups, g.Group)
			continue
		}
		s.groups[g.Group] = cloneGroupRecord(g)
	}
}

func (s *groupStore) replace(groups []clusterstate.SandboxGroupRecord) {
	next := make(map[string]clusterstate.SandboxGroupRecord, len(groups))
	now := time.Now().Unix()
	for _, g := range groups {
		if g.Group == "" || g.Deleted || (g.ExpiresUnix > 0 && g.ExpiresUnix <= now) {
			continue
		}
		next[g.Group] = cloneGroupRecord(g)
	}
	s.mu.Lock()
	s.groups = next
	s.mu.Unlock()
}

func (s *groupStore) pruneExpired(now int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for group, g := range s.groups {
		if g.ExpiresUnix > 0 && g.ExpiresUnix <= now {
			delete(s.groups, group)
		}
	}
}

func importGroups(rd io.Reader) ([]clusterstate.SandboxGroupRecord, GroupImportSummary, error) {
	sc := bufio.NewScanner(rd)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	var groups []clusterstate.SandboxGroupRecord
	line := 0
	for sc.Scan() {
		line++
		raw := strings.TrimSpace(sc.Text())
		if raw == "" {
			continue
		}
		var rec GroupImportRecord
		if err := json.Unmarshal([]byte(raw), &rec); err != nil {
			return nil, GroupImportSummary{}, fmt.Errorf("scaler group import line %d: %w", line, err)
		}
		if rec.Type == "" {
			rec.Type = SnapshotKindGroup
		}
		if rec.Type != SnapshotKindGroup {
			return nil, GroupImportSummary{}, fmt.Errorf("scaler group import line %d: unknown type %q", line, rec.Type)
		}
		if rec.Group == nil || rec.Group.Group == "" {
			return nil, GroupImportSummary{}, fmt.Errorf("scaler group import line %d: group record missing group", line)
		}
		groups = append(groups, cloneGroupRecord(*rec.Group))
	}
	if err := sc.Err(); err != nil {
		return nil, GroupImportSummary{}, err
	}
	return groups, GroupImportSummary{Groups: len(groups)}, nil
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

func manifestKeyPatch(g clusterstate.SandboxGroupRecord) (fp, keyType, keyValue, keyRef string, err error) {
	if g.ManifestKey.Value == "" {
		return "", "", "", "", nil
	}
	keyType = g.ManifestKey.Type
	if keyType == "" {
		keyType = clusterstate.SecretInline
	}
	switch keyType {
	case clusterstate.SecretInline:
		fp = manifestKeyFingerprint(g.ManifestKey.Value)
		if fp == "" {
			return "", "", "", "", fmt.Errorf("scaler: invalid manifest_key for group %q", g.Group)
		}
		return fp, keyType, g.ManifestKey.Value, "", nil
	case clusterstate.SecretRef:
		if g.ManifestKey.Fingerprint == "" {
			return "", "", "", "", fmt.Errorf("scaler: manifest_key ref for group %q missing fingerprint", g.Group)
		}
		return g.ManifestKey.Fingerprint, keyType, "", g.ManifestKey.Value, nil
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
