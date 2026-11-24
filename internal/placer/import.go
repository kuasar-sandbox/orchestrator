package placer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/orchestrator/internal/clusterclient"
	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	"github.com/kuasar-sandbox/orchestrator/internal/registry"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

const (
	importPairTTL     = 3 * time.Hour
	importRenewBefore = time.Hour
	maxImportBindings = 4096
)

func importTargets(group string, nodes []*registry.NodeRecord, selectors []map[string]string, rules []clustercfg.ShuffleRule) ([]map[string]string, []string) {
	effective := selectors
	if narrowed, ok := effectiveSelectors(group, nodes, selectors, rules); ok {
		if len(narrowed) == 0 {
			return narrowed, nil
		}
		effective = narrowed
	}
	var targets []string
	for _, node := range nodes {
		if node != nil && node.NodeID != "" && matchSelectors(node.Labels, effective) {
			targets = append(targets, node.NodeID)
		}
	}
	sort.Strings(targets)
	return effective, targets
}

type importTarget struct{ source, node, group string }
type importBinding struct {
	registration      metadata.Registration
	version           int64
	apiFP, manifestFP string
	expires           int64
}
type importState struct {
	binding importBinding
	pending *routesync.GroupCommand // fixed CAS, identity, definition and absolute expiry
}

// ImportResult deliberately contains no key, root or private Group context.
type ImportResult struct {
	SourceID          string `json:"source_id"`
	NodeID            string `json:"node_id"`
	Group             string `json:"group"`
	Status            string `json:"status"`
	Version           int64  `json:"version,omitempty"`
	KeyLeaseConfirmed bool   `json:"key_lease_confirmed,omitempty"`
	ExpiresUnix       int64  `json:"expires_unix,omitempty"`
	Error             string `json:"error,omitempty"`
}

func (s *Service) importState(target importTarget) (*importState, error) {
	if s.importStates == nil {
		s.importStates = make(map[importTarget]*importState)
	}
	if state := s.importStates[target]; state != nil {
		return state, nil
	}
	if len(s.importStates) >= maxImportBindings {
		// Confirmed leases are just an optimization. Never evict an outstanding
		// write to make room for another write with a different expiry or CAS.
		for key, state := range s.importStates {
			if state.pending == nil {
				delete(s.importStates, key)
				break
			}
		}
		if len(s.importStates) >= maxImportBindings {
			return nil, errors.New("too many unresolved Group writes")
		}
	}
	state := &importState{}
	s.importStates[target] = state
	return state, nil
}

func importCommand(rec clusterstate.SandboxGroupRecord) (routesync.GroupCommand, error) {
	c, _, err := importContext(rec)
	if err != nil {
		return routesync.GroupCommand{}, err
	}
	api, err := materializeAPISecret(rec.ManifestKey, rec.APISecret)
	if err != nil {
		return routesync.GroupCommand{}, err
	}
	secret, err := inlineSecret("api_secret", api)
	if err != nil {
		return routesync.GroupCommand{}, err
	}
	root, err := decodeRoot("api_secret", secret)
	if err != nil {
		return routesync.GroupCommand{}, err
	}
	defer clear(root)
	key, err := apikey.Mint(root)
	if err != nil {
		return routesync.GroupCommand{}, err
	}
	cmd := routesync.GroupCommand{Group: rec.Group, APIKey: key, Context: &c}
	if !secretAbsent(rec.ManifestKey) {
		manifest, err := normalizeManifestKey(rec.ManifestKey)
		if err != nil {
			return routesync.GroupCommand{}, err
		}
		value, err := inlineSecret("manifest_key", manifest)
		if err != nil {
			return routesync.GroupCommand{}, err
		}
		cmd.KeyPut = &nodegroup.KeyPut{APISecret: secret, ManifestKey: value, ExpiresUnix: time.Now().Add(importPairTTL).Unix()}
	}
	return cmd, nil
}

func sameImportContext(a, b nodegroup.Context) bool {
	_, ar, ae := nodegroup.Normalize(a)
	_, br, be := nodegroup.Normalize(b)
	return ae == nil && be == nil && bytes.Equal(ar, br)
}

func bindingFor(reply registry.NodeGroupResponse, cmd routesync.GroupCommand) importBinding {
	b := importBinding{registration: reply.Registration, version: reply.Ack.GroupResult.Version}
	if cmd.KeyPut != nil {
		b.apiFP, _ = apiSecretFingerprint(cmd.KeyPut.APISecret)
		b.manifestFP, _ = manifestKeyFingerprint(cmd.KeyPut.ManifestKey)
		b.expires = cmd.KeyPut.ExpiresUnix
	}
	return b
}

func (b importBinding) fresh(reply registry.NodeGroupResponse, cmd routesync.GroupCommand) bool {
	if reply.Ack.GroupDetail == nil || cmd.KeyPut == nil {
		return false
	}
	apiFP, _ := apiSecretFingerprint(cmd.KeyPut.APISecret)
	manifestFP, _ := manifestKeyFingerprint(cmd.KeyPut.ManifestKey)
	return b.registration == reply.Registration && b.version == reply.Ack.GroupDetail.Version &&
		b.apiFP == apiFP && b.manifestFP == manifestFP && b.expires > time.Now().Add(importRenewBefore).Unix()
}

// maintainGroup performs at most one CAS. Ambiguity is inspected on the same
// Node before a later attempt; it never rebases a rejected/unknown definition.
func (s *Service) maintainGroup(ctx context.Context, target importTarget, desired routesync.GroupCommand, lease importLeaseToken, apply bool) ImportResult {
	out := ImportResult{SourceID: target.source, NodeID: target.node, Group: target.group}
	fail := func(status, message string) ImportResult { out.Status, out.Error = status, message; return out }
	state, err := s.importState(target)
	if err != nil {
		return fail("unknown", err.Error())
	}
	cmd := desired
	recovering := state.pending != nil
	if recovering {
		cmd = *state.pending
	}
	get := routesync.GroupCommand{Group: cmd.Group, APIKey: cmd.APIKey}
	reply, err := s.callNodeGroup(ctx, target.node, routesync.CmdGroupGet, get, lease)
	if err != nil {
		return fail("unknown", "cannot inspect target Group")
	}
	ack := reply.Ack
	exists := ack.Status == routesync.AckAccepted && ack.GroupDetail != nil
	version := int64(0)
	if exists {
		if ack.GroupDetail.Group != target.group || ack.GroupDetail.Version <= 0 {
			return fail("unknown", "invalid Group response")
		}
		version = ack.GroupDetail.Version
	} else if ack.CurrentGroupVersion != nil {
		version = *ack.CurrentGroupVersion
	}
	out.Version = version
	if recovering {
		if exists && sameImportContext(ack.GroupDetail.Context, *cmd.Context) {
			state.pending = nil
			// A read proves only the definition. The next maintenance round
			// may confirm renewal using a new same-version conditional Put.
			out.Status = "observed"
			return out
		}
		if version != *cmd.ExpectedVersion || (exists && version == 0) {
			state.pending = nil
			return fail("conflict", "Group changed while the previous result was unknown")
		}
		if !exists && ack.HTTPStatus != http.StatusNotFound && !(ack.HTTPStatus == http.StatusForbidden && version == 0 && cmd.KeyPut != nil) {
			return fail("unknown", "cannot establish the original conditional write state")
		}
		if cmd.KeyPut != nil && cmd.KeyPut.ExpiresUnix <= time.Now().Unix() {
			state.pending = nil
			return fail("conflict", "original key lease expired; a new maintenance round is required")
		}
	} else {
		switch {
		case exists:
			equal := sameImportContext(ack.GroupDetail.Context, *cmd.Context)
			if !equal && !apply {
				return fail("conflict", "definition differs; explicit apply is required")
			}
			if equal && (cmd.KeyPut == nil || state.binding.fresh(reply, cmd)) {
				out.Status = "unchanged"
				if cmd.KeyPut != nil {
					out.KeyLeaseConfirmed, out.ExpiresUnix = true, state.binding.expires
				}
				return out
			}
		case ack.HTTPStatus == http.StatusNotFound && version > 0:
			return fail("conflict", "Group was deleted; explicit Put with the tombstone revision is required")
		case ack.HTTPStatus == http.StatusNotFound && ack.CurrentGroupVersion != nil:
			// Authenticated absence (version zero).
		case ack.HTTPStatus == http.StatusForbidden && cmd.KeyPut != nil:
			// A missing pair cannot authorize GET. A trusted combined first
			// Put still verifies the owner, binding and never-existed CAS.
			version = 0
		default:
			return fail("rejected", "Group read rejected")
		}
		cmd.ExpectedVersion = &version
	}
	state.pending = &cmd
	reply, err = s.callNodeGroup(ctx, target.node, routesync.CmdGroupPut, cmd, lease)
	if err != nil {
		return fail("unknown", "Group write result unknown")
	}
	ack = reply.Ack
	if ack.Status != routesync.AckAccepted {
		if ack.HTTPStatus >= 500 {
			return fail("unknown", "Group commit result unknown")
		}
		state.pending = nil
		if ack.CurrentGroupVersion != nil {
			out.Version = *ack.CurrentGroupVersion
		}
		if ack.HTTPStatus == http.StatusConflict {
			return fail("conflict", "conditional Group write rejected")
		}
		return fail("rejected", "Group write rejected")
	}
	if ack.GroupResult == nil || ack.GroupResult.Group != target.group || ack.GroupResult.Version <= 0 {
		return fail("unknown", "missing committed Group result")
	}
	state.binding = bindingFor(reply, cmd)
	state.pending = nil
	out.Status, out.Version = "committed", ack.GroupResult.Version
	if cmd.KeyPut != nil {
		out.KeyLeaseConfirmed, out.ExpiresUnix = true, cmd.KeyPut.ExpiresUnix
	}
	return out
}

func (s *Service) callNodeGroup(ctx context.Context, node string, kind string, cmd routesync.GroupCommand, lease importLeaseToken) (registry.NodeGroupResponse, error) {
	in := registry.NodeGroupRequest{NodeID: node, Kind: kind, Group: cmd, Lease: registry.ImportLease{
		SourceID: lease.sourceID, OwnerID: lease.lease.OwnerID, RunID: lease.lease.RunID, Term: lease.lease.Term,
	}}
	raw, err := json.Marshal(in)
	if err != nil {
		return registry.NodeGroupResponse{}, err
	}
	links := s.scaleLinkLinks(ctx, clusterstate.ImportSourceRoutingKey(lease.sourceID))
	if len(links) == 0 {
		return registry.NodeGroupResponse{}, errors.New("no Registry link")
	}
	// Mutations have exactly one delivery per attempt. Do not replay through
	// another Registry after a timeout; only a subsequent same-Node read may
	// authorize another CAS with the original payload.
	link := links[0]
	reqCtx, cancel := context.WithTimeout(ctx, 7*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, link.BaseURL+registry.PlacerLinkNodeGroupPath, bytes.NewReader(raw))
	if err != nil {
		return registry.NodeGroupResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := link.Client.Do(req)
	if err != nil {
		return registry.NodeGroupResponse{}, errors.New("Group relay unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return registry.NodeGroupResponse{}, fmt.Errorf("Group relay returned %d", resp.StatusCode)
	}
	var out registry.NodeGroupResponse
	if err := clusterclient.DecodeResponse(resp.Body, &out); err != nil {
		return out, err
	}
	if out.NodeID != node || out.Ack == nil || out.Registration.NodeID != node {
		return out, errors.New("invalid Group relay response")
	}
	return out, nil
}

// Apply imports once using explicit one-read/one-CAS updates. The caller must
// first start the Node view and wait for Ready; this never starts the background
// importer or modifies another Placer process.
func (s *Service) Apply(ctx context.Context, emit func(ImportResult)) error {
	if !s.Ready() {
		return errors.New("Node view is not ready")
	}
	return s.importAll(ctx, true, emit)
}

func (s *Service) importAll(ctx context.Context, apply bool, emit func(ImportResult)) error {
	if !s.importMu.TryLock() {
		return errors.New("a Group import pass is already running")
	}
	defer s.importMu.Unlock()
	nodes := s.nodes.values()
	failed := false
	report := func(result ImportResult) {
		if result.Error != "" {
			failed = true
		}
		if emit != nil {
			emit(result)
		}
	}
	for _, source := range s.importSources {
		if !apply && !s.isImportSourceCandidate(source.SourceID) {
			continue
		}
		if err := s.importSource(ctx, source, nodes, apply, report); err != nil {
			report(ImportResult{SourceID: source.SourceID, Status: "unknown", Error: err.Error()})
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	if failed {
		return errors.New("one or more Group targets require attention")
	}
	return nil
}

func (s *Service) importSource(ctx context.Context, source ImportSource, nodes []*registry.NodeRecord, apply bool, emit func(ImportResult)) error {
	lease, ok, err := s.acquireImportSourceLeaseToken(ctx, source.SourceID)
	if err != nil {
		return err
	}
	for !ok && apply {
		delay := time.Until(time.UnixMilli(lease.lease.ExpiresUnixMs))
		if delay < 100*time.Millisecond {
			delay = 100 * time.Millisecond
		}
		if delay > time.Second {
			delay = time.Second
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		lease, ok, err = s.acquireImportSourceLeaseToken(ctx, source.SourceID)
		if err != nil {
			return err
		}
	}
	if !ok {
		if apply {
			return errors.New("import source is leased by another Placer")
		}
		return nil
	}
	cursor := lease.lease.Cursor
	if apply {
		cursor = ""
	} // explicit apply covers the complete source once
	for {
		page, err := source.Importer.Range(ctx, cursor, nodegroup.MaxPageSize)
		if err != nil {
			return err
		}
		if len(page.Groups) > nodegroup.MaxPageSize {
			return errors.New("import source exceeded page budget")
		}
		for _, group := range page.Groups {
			rec, found, err := source.Importer.Read(ctx, group)
			if err != nil {
				emit(ImportResult{SourceID: source.SourceID, Group: group, Status: "rejected", Error: err.Error()})
				continue
			}
			if !found {
				continue
			} // removing input never prunes a Node Group
			if rec.Group != group {
				return errors.New("import source returned a different Group")
			}
			cmd, err := importCommand(rec)
			if err != nil {
				emit(ImportResult{SourceID: source.SourceID, Group: group, Status: "rejected", Error: err.Error()})
				continue
			}
			_, targets := importTargets(group, nodes, rec.NodeSelectors, s.cfg.ShuffleSharding)
			for _, node := range targets {
				if time.Until(time.UnixMilli(lease.lease.ExpiresUnixMs)) < 8*time.Second {
					lease, ok, err = s.acquireImportSourceLeaseToken(ctx, source.SourceID)
					if err != nil {
						return err
					}
					if !ok {
						return errors.New("import source lease was lost")
					}
				}
				// Each target owns its payload, including the fixed expiry.
				emit(s.maintainGroup(ctx, importTarget{source.SourceID, node, group}, cmd, lease, apply))
				if ctx.Err() != nil {
					return ctx.Err()
				}
			}
		}
		complete := page.NextCursor == ""
		if err := s.checkpointImportSource(ctx, source.SourceID, lease, page.NextCursor, complete, ""); err != nil {
			return err
		}
		if complete {
			return nil
		}
		if page.NextCursor == cursor {
			return errors.New("import source did not advance its cursor")
		}
		cursor = page.NextCursor
	}
}
