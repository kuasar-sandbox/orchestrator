package metastore

import (
	"bytes"
	"context"
	"encoding/json"
	"sort"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	records "github.com/kuasar-sandbox/orchestrator/internal/storage/record"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type groupProgress struct {
	Registration  metadata.Registration `json:"registration"`
	SyncID        string                `json:"sync_id"`
	Sequence      int64                 `json:"sequence"`
	Total         int                   `json:"total"`
	Received      int                   `json:"received"`
	Closing       bool                  `json:"closing"`
	CleanupCursor metadata.Cursor       `json:"cleanup_cursor,omitempty"`
}
type groupIndex struct {
	NodeID string `json:"node_id"`
	Group  string `json:"group"`
}

func (s *Store) groupPublicationKey(node string) records.Key { return s.key("group-publication", node) }
func (s *Store) groupProgressKey(node string) records.Key    { return s.key("group-progress", node) }
func (s *Store) groupSupportScope(group string) records.Key  { return s.key("group-support", group) }
func (s *Store) groupSupportKey(group, node string) records.Key {
	return mainKey(s.groupSupportScope(group), node)
}
func (s *Store) groupNodeScope(node string) records.Key { return s.key("group-node-index", node) }
func (s *Store) groupNodeKey(node, group string) records.Key {
	return mainKey(s.groupNodeScope(node), group)
}

func normalizeGroupSupport(value nodegroup.Support) (nodegroup.Support, error) {
	if err := nodegroup.ValidateName(value.Group); err != nil {
		return nodegroup.Support{}, err
	}
	if value.Version <= 0 || len(value.Entries) == 0 || len(value.Entries) > metadata.MaxGroupSupportBatchEntries {
		return nodegroup.Support{}, invalidRecord("Group support revision or entries")
	}
	if value.DefaultContext != nil {
		if _, err := types.ParseTemplateID(value.DefaultContext.TemplateID); err != nil {
			return nodegroup.Support{}, invalidRecord("Group default template")
		}
		copy := *value.DefaultContext
		value.DefaultContext = &copy
	}
	value.Entries = append([]nodegroup.Entry(nil), value.Entries...)
	sort.Slice(value.Entries, func(i, j int) bool { return value.Entries[i].AuthRef < value.Entries[j].AuthRef })
	for i, e := range value.Entries {
		if !validGroupAuth(e.AuthRef, e.AuthMaterial.APISecret) || e.Routing.TargetPort < 0 || e.Routing.TargetPort > 65535 || (i > 0 && e.AuthRef == value.Entries[i-1].AuthRef) {
			return nodegroup.Support{}, invalidRecord("Group support authentication or routing")
		}
	}
	if err := nodegroup.ValidateSupportBudget(value); err != nil {
		return nodegroup.Support{}, err
	}
	return value, nil
}

func projectGroupSupport(reg metadata.Registration, syncID string, value nodegroup.Support) metadata.GroupSupportRecord {
	out := metadata.GroupSupportRecord{Registration: reg, SyncID: syncID, Group: value.Group, GroupVersion: value.Version, DefaultContext: value.DefaultContext}
	for _, e := range value.Entries {
		out.Entries = append(out.Entries, metadata.GroupSupportEntry{AuthRef: e.AuthRef, Routing: e.Routing})
	}
	return out
}

func (s *Store) writeGroupSupport(m *mutation, reg metadata.Registration, syncID string, value nodegroup.Support) error {
	key := s.groupSupportKey(value.Group, reg.NodeID)
	old, err := getTyped[metadata.GroupSupportRecord](m, key)
	if err != nil {
		return err
	}
	next := projectGroupSupport(reg, syncID, value)
	keep := make(map[string]bool, len(value.Entries))
	for _, entry := range value.Entries {
		keep[entry.AuthRef] = true
		if err := s.retainGroupAuth(m, metadata.GroupAuthReference{Group: value.Group, AuthRef: entry.AuthRef, Kind: "support", ID: reg.NodeID}, entry.AuthMaterial.APISecret); err != nil {
			return err
		}
	}
	if old.Exists {
		if old.Value.Group != value.Group || old.Value.Registration.NodeID != reg.NodeID {
			return invalidRecord("Group support identity")
		}
		for _, entry := range old.Value.Entries {
			if !keep[entry.AuthRef] {
				if err := s.releaseGroupAuth(m, metadata.GroupAuthReference{Group: value.Group, AuthRef: entry.AuthRef, Kind: "support", ID: reg.NodeID}); err != nil {
					return err
				}
			}
		}
	}
	if err := putJSON(m.tr, key, next); err != nil {
		return err
	}
	return putJSON(m.tr, s.groupNodeKey(reg.NodeID, value.Group), groupIndex{NodeID: reg.NodeID, Group: value.Group})
}

func (s *Store) removeGroupSupport(m *mutation, node, group string) error {
	old, err := getTyped[metadata.GroupSupportRecord](m, s.groupSupportKey(group, node))
	if err != nil {
		return err
	}
	if old.Exists {
		if old.Value.Group != group || old.Value.Registration.NodeID != node {
			return invalidRecord("Group support removal identity")
		}
		for _, entry := range old.Value.Entries {
			if err := s.releaseGroupAuth(m, metadata.GroupAuthReference{Group: group, AuthRef: entry.AuthRef, Kind: "support", ID: node}); err != nil {
				return err
			}
		}
		clearRecord(m.tr, s.groupSupportKey(group, node))
	}
	clearRecord(m.tr, s.groupNodeKey(node, group))
	return nil
}

func (s *Store) BeginGroupSupport(ctx context.Context, reg metadata.Registration, syncID string, sequence int64, total int) error {
	if err := validateParts(reg.NodeID, syncID); err != nil {
		return err
	}
	if len(syncID) > 128 || sequence < 0 || total < 0 {
		return invalidRecord("Group snapshot boundary")
	}
	_, err := s.mutate(ctx, func(m *mutation) (bool, error) {
		if err := s.guardRegistration(m, reg); err != nil {
			return false, err
		}
		pub, err := getTyped[metadata.GroupPublication](m, s.groupPublicationKey(reg.NodeID))
		if err != nil {
			return false, err
		}
		progress, err := getTyped[groupProgress](m, s.groupProgressKey(reg.NodeID))
		if err != nil {
			return false, err
		}
		if pub.Exists && pub.Value.Registration == reg {
			if !progress.Exists || progress.Value.Registration != reg || progress.Value.SyncID != pub.Value.SyncID {
				return false, invalidRecord("Group progress identity")
			}
			if pub.Value.SyncID == syncID {
				if pub.Value.SnapshotSequence != sequence || progress.Value.Total != total {
					return false, metadata.ErrGroupResync
				}
				return false, nil
			}
			if sequence < progress.Value.Sequence {
				return false, metadata.ErrGroupResync
			}
		}
		if err := putJSON(m.tr, s.groupPublicationKey(reg.NodeID), metadata.GroupPublication{Registration: reg, SyncID: syncID, SnapshotSequence: sequence}); err != nil {
			return false, err
		}
		return true, putJSON(m.tr, s.groupProgressKey(reg.NodeID), groupProgress{Registration: reg, SyncID: syncID, Sequence: sequence, Total: total})
	})
	return err
}

func (s *Store) groupStream(m *mutation, reg metadata.Registration, syncID string) (metadata.Versioned[metadata.GroupPublication], metadata.Versioned[groupProgress], error) {
	if err := s.guardRegistration(m, reg); err != nil {
		return metadata.Versioned[metadata.GroupPublication]{}, metadata.Versioned[groupProgress]{}, err
	}
	pub, err := getTyped[metadata.GroupPublication](m, s.groupPublicationKey(reg.NodeID))
	if err != nil {
		return pub, metadata.Versioned[groupProgress]{}, err
	}
	progress, err := getTyped[groupProgress](m, s.groupProgressKey(reg.NodeID))
	if err != nil {
		return pub, progress, err
	}
	if !pub.Exists || !progress.Exists || pub.Value.Registration != reg || progress.Value.Registration != reg || pub.Value.SyncID != progress.Value.SyncID || (syncID != "" && pub.Value.SyncID != syncID) {
		return pub, progress, metadata.ErrGroupResync
	}
	return pub, progress, nil
}

func (s *Store) WriteGroupSupportBatch(ctx context.Context, reg metadata.Registration, syncID string, offset int, values []nodegroup.Support) error {
	if len(values) == 0 || len(values) > metadata.MaxGroupSupportBatchRows || offset < 0 {
		return invalidRecord("Group batch size or offset")
	}
	batch := make([]nodegroup.Support, len(values))
	seen := map[string]bool{}
	entries := 0
	for i, v := range values {
		normalized, err := normalizeGroupSupport(v)
		if err != nil {
			return err
		}
		if seen[v.Group] {
			return invalidRecord("duplicate Group in snapshot batch")
		}
		seen[v.Group] = true
		batch[i] = normalized
		entries += len(v.Entries)
	}
	raw, _ := json.Marshal(batch)
	if len(raw) > metadata.MaxGroupSupportBatchBytes || entries > metadata.MaxGroupSupportBatchEntries {
		return nodegroup.ErrTooLarge
	}
	_, err := s.mutate(ctx, func(m *mutation) (bool, error) {
		pub, progress, err := s.groupStream(m, reg, syncID)
		if err != nil {
			return false, err
		}
		p := progress.Value
		if pub.Value.Published || p.Closing || offset > p.Received || offset > p.Total || len(batch) > p.Total-offset {
			return false, metadata.ErrGroupResync
		}
		for _, value := range batch {
			if value.Version > pub.Value.SnapshotSequence {
				return false, metadata.ErrGroupResync
			}
			old, err := getTyped[metadata.GroupSupportRecord](m, s.groupSupportKey(value.Group, reg.NodeID))
			if err != nil {
				return false, err
			}
			if offset < p.Received {
				// The previous batch may have committed without an ACK. Its exact
				// rows must already exist; do not advance the count a second time.
				want, _ := json.Marshal(projectGroupSupport(reg, syncID, value))
				got, _ := json.Marshal(old.Value)
				if offset+len(batch) > p.Received || !old.Exists || !bytes.Equal(got, want) {
					return false, metadata.ErrGroupResync
				}
				continue
			}
			if old.Exists && old.Value.Registration == reg && old.Value.SyncID == syncID {
				return false, metadata.ErrGroupResync
			}
			if err := s.writeGroupSupport(m, reg, syncID, value); err != nil {
				return false, err
			}
		}
		if offset < p.Received {
			return false, nil
		}
		p.Received += len(batch)
		return true, putJSON(m.tr, s.groupProgressKey(reg.NodeID), p)
	})
	return err
}

// FinishGroupSupport performs one bounded cleanup page. The caller repeats it
// until complete; no candidate can observe a partial snapshot as published.
func (s *Store) FinishGroupSupport(ctx context.Context, reg metadata.Registration, syncID string) (complete bool, err error) {
	_, err = s.mutate(ctx, func(m *mutation) (bool, error) {
		complete = false
		pub, progress, err := s.groupStream(m, reg, syncID)
		if err != nil {
			return false, err
		}
		if pub.Value.Published {
			complete = true
			return false, nil
		}
		p := progress.Value
		if p.Received != p.Total {
			return false, metadata.ErrGroupResync
		}
		scope := s.groupNodeScope(reg.NodeID)
		after, err := decodeCursor(scope, p.CleanupCursor)
		if err != nil {
			return false, err
		}
		page, err := m.tr.Scan(scope, after, pageLimit(metadata.MaxGroupSupportBatchRows))
		if err != nil {
			return false, err
		}
		processed, entries := 0, 0
		for _, item := range page.Items {
			index, err := typed[groupIndex](wrapRecord(item.Value))
			if err != nil {
				return false, err
			}
			if index.Value.NodeID != reg.NodeID {
				return false, invalidRecord("Group reverse index")
			}
			row, err := getTyped[metadata.GroupSupportRecord](m, s.groupSupportKey(index.Value.Group, reg.NodeID))
			if err != nil {
				return false, err
			}
			if processed > 0 && entries+len(row.Value.Entries) > metadata.MaxGroupSupportBatchEntries {
				break
			}
			entries += len(row.Value.Entries)
			if !row.Exists || row.Value.Registration != reg || row.Value.SyncID != syncID {
				if err := s.removeGroupSupport(m, reg.NodeID, index.Value.Group); err != nil {
					return false, err
				}
			}
			processed++
		}
		p.Closing = true
		if page.More || processed < len(page.Items) {
			if processed == 0 {
				return false, invalidRecord("empty Group cleanup continuation")
			}
			p.CleanupCursor = encodeCursor(scope, page.Items[processed-1].Key)
		} else {
			pub.Value.Published = true
			if err := putJSON(m.tr, s.groupPublicationKey(reg.NodeID), pub.Value); err != nil {
				return false, err
			}
			p.CleanupCursor = ""
			complete = true
		}
		return true, putJSON(m.tr, s.groupProgressKey(reg.NodeID), p)
	})
	return complete && err == nil, err
}

func (s *Store) ApplyGroupSupport(ctx context.Context, reg metadata.Registration, sequence int64, value *nodegroup.Support, deleted string) error {
	if sequence <= 0 || (value == nil) == (deleted == "") {
		return invalidRecord("Group incremental report")
	}
	var next nodegroup.Support
	if value != nil {
		var err error
		next, err = normalizeGroupSupport(*value)
		if err != nil {
			return err
		}
		if next.Version != sequence {
			return invalidRecord("Group incremental revision")
		}
	} else if err := nodegroup.ValidateName(deleted); err != nil {
		return err
	}
	_, err := s.mutate(ctx, func(m *mutation) (bool, error) {
		pub, progress, err := s.groupStream(m, reg, "")
		if err != nil {
			return false, err
		}
		if !pub.Value.Published {
			return false, metadata.ErrGroupResync
		}
		if sequence <= progress.Value.Sequence {
			return false, nil
		}
		if sequence-1 != progress.Value.Sequence {
			return false, metadata.ErrGroupResync
		}
		if value != nil {
			if err := s.writeGroupSupport(m, reg, pub.Value.SyncID, next); err != nil {
				return false, err
			}
		} else if err := s.removeGroupSupport(m, reg.NodeID, deleted); err != nil {
			return false, err
		}
		progress.Value.Sequence = sequence
		return true, putJSON(m.tr, s.groupProgressKey(reg.NodeID), progress.Value)
	})
	return err
}

func (s *Store) readGroupSelection(tr *records.Tx, group, node string) (metadata.GroupSupportSelection, error) {
	row, err := readTyped[metadata.GroupSupportRecord](tr, s.groupSupportKey(group, node))
	if err != nil || !row.Exists {
		return metadata.GroupSupportSelection{}, err
	}
	if row.Value.Group != group || row.Value.Registration.NodeID != node {
		return metadata.GroupSupportSelection{}, invalidRecord("Group candidate identity")
	}
	related, err := tr.GetMany([]records.Key{s.groupPublicationKey(node), s.profileKey(node), s.lifecycleKey(node)})
	if err != nil {
		return metadata.GroupSupportSelection{}, err
	}
	return groupSelection(row, related)
}

func groupSelection(row metadata.Versioned[metadata.GroupSupportRecord], related []records.Value) (metadata.GroupSupportSelection, error) {
	pub, err := typed[metadata.GroupPublication](wrapRecord(related[0]))
	if err != nil {
		return metadata.GroupSupportSelection{}, err
	}
	if !pub.Exists || !pub.Value.Published || pub.Value.Registration != row.Value.Registration || pub.Value.SyncID != row.Value.SyncID {
		return metadata.GroupSupportSelection{}, nil
	}
	profile, err := typed[metadata.NodeProfileRecord](wrapRecord(related[1]))
	if err != nil {
		return metadata.GroupSupportSelection{}, err
	}
	life, err := typed[lifecycle](wrapRecord(related[2]))
	if err != nil {
		return metadata.GroupSupportSelection{}, err
	}
	snapshot, err := nodeSnapshot(profile, life)
	if err != nil {
		return metadata.GroupSupportSelection{}, err
	}
	if !snapshot.Exists || snapshot.Reaping || snapshot.Registration != row.Value.Registration {
		return metadata.GroupSupportSelection{}, nil
	}
	return metadata.GroupSupportSelection{Support: row, Publication: pub, Node: snapshot}, nil
}

func (s *Store) ReadGroupSupport(ctx context.Context, group, node string) (out metadata.GroupSupportSelection, err error) {
	if err = nodegroup.ValidateName(group); err != nil {
		return
	}
	if err = validateParts(node); err != nil {
		return
	}
	err = s.read(ctx, func(tr *records.Tx) error {
		var err error
		out, err = s.readGroupSelection(tr, group, node)
		return err
	})
	return
}

func (s *Store) ListGroupSupport(ctx context.Context, group string, cursor metadata.Cursor, limit int) (out metadata.Page[metadata.GroupSupportSelection], err error) {
	if err = nodegroup.ValidateName(group); err != nil {
		return
	}
	if limit < 1 || limit > metadata.MaxGroupSupportBatchRows {
		return out, invalidRecord("Group page limit")
	}
	scope := s.groupSupportScope(group)
	after, err := decodeCursor(scope, cursor)
	if err != nil {
		return out, err
	}
	err = s.read(ctx, func(tr *records.Tx) error {
		out = metadata.Page[metadata.GroupSupportSelection]{}
		page, err := tr.Scan(scope, after, pageLimit(limit))
		if err != nil {
			return err
		}
		// Keep the scanned rows and batch their dependent records inside this
		// same snapshot. Per-candidate reads otherwise add four sequential
		// database round trips for each Node in the bounded page.
		rows := make([]metadata.Versioned[metadata.GroupSupportRecord], len(page.Items))
		keys := make([]records.Key, 0, 3*len(page.Items))
		for i, item := range page.Items {
			row, err := typed[metadata.GroupSupportRecord](wrapRecord(item.Value))
			if err != nil {
				return err
			}
			node := row.Value.Registration.NodeID
			if !row.Exists || row.Value.Group != group || node == "" || !bytes.Equal(item.Key, s.groupSupportKey(group, node)) {
				return invalidRecord("Group candidate identity")
			}
			rows[i] = row
			keys = append(keys, s.groupPublicationKey(node), s.profileKey(node), s.lifecycleKey(node))
		}
		related, err := tr.GetMany(keys)
		if err != nil {
			return err
		}
		for i, row := range rows {
			selected, err := groupSelection(row, related[3*i:3*i+3])
			if err != nil {
				return err
			}
			if selected.Support.Exists {
				out.Items = append(out.Items, selected)
			}
		}
		if page.More && len(page.Items) > 0 {
			out.Next = encodeCursor(scope, page.Items[len(page.Items)-1].Key)
		}
		return nil
	})
	return
}

func (s *Store) CleanupGroupSupport(ctx context.Context, claim metadata.ReapClaim) (complete bool, err error) {
	_, err = s.mutate(ctx, func(m *mutation) (bool, error) {
		complete = false
		if err := s.guardReap(m, claim); err != nil {
			return false, err
		}
		page, err := m.tr.Scan(s.groupNodeScope(claim.NodeID), nil, pageLimit(metadata.MaxGroupSupportBatchRows))
		if err != nil {
			return false, err
		}
		processed, entries := 0, 0
		for _, item := range page.Items {
			index, err := typed[groupIndex](wrapRecord(item.Value))
			if err != nil {
				return false, err
			}
			if index.Value.NodeID != claim.NodeID {
				return false, invalidRecord("Group cleanup reverse identity")
			}
			row, err := getTyped[metadata.GroupSupportRecord](m, s.groupSupportKey(index.Value.Group, claim.NodeID))
			if err != nil {
				return false, err
			}
			if processed > 0 && entries+len(row.Value.Entries) > metadata.MaxGroupSupportBatchEntries {
				break
			}
			entries += len(row.Value.Entries)
			if err := s.removeGroupSupport(m, claim.NodeID, index.Value.Group); err != nil {
				return false, err
			}
			processed++
		}
		if !page.More && processed == len(page.Items) {
			clearRecord(m.tr, s.groupPublicationKey(claim.NodeID))
			clearRecord(m.tr, s.groupProgressKey(claim.NodeID))
			complete = true
		}
		return true, nil
	})
	return complete && err == nil, err
}
