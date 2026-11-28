package metastore

import (
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	records "github.com/kuasar-sandbox/orchestrator/internal/storage/record"
)

func (s *Store) guardGroupAdmission(m *mutation, selected metadata.GroupAdmission, group, nodeID, authRef string, revision int64, template, source string) error {
	if selected.Group != group || selected.Registration.NodeID != nodeID || selected.AuthRef != authRef || selected.GroupVersion != revision || revision <= 0 {
		return invalidRecord("Group intent identity")
	}
	if !selected.SupportVersion.IsCommitted() || !selected.PublicationVersion.IsCommitted() {
		return invalidRecord("Group selection versions")
	}
	if err := s.guardRegistration(m, selected.Registration); err != nil {
		return err
	}
	// Profile eligibility is current policy, not a selected revision. A heartbeat
	// watermark can change during a definitely aborted physical attempt without
	// changing any Group, registration or object condition below.
	if err := m.tr.CheckCurrent(s.profileKey(nodeID), func(raw records.Value) error {
		profile, err := typed[metadata.NodeProfileRecord](wrapRecord(raw))
		if err != nil {
			return err
		}
		if !profile.Exists || !selected.AllowsNode(profile.Value, template) {
			return metadata.ErrConflict
		}
		return nil
	}); err != nil {
		return err
	}
	raw, err := m.expect(s.groupSupportKey(group, nodeID), selected.SupportVersion)
	if err != nil {
		return err
	}
	row, err := typed[metadata.GroupSupportRecord](raw)
	if err != nil {
		return err
	}
	raw, err = m.expect(s.groupPublicationKey(nodeID), selected.PublicationVersion)
	if err != nil {
		return err
	}
	pub, err := typed[metadata.GroupPublication](raw)
	if err != nil {
		return err
	}
	if !row.Exists || !pub.Exists || !pub.Value.Published || row.Value.Registration != selected.Registration || pub.Value.Registration != selected.Registration || row.Value.SyncID != pub.Value.SyncID || row.Value.GroupVersion != revision || row.Value.Group != group {
		return metadata.ErrConflict
	}
	matched := false
	for _, entry := range row.Value.Entries {
		matched = matched || entry.AuthRef == authRef
	}
	if !matched {
		return metadata.ErrOwnershipConflict
	}
	switch source {
	case "default":
		if row.Value.DefaultContext == nil || row.Value.DefaultContext.TemplateID != template {
			return metadata.ErrConflict
		}
	case "explicit":
		if template == "" {
			return invalidRecord("explicit Group template is missing")
		}
	case "": // Build admission has no default SNP runtime constraint.
		if template != "" {
			return invalidRecord("Group template source is missing")
		}
	default:
		return invalidRecord("Group template source")
	}
	return nil
}
