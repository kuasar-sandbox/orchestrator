package types

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
)

// ExportResult keeps the established token/template result and adds the final
// publication topology. The references describe an operation, not GC authority.
type ExportResult struct {
	Result string `json:"result"`
	artifact.PublishReport
}

// UnmarshalJSON keeps publication presence rules at the platform boundary.
// In particular, a legacy roots-only response is normalized by the artifact
// decoder, whereas explicit refs:false may never carry an operation delta.
func (r *ExportResult) UnmarshalJSON(raw []byte) error {
	result, fields, err := decodeExportEnvelope(raw)
	if err != nil {
		return err
	}
	role := artifact.RoleSandbox
	if _, present := fields["snapshotRef"]; present {
		role = artifact.RoleSnapshot
	}
	data, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	report, err := artifact.DecodePublishReport(data, role)
	if err != nil {
		return err
	}
	out := ExportResult{Result: result, PublishReport: report}
	if err := out.Validate(); err != nil {
		return err
	}
	*r = out
	return nil
}

// WorkspaceExportResult pairs an authenticated complete disk token with the
// references produced by the same export. The disk's ordering stays in kwt1.
type WorkspaceExportResult struct {
	Result string `json:"result"`
	artifact.DiskReport
}

func (r WorkspaceExportResult) Validate() error {
	if !strings.HasPrefix(r.Result, "kwt1.") || len(r.Result) <= len("kwt1.") || strings.ContainsAny(r.Result, " \t\r\n") {
		return errors.New("workspace export result: invalid token")
	}
	return r.DiskReport.Validate()
}

func (r *WorkspaceExportResult) UnmarshalJSON(raw []byte) error {
	result, fields, err := decodeExportEnvelope(raw)
	if err != nil {
		return err
	}
	data, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	report, err := artifact.DecodeDiskReport(data)
	if err != nil {
		return err
	}
	out := WorkspaceExportResult{Result: result, DiskReport: report}
	if err := out.Validate(); err != nil {
		return err
	}
	*r = out
	return nil
}

func decodeExportEnvelope(raw []byte) (string, map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := strictjson.Decode(raw, &fields); err != nil {
		return "", nil, err
	}
	var result string
	value, present := fields["result"]
	if !present || json.Unmarshal(value, &result) != nil {
		return "", nil, errors.New("export result: missing or invalid result")
	}
	delete(fields, "result")
	return result, fields, nil
}

func (r ExportResult) Validate() error {
	if strings.TrimSpace(r.Result) == "" || strings.TrimSpace(r.Result) != r.Result || strings.ContainsAny(r.Result, "\r\n") {
		return errors.New("export result: missing or invalid result")
	}
	role := artifact.RoleSandbox
	root := r.SandboxRef
	if r.SnapshotRef != "" {
		role, root = artifact.RoleSnapshot, r.SnapshotRef
	}
	if err := r.PublishReport.Validate(role); err != nil {
		return err
	}
	if strings.HasPrefix(r.Result, "kmt1.") {
		return nil
	}
	template, err := ParseTemplateID(r.Result)
	if err != nil {
		return errors.New("export result: invalid token or template")
	}
	expected := KindSbx
	if role == artifact.RoleSnapshot {
		expected = KindSnp
	}
	if template.Kind != expected || template.Ref != root {
		return errors.New("export result: template root mismatch")
	}
	return nil
}
