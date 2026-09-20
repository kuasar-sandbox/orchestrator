package types

import (
	"errors"
	"strings"

	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
)

// ExportResult keeps the established token/template result and adds the final
// publication topology. The references describe an operation, not GC authority.
type ExportResult struct {
	Result string `json:"result"`
	artifact.PublishReport
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
