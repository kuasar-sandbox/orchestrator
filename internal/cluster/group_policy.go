package cluster

import "errors"

// ResolveTemplateRef applies the group default and the explicit caller-override
// policy. The returned value is the sole effective template persisted in a
// Sandbox dispatch spec.
func ResolveTemplateRef(group SandboxGroup, requested string) (string, error) {
	if group.TemplateRef == "" {
		return "", errors.New("cluster: Sandbox group requires a default template_ref")
	}
	if requested == "" || requested == group.TemplateRef {
		return group.TemplateRef, nil
	}
	if !group.AllowTemplateOverride {
		return "", errors.New("cluster: caller template override is disabled for this group")
	}
	return requested, nil
}
