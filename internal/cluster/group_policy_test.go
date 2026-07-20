package cluster

import "testing"

func TestResolveTemplateRefUsesDefaultAndGatesCallerOverride(t *testing.T) {
	group := SandboxGroup{TemplateRef: "e2b-img-default"}
	for _, requested := range []string{"", group.TemplateRef} {
		got, err := ResolveTemplateRef(group, requested)
		if err != nil || got != group.TemplateRef {
			t.Fatalf("requested %q resolved to %q, %v", requested, got, err)
		}
	}
	if _, err := ResolveTemplateRef(group, "e2b-img-caller"); err == nil {
		t.Fatal("caller override accepted while disabled")
	}
	group.AllowTemplateOverride = true
	if got, err := ResolveTemplateRef(group, "e2b-img-caller"); err != nil || got != "e2b-img-caller" {
		t.Fatalf("enabled override resolved to %q, %v", got, err)
	}
	group.TemplateRef = ""
	if _, err := ResolveTemplateRef(group, "e2b-img-caller"); err == nil {
		t.Fatal("group without a default template accepted")
	}
}
