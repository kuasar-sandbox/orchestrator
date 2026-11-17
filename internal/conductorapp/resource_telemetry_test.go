package conductorapp

import (
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
)

func TestResourceGroupsDoNotInventSandboxIdentityForNode(t *testing.T) {
	st := nodectl.NewState(8<<30, 8, 0, 0, nodectl.Watermarks{})
	groups := hostResourceGroups(st, time.Now())
	if len(groups) < 1 {
		t.Fatal("no node resource group")
	}
	if groups[0].Resource != nil {
		t.Fatalf("node metrics fabricated sandbox identity: %+v", groups[0].Resource)
	}
	if len(groups[0].Samples) == 0 {
		t.Fatal("no node resource samples")
	}
	for _, group := range groups {
		if group.Resource["sandbox.id"] != "" && group.Resource["sandbox.stable_id"] != "" {
			t.Fatal("stable id fabricated without domain identity")
		}
	}
}
