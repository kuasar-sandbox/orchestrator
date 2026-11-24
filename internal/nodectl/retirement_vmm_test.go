package nodectl

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/sandboxer/pkg/memorycheckpoint"
	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
)

func TestDeferredVMMInventoryDoesNotDuplicateRetainedCharge(t *testing.T) {
	srv, _, _ := committedRetentionFixture(t)
	receipt := srv.State.retainedSnapshot("sid")
	e := receipt.Captures[0]
	child := exec.Command("sleep", "30")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = child.Process.Kill(); _ = child.Wait() })
	writeFakeCgroup(t, e.Source.CgroupPath, "536870912", child.Process.Pid)
	vmm, err := memorycheckpoint.IdentifyVMM(child.Process.Pid, e.Source.CgroupPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"known", "foreign", "unrecognized"} {
		t.Run(scenario, func(t *testing.T) {
			recovered := makeState(8<<30, 0)
			if err := recovered.restoreRetained(receipt); err != nil {
				t.Fatal(err)
			}
			members := fmt.Sprintf("%d\n", child.Process.Pid)
			if scenario == "foreign" {
				members += fmt.Sprintf("%d\n", os.Getpid())
			}
			if err := os.WriteFile(filepath.Join(e.Source.CgroupPath, "cgroup.procs"), []byte(members), 0600); err != nil {
				t.Fatal(err)
			}
			known := map[string]memorycheckpoint.VMMIdentity{}
			if scenario != "unrecognized" {
				known[e.Source.CgroupPath] = vmm
			}
			if err := srv.Inventory.scanCgroups(recovered, nil, known); err != nil {
				t.Fatal(err)
			}
			got := recovered.ResourceSnapshot()
			want := uint64(768 << 20)
			if scenario == "known" {
				want = 256 << 20
			}
			if got.Reserved.MemoryBytes != want {
				t.Fatalf("charge=%d want %d", got.Reserved.MemoryBytes, want)
			}
		})
	}
}
func TestDeferredVMMCapabilityEcho(t *testing.T) {
	if resource.HasFeature(featureResponse([]string{resource.FeatureRetainedMemoryV2}), resource.FeatureDeferredVMMExit) {
		t.Fatal("unsolicited capability")
	}
	if !resource.HasFeature(featureResponse([]string{resource.FeatureRetainedMemoryV2, resource.FeatureDeferredVMMExit}), resource.FeatureDeferredVMMExit) {
		t.Fatal("missing capability")
	}
}
