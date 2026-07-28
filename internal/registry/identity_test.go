package registry

import "testing"

func TestEncodeNodeSandboxID(t *testing.T) {
	for _, tc := range []struct {
		generation uint64
		want       string
	}{
		{generation: 0, want: "sb-0123456789abcdef-g0"},
		{generation: 1, want: "sb-0123456789abcdef-g1"},
		{generation: 42, want: "sb-0123456789abcdef-g42"},
	} {
		if got := EncodeNodeSandboxID("sb-0123456789abcdef", tc.generation); got != tc.want {
			t.Fatalf("EncodeNodeSandboxID(%d)=%q, want %q", tc.generation, got, tc.want)
		}
	}
}

func TestValidNodeSandboxIdentity(t *testing.T) {
	stable := "sb-0123456789abcdef0123456789abcdef"
	if !validNodeSandboxIdentity(stable, stable+"-g0", 0) {
		t.Fatal("canonical g0 identity was rejected")
	}
	maxNodeID := EncodeNodeSandboxID(stable, ^uint64(0))
	if !validNodeSandboxIdentity(stable, maxNodeID, ^uint64(0)) {
		t.Fatal("canonical maximum uint64 identity was rejected")
	}
	for _, invalid := range []struct {
		stable     string
		node       string
		generation uint64
	}{
		{stable: stable, node: stable + "-g00", generation: 0},
		{stable: stable, node: stable + "-g1", generation: 0},
		{stable: stable + "-g0", node: stable + "-g0", generation: 0},
	} {
		if validNodeSandboxIdentity(invalid.stable, invalid.node, invalid.generation) {
			t.Fatalf("invalid identity accepted: %+v", invalid)
		}
	}
}
