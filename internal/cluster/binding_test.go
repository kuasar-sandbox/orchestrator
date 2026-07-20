package cluster

import (
	"crypto/sha256"
	"strings"
	"testing"
)

func TestExecutionBindingRoundTripAndDigest(t *testing.T) {
	binding := ExecutionBinding{
		RegistryGeneration: "generation-1",
		Kind:               ExecutionKindSandbox,
		ObjectID:           "sandbox-1",
		Group:              "/acme/dev",
		RouteKey:           "route-1",
		NodeID:             "node-1",
		NodeEpoch:          7,
		DemandDigest:       sha256.Sum256([]byte("demand")),
		DispatchSpecDigest: sha256.Sum256([]byte("dispatch")),
	}
	opaque, err := EncodeExecutionBinding(binding)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeExecutionBinding(opaque)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != binding {
		t.Fatalf("decoded = %+v, want %+v", decoded, binding)
	}
	digest1, err := ExecutionBindingDigest(opaque)
	if err != nil {
		t.Fatal(err)
	}
	digest2, err := ExecutionBindingDigest(opaque)
	if err != nil || digest1 != digest2 || len(digest1) != sha256.Size*2 {
		t.Fatalf("digest = %q/%q err=%v", digest1, digest2, err)
	}
}

func TestExecutionBindingMetadataIsSystemOwned(t *testing.T) {
	binding := ExecutionBinding{
		RegistryGeneration: "generation-1",
		Kind:               ExecutionKindBuild,
		ObjectID:           "build-1",
		Group:              "/acme/dev",
		NodeID:             "node-1",
		NodeEpoch:          9,
		DemandDigest:       sha256.Sum256([]byte("demand")),
		DispatchSpecDigest: sha256.Sum256([]byte("dispatch")),
	}
	opaque, err := EncodeExecutionBinding(binding)
	if err != nil {
		t.Fatal(err)
	}
	in := map[string]string{"user": "value", ObjectMetadataKey: "forged"}
	stored, err := WithExecutionBinding(in, opaque)
	if err != nil {
		t.Fatal(err)
	}
	if in[ObjectMetadataKey] != "forged" {
		t.Fatal("input metadata mutated")
	}
	if stored[ObjectMetadataKey] != opaque {
		t.Fatal("authenticated binding did not replace forged metadata")
	}
	public := WithoutSystemMetadata(stored)
	if public["user"] != "value" {
		t.Fatalf("public metadata = %#v", public)
	}
	if _, ok := public[ObjectMetadataKey]; ok {
		t.Fatal("system binding leaked into public metadata")
	}
}

func TestExecutionBindingRejectsMalformedValues(t *testing.T) {
	if _, err := DecodeExecutionBinding("forged"); err == nil {
		t.Fatal("unversioned binding accepted")
	}
	if _, err := DecodeExecutionBinding(ExecutionBindingPrefix + strings.Repeat("A", MaxExecutionBindingSize*2)); err == nil {
		t.Fatal("oversized binding accepted")
	}
	if _, err := EncodeExecutionBinding(ExecutionBinding{Kind: ExecutionKindSandbox}); err == nil {
		t.Fatal("incomplete binding accepted")
	}
}

func TestExecutionBindingRejectsUnprojectableIdentity(t *testing.T) {
	base := ExecutionBinding{
		RegistryGeneration: "g1", Kind: ExecutionKindSandbox, ObjectID: "s1",
		Group: "/g", RouteKey: "rk", NodeID: "n1", NodeEpoch: 1,
	}
	tooLongNode := base
	tooLongNode.NodeID = strings.Repeat("n", MaxExecutionBindingNodeIDSize+1)
	if _, err := EncodeExecutionBinding(tooLongNode); err == nil {
		t.Fatal("unprojectable node ID was accepted")
	}
	tooLongRegistryGeneration := base
	tooLongRegistryGeneration.RegistryGeneration = strings.Repeat("g", MaxExecutionBindingRegistryGenerationIDSize+1)
	if _, err := EncodeExecutionBinding(tooLongRegistryGeneration); err == nil {
		t.Fatal("unprojectable Registry History Generation was accepted")
	}
}
