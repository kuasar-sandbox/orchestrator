package raftstore

import (
	"errors"
	"testing"
)

func TestRuntimeAuthorizeServeFailsClosedWithoutPermit(t *testing.T) {
	runtime := &Runtime{permitCache: NewPermitCache(nil)}
	if err := runtime.AuthorizeServe(PermitIdentity{}, PermitRegistryRead); !errors.Is(err, ErrPermitMissing) && err == nil {
		t.Fatalf("AuthorizeServe error = %v", err)
	}
}

func TestCloneRegistryLayoutDoesNotShareMutableState(t *testing.T) {
	source := testRegistryLayout(2, "generation-clone")
	clone := CloneRegistryLayout(source)
	clone.Members[0].MemberID = "changed"
	clone.SystemReplicas[0].ReplicaID++
	clone.DataShards[0].Replicas[0].ReplicaID++
	if source.Members[0].MemberID == "changed" ||
		source.SystemReplicas[0].ReplicaID == clone.SystemReplicas[0].ReplicaID ||
		source.DataShards[0].Replicas[0].ReplicaID == clone.DataShards[0].Replicas[0].ReplicaID {
		t.Fatal("registryLayout clone shares mutable slices")
	}
}
