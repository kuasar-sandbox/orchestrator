package routesync

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestRecoveryReportPageEnforcesRequestedBounds(t *testing.T) {
	request := RecoveryReportRequest{
		RecoveryEpoch: 1, SourceClusterID: "cluster-1",
		SourceRegistryGeneration: "generation-1", SourceRegistryLayoutDigest: strings.Repeat("a", 64),
		TargetRegistryGeneration: "generation-2", TargetRegistryLayoutDigest: strings.Repeat("b", 64),
		Limit: 1, MaxBytes: 1,
	}
	page := RecoveryReportPage{
		RecoveryEpoch: request.RecoveryEpoch, SourceClusterID: request.SourceClusterID,
		SourceRegistryGeneration: request.SourceRegistryGeneration, SourceRegistryLayoutDigest: request.SourceRegistryLayoutDigest,
		TargetRegistryGeneration: request.TargetRegistryGeneration, TargetRegistryLayoutDigest: request.TargetRegistryLayoutDigest,
		NodeID: "node-1", NodeEpoch: 7, SessionSeq: 11, ReportDigest: strings.Repeat("c", 64),
		Complete: true, Objects: []RecoveryExecutionFact{},
	}
	if encoded, err := json.Marshal(page.Objects); err != nil || len(encoded) <= int(request.MaxBytes) {
		t.Fatalf("test page encoding = %q, %v", encoded, err)
	}
	if err := page.ValidateFor(request, page.NodeID, page.NodeEpoch, page.SessionSeq); err == nil {
		t.Fatal("recovery page exceeded the negotiated byte limit")
	}
}
