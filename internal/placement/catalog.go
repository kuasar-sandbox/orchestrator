package placement

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"unicode/utf8"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
)

const (
	CatalogSnapshotVersion uint8  = 1
	MaxCatalogNodes        uint32 = 100_000
)

// CatalogReference identifies one complete, immutable projection of the
// System Group's low-frequency Node Catalog.
type CatalogReference struct {
	Version              uint8  `json:"version"`
	ClusterID            string `json:"cluster_id"`
	RegistryGeneration   string `json:"registry_generation"`
	SystemEpoch          uint64 `json:"system_epoch"`
	RegistryLayoutDigest string `json:"registry_layout_digest"`
	Revision             uint64 `json:"revision"`
	NodeCount            uint32 `json:"node_count"`
	Digest               string `json:"digest"`
}

func (r CatalogReference) Validate() error {
	if r.Version != CatalogSnapshotVersion || r.ClusterID == "" || r.RegistryGeneration == "" ||
		r.SystemEpoch == 0 || r.RegistryLayoutDigest == "" || r.NodeCount > MaxCatalogNodes {
		return errors.New("placement: incomplete Node Catalog reference")
	}
	digest, err := hex.DecodeString(r.Digest)
	if err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != r.Digest {
		return errors.New("placement: Node Catalog digest must be canonical SHA-256 hex")
	}
	return nil
}

type CatalogSnapshot struct {
	Reference CatalogReference `json:"reference"`
	Nodes     []CatalogNode    `json:"nodes"`
}

func NewCatalogSnapshot(
	clusterID string,
	registryGeneration string,
	systemEpoch uint64,
	registryLayoutDigest string,
	revision uint64,
	nodes []CatalogNode,
) (CatalogSnapshot, error) {
	if uint64(len(nodes)) > uint64(MaxCatalogNodes) {
		return CatalogSnapshot{}, fmt.Errorf("placement: Node Catalog exceeds %d nodes", MaxCatalogNodes)
	}
	copyNodes := cloneCatalogNodes(nodes)
	sort.Slice(copyNodes, func(i, j int) bool { return copyNodes[i].NodeID < copyNodes[j].NodeID })
	reference := CatalogReference{
		Version: CatalogSnapshotVersion, ClusterID: clusterID, RegistryGeneration: registryGeneration,
		SystemEpoch: systemEpoch, RegistryLayoutDigest: registryLayoutDigest,
		Revision: revision, NodeCount: uint32(len(copyNodes)),
	}
	digest, err := catalogSnapshotDigest(reference, copyNodes)
	if err != nil {
		return CatalogSnapshot{}, err
	}
	reference.Digest = digest
	snapshot := CatalogSnapshot{Reference: reference, Nodes: copyNodes}
	return snapshot, snapshot.Validate()
}

func (s CatalogSnapshot) Validate() error {
	if err := s.Reference.Validate(); err != nil {
		return err
	}
	if len(s.Nodes) != int(s.Reference.NodeCount) {
		return errors.New("placement: Node Catalog count does not match its reference")
	}
	previous := ""
	for index, node := range s.Nodes {
		if err := validateCatalogNode(node); err != nil {
			return fmt.Errorf("placement: invalid Node Catalog row %d: %w", index, err)
		}
		if index != 0 && node.NodeID <= previous {
			return errors.New("placement: Node Catalog rows are not uniquely sorted")
		}
		previous = node.NodeID
	}
	digest, err := catalogSnapshotDigest(s.Reference, s.Nodes)
	if err != nil {
		return err
	}
	if digest != s.Reference.Digest {
		return errors.New("placement: Node Catalog snapshot digest mismatch")
	}
	return nil
}

func ValidateCatalogPage(nodes []CatalogNode) error {
	previous := ""
	for index, node := range nodes {
		if err := validateCatalogNode(node); err != nil {
			return fmt.Errorf("placement: invalid Node Catalog row %d: %w", index, err)
		}
		if index != 0 && node.NodeID <= previous {
			return errors.New("placement: Node Catalog page is not uniquely sorted")
		}
		previous = node.NodeID
	}
	return nil
}

func (s CatalogSnapshot) Clone() CatalogSnapshot {
	return CatalogSnapshot{Reference: s.Reference, Nodes: cloneCatalogNodes(s.Nodes)}
}

func validateCatalogNode(node CatalogNode) error {
	if err := clusterstate.ValidateExecutionBindingNodeID(node.NodeID); err != nil {
		return err
	}
	for name, value := range map[string]string{
		"runtime digest": node.RuntimeDigest, "failure domain": node.FailureDomain,
	} {
		if !utf8.ValidString(value) || len(value) > clusterstate.MaxPlacementCandidateMetadataBytes {
			return fmt.Errorf("%s is invalid or exceeds %d bytes", name, clusterstate.MaxPlacementCandidateMetadataBytes)
		}
	}
	for key, value := range node.Labels {
		if key == "" || !utf8.ValidString(key) || !utf8.ValidString(value) {
			return errors.New("labels must contain non-empty UTF-8 keys and UTF-8 values")
		}
	}
	for capability := range node.Capabilities {
		if capability == "" || !utf8.ValidString(capability) {
			return errors.New("capabilities must contain non-empty UTF-8 names")
		}
	}
	if node.SandboxSlotCapacity == 0 {
		return errors.New("Sandbox slot capacity is required")
	}
	return nil
}

func catalogSnapshotDigest(reference CatalogReference, nodes []CatalogNode) (string, error) {
	value := struct {
		Version              uint8         `json:"version"`
		ClusterID            string        `json:"cluster_id"`
		RegistryGeneration   string        `json:"registry_generation"`
		SystemEpoch          uint64        `json:"system_epoch"`
		RegistryLayoutDigest string        `json:"registry_layout_digest"`
		Revision             uint64        `json:"revision"`
		NodeCount            uint32        `json:"node_count"`
		Nodes                []CatalogNode `json:"nodes"`
	}{
		Version: reference.Version, ClusterID: reference.ClusterID,
		RegistryGeneration: reference.RegistryGeneration, SystemEpoch: reference.SystemEpoch,
		RegistryLayoutDigest: reference.RegistryLayoutDigest, Revision: reference.Revision,
		NodeCount: reference.NodeCount, Nodes: nodes,
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(append([]byte("kuasar-node-catalog-v1\x00"), raw...))
	return hex.EncodeToString(digest[:]), nil
}

func cloneCatalogNodes(source []CatalogNode) []CatalogNode {
	result := make([]CatalogNode, len(source))
	for index, node := range source {
		result[index] = node
		if node.Labels != nil {
			result[index].Labels = make(map[string]string, len(node.Labels))
			for key, value := range node.Labels {
				result[index].Labels[key] = value
			}
		}
		if node.Capabilities != nil {
			result[index].Capabilities = make(map[string]bool, len(node.Capabilities))
			for key, value := range node.Capabilities {
				result[index].Capabilities[key] = value
			}
		}
	}
	return result
}
