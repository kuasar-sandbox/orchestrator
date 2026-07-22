package placement

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

type NormalizedDemand struct {
	Version uint16         `json:"version"`
	Kind    ObjectKind     `json:"kind"`
	Sandbox *SandboxDemand `json:"sandbox,omitempty"`
	Build   *BuildDemand   `json:"build,omitempty"`
}

func NormalizeSandboxDemand(demand SandboxDemand) ([]byte, error) {
	value := NormalizedDemand{Version: LoadModelVersion, Kind: ObjectSandbox, Sandbox: &demand}
	if err := value.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func NormalizeBuildDemand(demand BuildDemand) ([]byte, error) {
	value := NormalizedDemand{Version: LoadModelVersion, Kind: ObjectBuild, Build: &demand}
	if err := value.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func ParseNormalizedDemand(encoded []byte) (NormalizedDemand, error) {
	if len(encoded) == 0 {
		return NormalizedDemand{}, errors.New("placement: normalized demand is empty")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var demand NormalizedDemand
	if err := decoder.Decode(&demand); err != nil {
		return NormalizedDemand{}, fmt.Errorf("placement: decode normalized demand: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return NormalizedDemand{}, errors.New("placement: trailing normalized demand value")
	}
	if err := demand.Validate(); err != nil {
		return NormalizedDemand{}, err
	}
	canonical, err := json.Marshal(demand)
	if err != nil {
		return NormalizedDemand{}, err
	}
	if !bytes.Equal(canonical, encoded) {
		return NormalizedDemand{}, errors.New("placement: demand is not in canonical encoding")
	}
	return demand, nil
}

func (d NormalizedDemand) Validate() error {
	if d.Version != LoadModelVersion {
		return errors.New("placement: unsupported normalized demand version")
	}
	switch d.Kind {
	case ObjectSandbox:
		if d.Sandbox == nil || d.Build != nil || d.Sandbox.SlotUnits == 0 {
			return errors.New("placement: invalid normalized sandbox demand")
		}
	case ObjectBuild:
		if d.Build == nil || d.Sandbox != nil || d.Build.Slots == 0 {
			return errors.New("placement: invalid normalized build demand")
		}
	default:
		return errors.New("placement: invalid normalized demand kind")
	}
	return nil
}

func (d NormalizedDemand) ProbeRequest(nodeID, runtimeDigest, catalogDigest string) PlacementProbeRequest {
	return PlacementProbeRequest{
		Kind: d.Kind, NodeID: nodeID, LoadModelVersion: d.Version,
		RuntimeDigest: runtimeDigest, CatalogDigest: catalogDigest, Sandbox: d.Sandbox, Build: d.Build,
	}
}
