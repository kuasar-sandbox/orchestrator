package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ObjectMetadataKey carries cluster routing identity in sandbox/build metadata.
// Nodes persist this value opaquely; only cluster components interpret it.
const ObjectMetadataKey = "kuasar-sandbox.cluster"

type ObjectLocation struct {
	Group    string `json:"group"`
	RouteKey string `json:"route_key,omitempty"`
}

func WithObjectLocation(metadata map[string]string, location ObjectLocation) (map[string]string, error) {
	if location.Group == "" {
		return nil, errors.New("cluster: object metadata group is required")
	}
	raw, err := json.Marshal(location)
	if err != nil {
		return nil, fmt.Errorf("cluster: encode object metadata: %w", err)
	}
	out := cloneStringMap(metadata)
	if out == nil {
		out = map[string]string{}
	}
	out[ObjectMetadataKey] = string(raw)
	return out, nil
}

func ObjectLocationFromMetadata(metadata map[string]string) (ObjectLocation, error) {
	raw := metadata[ObjectMetadataKey]
	if raw == "" {
		return ObjectLocation{}, errors.New("cluster: object metadata is missing")
	}
	var location ObjectLocation
	if err := json.Unmarshal([]byte(raw), &location); err != nil {
		return ObjectLocation{}, fmt.Errorf("cluster: decode object metadata: %w", err)
	}
	if location.Group == "" {
		return ObjectLocation{}, errors.New("cluster: object metadata group is required")
	}
	return location, nil
}

func NodeSandboxRefFromMetadata(sandboxID string, metadata map[string]string) (NodeSandboxRef, error) {
	if sandboxID == "" {
		return NodeSandboxRef{}, errors.New("cluster: sandbox id is required")
	}
	location, err := ObjectLocationFromMetadata(metadata)
	if err != nil {
		return NodeSandboxRef{}, err
	}
	if location.RouteKey == "" {
		return NodeSandboxRef{}, errors.New("cluster: sandbox route key is required")
	}
	return NodeSandboxRef{SandboxID: sandboxID, Group: location.Group, RouteKey: location.RouteKey}, nil
}

func NodeBuildRefFromMetadata(buildID string, metadata map[string]string) (NodeBuildRef, error) {
	if buildID == "" {
		return NodeBuildRef{}, errors.New("cluster: build id is required")
	}
	location, err := ObjectLocationFromMetadata(metadata)
	if err != nil {
		return NodeBuildRef{}, err
	}
	return NodeBuildRef{BuildID: buildID, Group: location.Group}, nil
}
