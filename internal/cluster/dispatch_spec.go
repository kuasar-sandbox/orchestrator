package cluster

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

const DispatchSpecVersionV1 uint16 = 1

type SandboxDispatchSpecV1 struct {
	Version                uint16                `json:"version"`
	TemplateRef            string                `json:"template_ref"`
	AuthKeyFingerprint     string                `json:"auth_key_fingerprint"`
	ManifestKeyFingerprint string                `json:"manifest_key_fingerprint"`
	TargetRuntimeDigest    string                `json:"target_runtime_digest,omitempty"`
	RequestedConfig        map[string]string     `json:"requested_config,omitempty"`
	Config                 map[string]string     `json:"config,omitempty"`
	AccessToken            string                `json:"access_token"`
	TargetPort             int                   `json:"target_port,omitempty"`
	TimeoutSeconds         int                   `json:"timeout_seconds,omitempty"`
	Request                NodeRequestEnvelopeV1 `json:"request"`
}

func (s SandboxDispatchSpecV1) Validate() error {
	if s.Version != DispatchSpecVersionV1 || s.TemplateRef == "" || s.AccessToken == "" {
		return errors.New("cluster: incomplete Sandbox dispatch spec")
	}
	if _, err := types.ParseTemplateID(s.TemplateRef); err != nil {
		return fmt.Errorf("cluster: invalid Sandbox template reference: %w", err)
	}
	if err := validateKeyFingerprints(s.AuthKeyFingerprint, s.ManifestKeyFingerprint); err != nil {
		return fmt.Errorf("cluster: Sandbox dispatch spec: %w", err)
	}
	if s.TargetPort < 0 || s.TargetPort > 65535 || s.TimeoutSeconds < 0 {
		return errors.New("cluster: invalid Sandbox target port or timeout")
	}
	if _, exists := s.Config[ObjectMetadataKey]; exists {
		return errors.New("cluster: dispatch spec cannot supply system-owned metadata")
	}
	if _, exists := s.RequestedConfig[ObjectMetadataKey]; exists {
		return errors.New("cluster: requested config cannot supply system-owned metadata")
	}
	if err := validateDispatchRequest(s.Request, "/sandboxes", "/v2/sandboxes"); err != nil {
		return fmt.Errorf("cluster: Sandbox dispatch request: %w", err)
	}
	return nil
}

type BuildDispatchSpecV1 struct {
	Version                uint16                `json:"version"`
	TemplateID             string                `json:"template_id"`
	AuthKeyFingerprint     string                `json:"auth_key_fingerprint"`
	ManifestKeyFingerprint string                `json:"manifest_key_fingerprint"`
	TargetRuntimeDigest    string                `json:"target_runtime_digest,omitempty"`
	Profile                types.Profile         `json:"profile"`
	CPUCount               int                   `json:"cpu_count"`
	MemoryMB               int                   `json:"memory_mb"`
	Names                  []string              `json:"names,omitempty"`
	Aliases                []string              `json:"aliases,omitempty"`
	Metadata               map[string]string     `json:"metadata,omitempty"`
	Request                NodeRequestEnvelopeV1 `json:"request"`
}

func (s BuildDispatchSpecV1) Validate() error {
	if s.Version != DispatchSpecVersionV1 || s.TemplateID == "" || !s.Profile.Valid() {
		return errors.New("cluster: incomplete Build dispatch spec")
	}
	if err := validateKeyFingerprints(s.AuthKeyFingerprint, s.ManifestKeyFingerprint); err != nil {
		return fmt.Errorf("cluster: Build dispatch spec: %w", err)
	}
	if s.CPUCount <= 0 || s.MemoryMB <= 0 {
		return errors.New("cluster: Build registration requires positive CPU and memory ceilings")
	}
	if _, exists := s.Metadata[ObjectMetadataKey]; exists {
		return errors.New("cluster: dispatch spec cannot supply system-owned metadata")
	}
	if err := validateDispatchRequest(s.Request, "/templates", "/v3/templates"); err != nil {
		return fmt.Errorf("cluster: Build dispatch request: %w", err)
	}
	return nil
}

func MarshalSandboxDispatchSpec(spec SandboxDispatchSpecV1) ([]byte, error) {
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	return marshalBoundedDispatchSpec(spec)
}

func ParseSandboxDispatchSpec(encoded []byte) (SandboxDispatchSpecV1, error) {
	var spec SandboxDispatchSpecV1
	if err := unmarshalDispatchSpec(encoded, &spec); err != nil {
		return SandboxDispatchSpecV1{}, err
	}
	return spec, spec.Validate()
}

func MarshalBuildDispatchSpec(spec BuildDispatchSpecV1) ([]byte, error) {
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	return marshalBoundedDispatchSpec(spec)
}

func ParseBuildDispatchSpec(encoded []byte) (BuildDispatchSpecV1, error) {
	var spec BuildDispatchSpecV1
	if err := unmarshalDispatchSpec(encoded, &spec); err != nil {
		return BuildDispatchSpecV1{}, err
	}
	return spec, spec.Validate()
}

func marshalBoundedDispatchSpec(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(encoded) > MaxDispatchSpecBytes {
		return nil, fmt.Errorf("cluster: dispatch spec exceeds %d bytes", MaxDispatchSpecBytes)
	}
	return encoded, nil
}

func unmarshalDispatchSpec(encoded []byte, target any) error {
	if len(encoded) == 0 || len(encoded) > MaxDispatchSpecBytes {
		return errors.New("cluster: dispatch spec has invalid size")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("cluster: decode dispatch spec: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("cluster: dispatch spec contains trailing data")
	}
	return nil
}

func validateKeyFingerprints(authFingerprint, manifestFingerprint string) error {
	if !validKeyFingerprint(authFingerprint) || !validKeyFingerprint(manifestFingerprint) {
		return errors.New("AuthKey and ManifestKey fingerprints must each be 24 hexadecimal characters")
	}
	if authFingerprint == manifestFingerprint {
		return errors.New("AuthKey and ManifestKey must use separate key material")
	}
	return nil
}

func validKeyFingerprint(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 12
}

func validateDispatchRequest(request NodeRequestEnvelopeV1, paths ...string) error {
	if err := request.Validate(); err != nil {
		return err
	}
	matched := false
	for _, path := range paths {
		if request.Path == path {
			matched = true
			break
		}
	}
	if !matched {
		return fmt.Errorf("unexpected node request path %q", request.Path)
	}
	fields, err := DecodeJSONObject(request.Body)
	if err != nil {
		return err
	}
	metadata := fields["metadata"]
	if len(metadata) == 0 || bytes.Equal(metadata, []byte("null")) {
		return nil
	}
	var values map[string]json.RawMessage
	if err := json.Unmarshal(metadata, &values); err != nil {
		return errors.New("cluster: request metadata must be a JSON object")
	}
	if _, exists := values[ObjectMetadataKey]; exists {
		return errors.New("cluster: request cannot supply system-owned metadata")
	}
	return nil
}
