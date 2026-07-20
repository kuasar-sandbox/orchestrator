package cluster

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

const DispatchSpecVersionV1 uint16 = 1

type SandboxDispatchSpecV1 struct {
	Version             uint16            `json:"version"`
	TemplateRef         string            `json:"template_ref"`
	KeyFingerprint      string            `json:"key_fingerprint"`
	TargetRuntimeDigest string            `json:"target_runtime_digest,omitempty"`
	RequestedConfig     map[string]string `json:"requested_config,omitempty"`
	Config              map[string]string `json:"config,omitempty"`
	AccessToken         string            `json:"access_token"`
	TargetPort          int               `json:"target_port,omitempty"`
	TimeoutSeconds      int               `json:"timeout_seconds,omitempty"`
}

func (s SandboxDispatchSpecV1) Validate() error {
	if s.Version != DispatchSpecVersionV1 || s.TemplateRef == "" || s.AccessToken == "" {
		return errors.New("cluster: incomplete Sandbox dispatch spec")
	}
	if _, err := types.ParseTemplateID(s.TemplateRef); err != nil {
		return fmt.Errorf("cluster: invalid Sandbox template reference: %w", err)
	}
	if !validKeyFingerprint(s.KeyFingerprint) {
		return errors.New("cluster: Sandbox key fingerprint is invalid")
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
	return nil
}

type BuildDispatchSpecV1 struct {
	Version             uint16               `json:"version"`
	TemplateID          string               `json:"template_id"`
	KeyFingerprint      string               `json:"key_fingerprint"`
	TargetRuntimeDigest string               `json:"target_runtime_digest,omitempty"`
	Profile             types.Profile        `json:"profile"`
	FromImage           string               `json:"from_image,omitempty"`
	FromTemplate        string               `json:"from_template,omitempty"`
	StartCmd            string               `json:"start_cmd,omitempty"`
	ReadyCmd            string               `json:"ready_cmd,omitempty"`
	Steps               []types.TemplateStep `json:"steps,omitempty"`
	Names               []string             `json:"names,omitempty"`
	Aliases             []string             `json:"aliases,omitempty"`
	Metadata            map[string]string    `json:"metadata,omitempty"`
	Builder             types.BuildOptions   `json:"builder,omitempty"`
	PullCapability      string               `json:"pull_capability,omitempty"`
}

func (s BuildDispatchSpecV1) Validate() error {
	if s.Version != DispatchSpecVersionV1 || s.TemplateID == "" || !s.Profile.Valid() {
		return errors.New("cluster: incomplete Build dispatch spec")
	}
	if !validKeyFingerprint(s.KeyFingerprint) {
		return errors.New("cluster: Build key fingerprint is invalid")
	}
	if s.FromImage != "" && s.FromTemplate != "" {
		return errors.New("cluster: Build from_image and from_template are mutually exclusive")
	}
	if _, exists := s.Metadata[ObjectMetadataKey]; exists {
		return errors.New("cluster: dispatch spec cannot supply system-owned metadata")
	}
	if s.PullCapability != "" && !strings.HasPrefix(s.PullCapability, "kpt_") {
		return errors.New("cluster: Build credentials must use an encrypted pull capability")
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

func validKeyFingerprint(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 12
}
