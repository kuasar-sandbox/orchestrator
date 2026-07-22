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

const MaxSandboxTimeoutSeconds int64 = (1<<63 - 1) / 1_000_000_000

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
	if s.TargetPort < 0 || s.TargetPort > 65535 || s.TimeoutSeconds < 0 ||
		int64(s.TimeoutSeconds) > MaxSandboxTimeoutSeconds {
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
	if err := validateConfigHeaders(s.Request.Header, s.Config, false); err != nil {
		return fmt.Errorf("cluster: Sandbox dispatch request: %w", err)
	}
	if err := validateSandboxTemplateRef(s.Request.Body, s.TemplateRef); err != nil {
		return fmt.Errorf("cluster: Sandbox dispatch request: %w", err)
	}
	if err := validateExactRequestField(s.Request.Body, "timeout", s.TimeoutSeconds); err != nil {
		return fmt.Errorf("cluster: Sandbox dispatch request: %w", err)
	}
	if err := validateExactRequestField(s.Request.Body, "metadata", s.Config); err != nil {
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
	if err := validateConfigHeaders(s.Request.Header, s.Metadata, true); err != nil {
		return fmt.Errorf("cluster: Build dispatch request: %w", err)
	}
	if err := validateBuildResourceCeilings(s.Request.Body, s.CPUCount, s.MemoryMB); err != nil {
		return fmt.Errorf("cluster: Build dispatch request: %w", err)
	}
	name := ""
	if len(s.Names) > 0 {
		name = s.Names[0]
	}
	for _, field := range []struct {
		name  string
		value any
	}{
		{name: "name", value: name},
		{name: "tags", value: s.Aliases},
		{name: "profile", value: string(s.Profile)},
		{name: "metadata", value: s.Metadata},
	} {
		if err := validateExactRequestField(s.Request.Body, field.name, field.value); err != nil {
			return fmt.Errorf("cluster: Build dispatch request: %w", err)
		}
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

var dispatchConfigHeaders = []struct {
	header      string
	metadataKey string
}{
	{header: "X-Kuasar-Sandbox-Resource", metadataKey: "kuasar-sandbox.resource"},
	{header: "X-Kuasar-Sandbox-Network", metadataKey: "kuasar-sandbox.network"},
	{header: "X-Kuasar-Sandbox-Launch", metadataKey: "kuasar-sandbox.launch"},
	{header: "X-Kuasar-Sandbox-Init", metadataKey: "kuasar-sandbox.init"},
	{header: "X-Kuasar-Sandbox-Mounts", metadataKey: "kuasar-sandbox.mounts"},
	{header: "X-Kuasar-Sandbox-Files", metadataKey: "kuasar-sandbox.files"},
	{header: "X-Kuasar-Sandbox-Metadata", metadataKey: "kuasar-sandbox.metadata"},
}

func validateConfigHeaders(headers map[string][]string, metadata map[string]string, build bool) error {
	for _, field := range dispatchConfigHeaders {
		if err := validateConfigHeader(headers, metadata, field.header, field.metadataKey); err != nil {
			return err
		}
	}
	if build {
		return validateConfigHeader(headers, metadata, "X-Kuasar-Sandbox-Builder", "kuasar-sandbox.builder")
	}
	return nil
}

func validateConfigHeader(headers map[string][]string, metadata map[string]string, header, metadataKey string) error {
	values := headers[header]
	if len(values) == 0 {
		return nil
	}
	if len(values) != 1 || metadata[metadataKey] != values[0] {
		return fmt.Errorf("%s must match canonical request metadata", header)
	}
	return nil
}

func validKeyFingerprint(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 12 && hex.EncodeToString(decoded) == value
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
	var metadata json.RawMessage
	for name, value := range fields {
		if strings.EqualFold(name, "metadata") {
			if name != "metadata" {
				return errors.New("cluster: request metadata field must use canonical lowercase spelling")
			}
			metadata = value
		}
	}
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

func validateBuildResourceCeilings(body []byte, cpuCount, memoryMB int) error {
	fields, err := DecodeJSONObject(body)
	if err != nil {
		return err
	}
	for name := range fields {
		for _, canonical := range []string{"cpuCount", "cpu_count", "memoryMB", "memory_mb"} {
			if strings.EqualFold(name, canonical) && name != canonical {
				return fmt.Errorf("resource field %q must use canonical spelling", name)
			}
		}
	}
	bodyCPU, err := aliasedPositiveInt(fields, "cpuCount", "cpu_count")
	if err != nil {
		return fmt.Errorf("CPU ceiling: %w", err)
	}
	bodyMemory, err := aliasedPositiveInt(fields, "memoryMB", "memory_mb")
	if err != nil {
		return fmt.Errorf("memory ceiling: %w", err)
	}
	if bodyCPU != cpuCount || bodyMemory != memoryMB {
		return errors.New("replayed CPU or memory ceiling differs from normalized Build resources")
	}
	return nil
}

func validateSandboxTemplateRef(body []byte, templateRef string) error {
	return validateExactRequestField(body, "templateID", templateRef)
}

func validateExactRequestField(body []byte, field string, expected any) error {
	fields, err := DecodeJSONObject(body)
	if err != nil {
		return err
	}
	for name := range fields {
		if strings.EqualFold(name, field) && name != field {
			return fmt.Errorf("request field %q must use canonical spelling", name)
		}
	}
	raw, found := fields[field]
	if !found {
		return fmt.Errorf("replayed request is missing %s", field)
	}
	encoded, err := json.Marshal(expected)
	if err != nil {
		return fmt.Errorf("encode expected %s: %w", field, err)
	}
	if !bytes.Equal(raw, encoded) {
		return fmt.Errorf("replayed request %s differs from immutable dispatch value", field)
	}
	return nil
}

func aliasedPositiveInt(fields map[string]json.RawMessage, names ...string) (int, error) {
	value := 0
	found := false
	for _, name := range names {
		raw, ok := fields[name]
		if !ok {
			continue
		}
		var current int
		if err := json.Unmarshal(raw, &current); err != nil || current <= 0 {
			return 0, fmt.Errorf("%s must be a positive integer", name)
		}
		if found && current != value {
			return 0, fmt.Errorf("conflicting %s aliases", names[0])
		}
		value = current
		found = true
	}
	if !found {
		return 0, fmt.Errorf("one of %s is required", strings.Join(names, "/"))
	}
	return value, nil
}
