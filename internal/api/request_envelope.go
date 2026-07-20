package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
)

// ParseSandboxCreateEnvelope applies the same recognized-field and config
// header normalization as the standalone HTTP API while leaving unknown fields
// in the immutable envelope.
func ParseSandboxCreateEnvelope(envelope clusterstate.NodeRequestEnvelopeV1) (CreateReq, error) {
	if err := envelope.Validate(); err != nil {
		return CreateReq{}, err
	}
	var request CreateReq
	if err := json.Unmarshal(envelope.Body, &request); err != nil {
		return CreateReq{}, fmt.Errorf("api: decode Sandbox create request: %w", err)
	}
	request.Metadata = mergeConfigHeaders(request.Metadata, envelope.Header)
	request.Metadata = clusterstate.WithoutSystemMetadata(request.Metadata)
	return request, nil
}

// RewriteSandboxCreateEnvelope overwrites only cluster-owned recognized fields.
// Every unknown body field, query parameter, and ordinary header is retained.
func RewriteSandboxCreateEnvelope(
	envelope clusterstate.NodeRequestEnvelopeV1,
	templateRef string,
	timeoutSeconds int,
	metadata map[string]string,
) (clusterstate.NodeRequestEnvelopeV1, error) {
	fields, err := clusterstate.DecodeJSONObject(envelope.Body)
	if err != nil {
		return clusterstate.NodeRequestEnvelopeV1{}, err
	}
	if err := validateEnvelopeFieldSpellings(fields,
		[]string{"templateID"}, []string{"timeout"}, []string{"metadata"},
	); err != nil {
		return clusterstate.NodeRequestEnvelopeV1{}, err
	}
	metadata = clusterstate.WithoutSystemMetadata(metadata)
	if err := setEnvelopeField(fields, "templateID", templateRef); err != nil {
		return clusterstate.NodeRequestEnvelopeV1{}, err
	}
	if err := setEnvelopeField(fields, "timeout", timeoutSeconds); err != nil {
		return clusterstate.NodeRequestEnvelopeV1{}, err
	}
	if err := setEnvelopeField(fields, "metadata", metadata); err != nil {
		return clusterstate.NodeRequestEnvelopeV1{}, err
	}
	envelope.Body, err = clusterstate.EncodeJSONObject(fields)
	if err != nil {
		return clusterstate.NodeRequestEnvelopeV1{}, err
	}
	return envelope, envelope.Validate()
}

// ParseBuildRegisterEnvelope normalizes the supported resource aliases and
// requires the immutable registration ceiling before placement.
func ParseBuildRegisterEnvelope(envelope clusterstate.NodeRequestEnvelopeV1) (RegisterSpec, error) {
	if err := envelope.Validate(); err != nil {
		return RegisterSpec{}, err
	}
	var body struct {
		Name       string            `json:"name"`
		Tags       []string          `json:"tags"`
		Profile    string            `json:"profile"`
		CPUCount   *int              `json:"cpuCount"`
		CPUCountSn *int              `json:"cpu_count"`
		MemoryMB   *int              `json:"memoryMB"`
		MemoryMBSn *int              `json:"memory_mb"`
		Metadata   map[string]string `json:"metadata"`
	}
	if err := json.Unmarshal(envelope.Body, &body); err != nil {
		return RegisterSpec{}, fmt.Errorf("api: decode Build register request: %w", err)
	}
	profile, err := requestedBuildProfile(body.Profile)
	if err != nil {
		return RegisterSpec{}, err
	}
	cpu, err := positiveAlias("cpuCount", body.CPUCount, body.CPUCountSn, true)
	if err != nil {
		return RegisterSpec{}, err
	}
	memory, err := positiveAlias("memoryMB", body.MemoryMB, body.MemoryMBSn, true)
	if err != nil {
		return RegisterSpec{}, err
	}
	metadata := mergeBuildConfigHeaders(body.Metadata, envelope.Header)
	return RegisterSpec{
		Name: body.Name, Tags: append([]string(nil), body.Tags...), Profile: profile,
		CPUCount: cpu, MemoryMB: memory,
		Metadata: clusterstate.WithoutSystemMetadata(metadata),
	}, nil
}

// RewriteBuildRegisterEnvelope records one canonical alias spelling and the
// final immutable registration values without narrowing the request body.
func RewriteBuildRegisterEnvelope(
	envelope clusterstate.NodeRequestEnvelopeV1,
	spec RegisterSpec,
) (clusterstate.NodeRequestEnvelopeV1, error) {
	if !spec.Profile.Valid() || spec.CPUCount <= 0 || spec.MemoryMB <= 0 {
		return clusterstate.NodeRequestEnvelopeV1{}, errors.New("api: incomplete Build registration")
	}
	fields, err := clusterstate.DecodeJSONObject(envelope.Body)
	if err != nil {
		return clusterstate.NodeRequestEnvelopeV1{}, err
	}
	if err := validateEnvelopeFieldSpellings(fields,
		[]string{"name"}, []string{"tags"}, []string{"profile"},
		[]string{"cpuCount", "cpu_count"}, []string{"memoryMB", "memory_mb"}, []string{"metadata"},
	); err != nil {
		return clusterstate.NodeRequestEnvelopeV1{}, err
	}
	delete(fields, "cpu_count")
	delete(fields, "memory_mb")
	values := []struct {
		name  string
		value any
	}{
		{"name", spec.Name},
		{"tags", spec.Tags},
		{"profile", string(spec.Profile)},
		{"cpuCount", spec.CPUCount},
		{"memoryMB", spec.MemoryMB},
		{"metadata", clusterstate.WithoutSystemMetadata(spec.Metadata)},
	}
	for _, value := range values {
		if err := setEnvelopeField(fields, value.name, value.value); err != nil {
			return clusterstate.NodeRequestEnvelopeV1{}, err
		}
	}
	envelope.Body, err = clusterstate.EncodeJSONObject(fields)
	if err != nil {
		return clusterstate.NodeRequestEnvelopeV1{}, err
	}
	return envelope, envelope.Validate()
}

func setEnvelopeField(fields map[string]json.RawMessage, name string, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("api: encode request field %q: %w", name, err)
	}
	fields[name] = encoded
	return nil
}

func validateEnvelopeFieldSpellings(fields map[string]json.RawMessage, groups ...[]string) error {
	for name := range fields {
		for _, group := range groups {
			matched := false
			exact := false
			for _, allowed := range group {
				matched = matched || strings.EqualFold(name, allowed)
				exact = exact || name == allowed
			}
			if matched && !exact {
				return fmt.Errorf("api: request field %q uses a non-canonical spelling", name)
			}
		}
	}
	return nil
}
