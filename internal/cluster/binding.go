package cluster

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

const (
	ExecutionBindingVersion             = 1
	ExecutionBindingPrefix              = "keb1."
	MaxExecutionBindingSize             = 16 << 10
	MaxExecutionBindingNodeIDSize       = 128
	MaxExecutionBindingGenerationIDSize = 128
)

const executionBindingMagic = "kuasar-execution-binding-v1"

type ExecutionKind uint8

const (
	ExecutionKindSandbox ExecutionKind = 1
	ExecutionKindBuild   ExecutionKind = 2
)

type ExecutionBinding struct {
	StorageGeneration  string
	Kind               ExecutionKind
	ObjectID           string
	Group              string
	RouteKey           string
	NodeID             string
	NodeEpoch          uint64
	DemandDigest       [sha256.Size]byte
	DispatchSpecDigest [sha256.Size]byte
}

func EncodeExecutionBinding(binding ExecutionBinding) (string, error) {
	if err := binding.validate(); err != nil {
		return "", err
	}
	var payload bytes.Buffer
	payload.WriteString(executionBindingMagic)
	payload.WriteByte(byte(binding.Kind))
	for _, field := range []string{
		binding.StorageGeneration,
		binding.ObjectID,
		binding.Group,
		binding.RouteKey,
		binding.NodeID,
	} {
		writeBindingString(&payload, field)
	}
	var epoch [8]byte
	binary.BigEndian.PutUint64(epoch[:], binding.NodeEpoch)
	payload.Write(epoch[:])
	payload.Write(binding.DemandDigest[:])
	payload.Write(binding.DispatchSpecDigest[:])
	if payload.Len() > MaxExecutionBindingSize {
		return "", fmt.Errorf("cluster: execution binding exceeds %d bytes", MaxExecutionBindingSize)
	}
	return ExecutionBindingPrefix + base64.RawURLEncoding.EncodeToString(payload.Bytes()), nil
}

func DecodeExecutionBinding(opaque string) (ExecutionBinding, error) {
	if !strings.HasPrefix(opaque, ExecutionBindingPrefix) {
		return ExecutionBinding{}, errors.New("cluster: unsupported execution binding version")
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(opaque, ExecutionBindingPrefix))
	if err != nil {
		return ExecutionBinding{}, fmt.Errorf("cluster: decode execution binding: %w", err)
	}
	if len(payload) > MaxExecutionBindingSize {
		return ExecutionBinding{}, fmt.Errorf("cluster: execution binding exceeds %d bytes", MaxExecutionBindingSize)
	}
	r := bytes.NewReader(payload)
	magic := make([]byte, len(executionBindingMagic))
	if _, err := io.ReadFull(r, magic); err != nil || string(magic) != executionBindingMagic {
		return ExecutionBinding{}, errors.New("cluster: invalid execution binding magic")
	}
	kind, err := r.ReadByte()
	if err != nil {
		return ExecutionBinding{}, errors.New("cluster: truncated execution binding")
	}
	binding := ExecutionBinding{Kind: ExecutionKind(kind)}
	fields := []*string{
		&binding.StorageGeneration,
		&binding.ObjectID,
		&binding.Group,
		&binding.RouteKey,
		&binding.NodeID,
	}
	for _, field := range fields {
		if *field, err = readBindingString(r); err != nil {
			return ExecutionBinding{}, err
		}
	}
	var epoch [8]byte
	if _, err := io.ReadFull(r, epoch[:]); err != nil {
		return ExecutionBinding{}, errors.New("cluster: truncated execution binding epoch")
	}
	binding.NodeEpoch = binary.BigEndian.Uint64(epoch[:])
	if _, err := io.ReadFull(r, binding.DemandDigest[:]); err != nil {
		return ExecutionBinding{}, errors.New("cluster: truncated execution binding demand digest")
	}
	if _, err := io.ReadFull(r, binding.DispatchSpecDigest[:]); err != nil {
		return ExecutionBinding{}, errors.New("cluster: truncated execution binding spec digest")
	}
	if r.Len() != 0 {
		return ExecutionBinding{}, errors.New("cluster: trailing execution binding bytes")
	}
	if err := binding.validate(); err != nil {
		return ExecutionBinding{}, err
	}
	return binding, nil
}

func ExecutionBindingDigest(opaque string) (string, error) {
	if _, err := DecodeExecutionBinding(opaque); err != nil {
		return "", err
	}
	digest := sha256.Sum256([]byte(opaque))
	return hex.EncodeToString(digest[:]), nil
}

func WithExecutionBinding(metadata map[string]string, opaque string) (map[string]string, error) {
	if _, err := DecodeExecutionBinding(opaque); err != nil {
		return nil, err
	}
	out := cloneStringMap(metadata)
	if out == nil {
		out = map[string]string{}
	}
	out[ObjectMetadataKey] = opaque
	return out, nil
}

func ExecutionBindingFromMetadata(metadata map[string]string) (ExecutionBinding, string, error) {
	opaque := metadata[ObjectMetadataKey]
	if opaque == "" {
		return ExecutionBinding{}, "", errors.New("cluster: execution binding metadata is missing")
	}
	binding, err := DecodeExecutionBinding(opaque)
	return binding, opaque, err
}

// WithoutSystemMetadata returns user-visible/inheritable metadata. The reserved
// cluster entry is never accepted from, exported to, or inherited by user paths.
func WithoutSystemMetadata(metadata map[string]string) map[string]string {
	if len(metadata) == 0 {
		return nil
	}
	out := cloneStringMap(metadata)
	delete(out, ObjectMetadataKey)
	if len(out) == 0 {
		return nil
	}
	return out
}

func (b ExecutionBinding) validate() error {
	for name, value := range map[string]string{
		"storage generation": b.StorageGeneration,
		"object id":          b.ObjectID,
		"group":              b.Group,
		"node id":            b.NodeID,
	} {
		if value == "" {
			return fmt.Errorf("cluster: execution binding %s is required", name)
		}
		if !utf8.ValidString(value) {
			return fmt.Errorf("cluster: execution binding %s is not valid UTF-8", name)
		}
	}
	if !utf8.ValidString(b.RouteKey) {
		return errors.New("cluster: execution binding route key is not valid UTF-8")
	}
	if len(b.NodeID) > MaxExecutionBindingNodeIDSize {
		return fmt.Errorf("cluster: execution binding node id exceeds %d bytes", MaxExecutionBindingNodeIDSize)
	}
	if len(b.StorageGeneration) > MaxExecutionBindingGenerationIDSize {
		return fmt.Errorf("cluster: execution binding storage generation exceeds %d bytes", MaxExecutionBindingGenerationIDSize)
	}
	switch b.Kind {
	case ExecutionKindSandbox:
		if b.RouteKey == "" {
			return errors.New("cluster: sandbox execution binding route key is required")
		}
	case ExecutionKindBuild:
		if b.RouteKey != "" {
			return errors.New("cluster: build execution binding cannot carry a route key")
		}
	default:
		return fmt.Errorf("cluster: invalid execution kind %d", b.Kind)
	}
	if b.NodeEpoch == 0 {
		return errors.New("cluster: execution binding node epoch is required")
	}
	return nil
}

func writeBindingString(w *bytes.Buffer, value string) {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))
	w.Write(length[:])
	w.WriteString(value)
}

func readBindingString(r *bytes.Reader) (string, error) {
	var length [4]byte
	if _, err := io.ReadFull(r, length[:]); err != nil {
		return "", errors.New("cluster: truncated execution binding string length")
	}
	n := binary.BigEndian.Uint32(length[:])
	if n > MaxExecutionBindingSize || int(n) > r.Len() {
		return "", errors.New("cluster: invalid execution binding string length")
	}
	value := make([]byte, int(n))
	if _, err := io.ReadFull(r, value); err != nil {
		return "", errors.New("cluster: truncated execution binding string")
	}
	if !utf8.Valid(value) {
		return "", errors.New("cluster: execution binding string is not valid UTF-8")
	}
	return string(value), nil
}
