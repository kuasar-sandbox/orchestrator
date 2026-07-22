package raftstore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"unicode/utf8"
)

const MaxRaftCommandBytes = 4 << 20

func EncodeSystemCommand(command SystemCommand) ([]byte, error) {
	if err := validateUTF8Strings(reflect.ValueOf(command)); err != nil {
		return nil, err
	}
	if err := validateSystemCommandEnvelope(command); err != nil {
		return nil, err
	}
	return marshalBounded(command, MaxRaftCommandBytes)
}

func DecodeSystemCommand(raw []byte) (SystemCommand, error) {
	var command SystemCommand
	if err := unmarshalStrictBounded(raw, &command, MaxRaftCommandBytes); err != nil {
		return SystemCommand{}, err
	}
	if err := validateSystemCommandEnvelope(command); err != nil {
		return SystemCommand{}, err
	}
	return command, nil
}

func EncodeDataCommand(command DataCommand) ([]byte, error) {
	if err := validateUTF8Strings(reflect.ValueOf(command)); err != nil {
		return nil, err
	}
	if err := validateDataCommandEnvelope(command); err != nil {
		return nil, err
	}
	return marshalBounded(command, MaxRaftCommandBytes)
}

func DecodeDataCommand(raw []byte) (DataCommand, error) {
	var command DataCommand
	if err := unmarshalStrictBounded(raw, &command, MaxRaftCommandBytes); err != nil {
		return DataCommand{}, err
	}
	if err := validateDataCommandEnvelope(command); err != nil {
		return DataCommand{}, err
	}
	return command, nil
}

func validateSystemCommandEnvelope(command SystemCommand) error {
	pointers := countPresent(
		command.RegistryLayout != nil, command.Gates != nil, command.Transition != nil, command.Advance != nil,
		command.Closure != nil, command.Drain != nil, command.TransitionDrain != nil,
		command.Recovery != nil, command.RecoveryDrain != nil, command.RecoveryAdvance != nil,
	)
	switch command.Type {
	case SystemBootstrap:
		if pointers != 1 || command.RegistryLayout == nil || !isSHA256(command.Digest) {
			return errors.New("raftstore: malformed System bootstrap command")
		}
	case SystemRefreshPermit, SystemActivateTransition, SystemFinalizeTransition:
		if pointers != 0 || command.Digest != "" {
			return errors.New("raftstore: parameterless System command carries a payload")
		}
	case SystemSetGates:
		if pointers != 1 || command.Gates == nil || command.Digest != "" {
			return errors.New("raftstore: malformed gate command")
		}
	case SystemBeginTransition:
		if pointers != 1 || command.Transition == nil || command.Digest != "" {
			return errors.New("raftstore: malformed registryLayout transition command")
		}
	case SystemAdvanceTransition:
		if pointers != 1 || command.Advance == nil || command.Digest != "" {
			return errors.New("raftstore: malformed transition advance command")
		}
	case SystemCloseRegistryGeneration:
		if pointers != 1 || command.Closure == nil || command.Digest != "" {
			return errors.New("raftstore: malformed Registry History Generation closure command")
		}
	case SystemConfirmDrain:
		if pointers != 1 || command.Drain == nil || command.Digest != "" {
			return errors.New("raftstore: malformed predecessor drain command")
		}
	case SystemConfirmTransitionDrain:
		if pointers != 1 || command.TransitionDrain == nil || command.Digest != "" {
			return errors.New("raftstore: malformed registryLayout transition drain command")
		}
	case SystemBeginRecovery:
		if pointers != 1 || command.Recovery == nil || command.Digest != "" {
			return errors.New("raftstore: malformed recovery command")
		}
	case SystemConfirmRecoveryDrain:
		if pointers != 1 || command.RecoveryDrain == nil || command.Digest != "" {
			return errors.New("raftstore: malformed recovery permit drain command")
		}
	case SystemAdvanceRecovery:
		if pointers != 1 || command.RecoveryAdvance == nil || command.Digest != "" {
			return errors.New("raftstore: malformed recovery advance command")
		}
	default:
		return fmt.Errorf("raftstore: unknown System command %q", command.Type)
	}
	return nil
}

func validateDataCommandEnvelope(command DataCommand) error {
	if err := command.Identity.Validate(); err != nil {
		return err
	}
	pointers := countPresent(
		command.Bootstrap != nil, command.Epoch != nil, command.Route != nil, command.Build != nil,
		command.Fence != nil, command.Compaction != nil,
	)
	hasReplicas := len(command.ReplicaIDs) != 0
	hasExpectation := command.Expect.Absent || command.Expect.LogIndex != 0
	validExpectation := command.Expect.Absent != (command.Expect.LogIndex != 0)
	switch command.Type {
	case DataInitializeShard:
		if pointers != 1 || command.Bootstrap == nil || !hasReplicas || hasExpectation {
			return errors.New("raftstore: malformed data-shard bootstrap command")
		}
	case DataPrepareEpoch:
		if pointers != 1 || command.Epoch == nil || !hasReplicas || hasExpectation {
			return errors.New("raftstore: malformed serving-epoch command")
		}
	case DataRetireEpoch:
		if pointers != 1 || command.Epoch == nil || hasReplicas || hasExpectation {
			return errors.New("raftstore: malformed serving-epoch command")
		}
	case DataPutRoute:
		if pointers != 1 || command.Route == nil || hasReplicas || !hasExpectation || !validExpectation {
			return errors.New("raftstore: malformed Route mutation command")
		}
	case DataPutBuild:
		if pointers != 1 || command.Build == nil || hasReplicas || !hasExpectation || !validExpectation {
			return errors.New("raftstore: malformed Build mutation command")
		}
	case DataPutFence:
		if pointers != 1 || command.Fence == nil || hasReplicas || !hasExpectation || !validExpectation {
			return errors.New("raftstore: malformed execution-fence mutation command")
		}
	case DataCompactFence:
		if pointers != 1 || command.Compaction == nil || hasReplicas || hasExpectation {
			return errors.New("raftstore: malformed execution-fence compaction command")
		}
	default:
		return fmt.Errorf("raftstore: unknown data command %q", command.Type)
	}
	return nil
}

func countPresent(values ...bool) int {
	count := 0
	for _, value := range values {
		if value {
			count++
		}
	}
	return count
}

func marshalBounded(value any, maximum int) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 || len(raw) > maximum {
		return nil, fmt.Errorf("raftstore: encoded value exceeds %d bytes", maximum)
	}
	return raw, nil
}

func unmarshalStrictBounded(raw []byte, out any, maximum int) error {
	if len(raw) == 0 || len(raw) > maximum {
		return fmt.Errorf("raftstore: encoded value must be between 1 and %d bytes", maximum)
	}
	if !utf8.Valid(raw) {
		return errors.New("raftstore: encoded value contains invalid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("raftstore: encoded value contains trailing JSON")
	}
	return nil
}

func validateUTF8Strings(value reflect.Value) error {
	if !value.IsValid() {
		return nil
	}
	switch value.Kind() {
	case reflect.Interface, reflect.Pointer:
		if value.IsNil() {
			return nil
		}
		return validateUTF8Strings(value.Elem())
	case reflect.String:
		if !utf8.ValidString(value.String()) {
			return errors.New("raftstore: command contains invalid UTF-8")
		}
	case reflect.Struct:
		for index := 0; index < value.NumField(); index++ {
			if err := validateUTF8Strings(value.Field(index)); err != nil {
				return err
			}
		}
	case reflect.Array, reflect.Slice:
		if value.Type().Elem().Kind() == reflect.Uint8 {
			return nil
		}
		for index := 0; index < value.Len(); index++ {
			if err := validateUTF8Strings(value.Index(index)); err != nil {
				return err
			}
		}
	case reflect.Map:
		iterator := value.MapRange()
		for iterator.Next() {
			if err := validateUTF8Strings(iterator.Key()); err != nil {
				return err
			}
			if err := validateUTF8Strings(iterator.Value()); err != nil {
				return err
			}
		}
	}
	return nil
}
