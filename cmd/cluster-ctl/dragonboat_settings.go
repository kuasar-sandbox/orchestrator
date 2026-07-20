package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const dragonboatSoftSettingsFilename = "dragonboat-soft-settings.json"

type dragonboatSoftSettings struct {
	InMemEntrySliceSize          uint64
	MinEntrySliceFreeSize        uint64
	PendingProposalShards        uint64
	IncomingReadIndexQueueLength uint64
	IncomingProposalQueueLength  uint64
	ReceiveQueueLength           uint64
	TaskQueueInitialCap          uint64
	TaskQueueTargetLength        uint64
	TaskBatchSize                uint64
}

var requiredDragonboatSoftSettings = dragonboatSoftSettings{
	InMemEntrySliceSize:          128,
	MinEntrySliceFreeSize:        32,
	PendingProposalShards:        4,
	IncomingReadIndexQueueLength: 128,
	IncomingProposalQueueLength:  128,
	ReceiveQueueLength:           128,
	TaskQueueInitialCap:          16,
	TaskQueueTargetLength:        64,
	TaskBatchSize:                128,
}

// Dragonboat reads this fixed file name during package initialization. Validate
// the launch directory before creating a NodeHost so a missing profile cannot
// silently restore the per-replica default queue allocations.
func validateDragonboatSoftSettings(configPath string) error {
	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("get working directory: %w", err)
	}
	return validateDragonboatSoftSettingsAt(configPath, cwd)
}

func validateDragonboatSoftSettingsAt(configPath, cwd string) error {
	if !filepath.IsAbs(configPath) {
		configPath = filepath.Join(cwd, configPath)
	}
	configDir := filepath.Dir(filepath.Clean(configPath))
	cwdInfo, err := os.Stat(cwd)
	if err != nil {
		return fmt.Errorf("stat working directory %q: %w", cwd, err)
	}
	configDirInfo, err := os.Stat(configDir)
	if err != nil {
		return fmt.Errorf("stat config directory %q: %w", configDir, err)
	}
	if !os.SameFile(cwdInfo, configDirInfo) {
		return fmt.Errorf(
			"working directory %q must be the Registry config directory %q",
			cwd, configDir,
		)
	}

	profilePath := filepath.Join(cwd, dragonboatSoftSettingsFilename)
	info, err := os.Stat(profilePath)
	if err != nil {
		return fmt.Errorf("stat %q: %w", profilePath, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%q must be a regular file", profilePath)
	}
	if info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("%q must not be writable by group or other", profilePath)
	}

	file, err := os.Open(profilePath)
	if err != nil {
		return fmt.Errorf("open %q: %w", profilePath, err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var actual dragonboatSoftSettings
	if err := decoder.Decode(&actual); err != nil {
		return fmt.Errorf("decode %q: %w", profilePath, err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return fmt.Errorf("decode %q: %w", profilePath, err)
	}
	if actual != requiredDragonboatSoftSettings {
		return fmt.Errorf("%q does not match the required bounded-memory profile", profilePath)
	}
	return nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("multiple JSON values")
	}
	return err
}
