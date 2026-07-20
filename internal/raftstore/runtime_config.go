package raftstore

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	dbconfig "github.com/lni/dragonboat/v4/config"
)

const runtimeConfigVersion = uint32(1)

const maximumFenceRetentionMillis = uint64(30 * 24 * 60 * 60 * 1000)

type StorageAttestor interface {
	VerifyEncrypted(paths ...string) error
}

type StorageAttestorFunc func(paths ...string) error

func (f StorageAttestorFunc) VerifyEncrypted(paths ...string) error { return f(paths...) }

type RaftTLS struct {
	CAFile   string
	CertFile string
	KeyFile  string
}

type RuntimeTuning struct {
	RTTMillis              uint64
	HeartbeatRTT           uint64
	ElectionRTT            uint64
	SnapshotEntries        uint64
	CompactionOverhead     uint64
	MaxInMemLogBytes       uint64
	MaxSendQueueBytes      uint64
	MaxRecvQueueBytes      uint64
	SnapshotWorkers        uint64
	LogDBMemory            string
	OperationTimeoutMillis uint64
	FenceRetentionMillis   uint64
	StateEngine            StateEngineTuning
}

func DefaultRuntimeTuning() RuntimeTuning {
	return RuntimeTuning{
		RTTMillis: 5, HeartbeatRTT: 2, ElectionRTT: 20,
		SnapshotEntries: 100_000, CompactionOverhead: 10_000,
		MaxInMemLogBytes: 128 << 20, MaxSendQueueBytes: 256 << 20,
		MaxRecvQueueBytes: 256 << 20, SnapshotWorkers: 4, LogDBMemory: "medium",
		OperationTimeoutMillis: 5000,
		FenceRetentionMillis:   60 * 60 * 1000,
		StateEngine:            DefaultStateEngineTuning(),
	}
}

func (t RuntimeTuning) Validate() error {
	if t.RTTMillis == 0 || t.HeartbeatRTT == 0 || t.ElectionRTT <= 2*t.HeartbeatRTT ||
		t.SnapshotEntries == 0 || t.CompactionOverhead >= t.SnapshotEntries ||
		t.MaxInMemLogBytes < 2*MaxRaftCommandBytes || t.MaxSendQueueBytes == 0 ||
		t.MaxRecvQueueBytes == 0 || t.SnapshotWorkers == 0 {
		return errors.New("raftstore: invalid bounded Raft tuning")
	}
	if t.OperationTimeoutMillis < 100 || t.OperationTimeoutMillis > 60_000 {
		return errors.New("raftstore: operation timeout must be between 100 milliseconds and 60 seconds")
	}
	if t.FenceRetentionMillis == 0 || t.FenceRetentionMillis > maximumFenceRetentionMillis {
		return errors.New("raftstore: execution-fence retention must be between one millisecond and 30 days")
	}
	if err := t.StateEngine.Validate(); err != nil {
		return err
	}
	switch t.LogDBMemory {
	case "tiny", "small", "medium", "large":
		return nil
	default:
		return errors.New("raftstore: LogDB memory profile must be tiny, small, medium, or large")
	}
}

type RuntimeConfig struct {
	MemberID                string
	NodeHostDir             string
	WALDir                  string
	StateEngineDir          string
	ListenAddress           string
	RegistryLayoutGuardPath string
	EnrollmentPath          string
	TLS                     RaftTLS
	Tuning                  RuntimeTuning
	StorageAttestor         StorageAttestor
}

func (c RuntimeConfig) validate(member RegistryMember) error {
	if c.MemberID == "" || c.MemberID != member.MemberID || c.NodeHostDir == "" || c.StateEngineDir == "" ||
		c.RegistryLayoutGuardPath == "" || c.EnrollmentPath == "" {
		return errors.New("raftstore: incomplete local Registry member configuration")
	}
	if !filepath.IsAbs(c.NodeHostDir) || c.WALDir != "" && !filepath.IsAbs(c.WALDir) ||
		!filepath.IsAbs(c.StateEngineDir) ||
		!filepath.IsAbs(c.RegistryLayoutGuardPath) || !filepath.IsAbs(c.EnrollmentPath) {
		return errors.New("raftstore: Raft storage and identity paths must be absolute")
	}
	if c.TLS.CAFile == "" || c.TLS.CertFile == "" || c.TLS.KeyFile == "" {
		return errors.New("raftstore: Raft mutual TLS material is required")
	}
	if filepath.Clean(c.RegistryLayoutGuardPath) == filepath.Clean(c.EnrollmentPath) {
		return errors.New("raftstore: registryLayout guard and enrollment must use distinct files")
	}
	if c.StorageAttestor == nil {
		return errors.New("raftstore: encrypted storage attestation is required")
	}
	if err := c.Tuning.Validate(); err != nil {
		return err
	}
	for _, identityPath := range []string{c.RegistryLayoutGuardPath, c.EnrollmentPath} {
		if pathWithin(c.NodeHostDir, identityPath) || c.WALDir != "" && pathWithin(c.WALDir, identityPath) ||
			pathWithin(c.StateEngineDir, identityPath) {
			return errors.New("raftstore: registryLayout/enrollment state must be outside Dragonboat data directories")
		}
	}
	if pathWithin(c.NodeHostDir, c.StateEngineDir) || pathWithin(c.StateEngineDir, c.NodeHostDir) ||
		c.WALDir != "" && (pathWithin(c.WALDir, c.StateEngineDir) || pathWithin(c.StateEngineDir, c.WALDir)) {
		return errors.New("raftstore: Pebble state engine and Dragonboat storage must use distinct directories")
	}
	return nil
}

func (c RuntimeConfig) dragonboatConfig(registryLayout RegistryLayout, member RegistryMember) (dbconfig.NodeHostConfig, error) {
	if err := c.validate(member); err != nil {
		return dbconfig.NodeHostConfig{}, err
	}
	expert := dbconfig.GetDefaultExpertConfig()
	expert.Engine.SnapshotShards = c.Tuning.SnapshotWorkers
	switch c.Tuning.LogDBMemory {
	case "tiny":
		expert.LogDB = dbconfig.GetTinyMemLogDBConfig()
	case "small":
		expert.LogDB = dbconfig.GetSmallMemLogDBConfig()
	case "medium":
		expert.LogDB = dbconfig.GetMediumMemLogDBConfig()
	case "large":
		expert.LogDB = dbconfig.GetLargeMemLogDBConfig()
	}
	config := dbconfig.NodeHostConfig{
		DeploymentID: deploymentID(registryLayout.ClusterID, registryLayout.RegistryGeneration),
		NodeHostDir:  c.NodeHostDir, WALDir: c.WALDir, RTTMillisecond: c.Tuning.RTTMillis,
		RaftAddress: member.RaftEndpoint, ListenAddress: c.ListenAddress,
		MutualTLS: true, CAFile: c.TLS.CAFile, CertFile: c.TLS.CertFile, KeyFile: c.TLS.KeyFile,
		MaxSendQueueSize: c.Tuning.MaxSendQueueBytes, MaxReceiveQueueSize: c.Tuning.MaxRecvQueueBytes,
		DefaultNodeRegistryEnabled: false, Expert: expert,
	}
	if err := config.Validate(); err != nil {
		return dbconfig.NodeHostConfig{}, fmt.Errorf("raftstore: invalid Dragonboat NodeHost config: %w", err)
	}
	return config, nil
}

func (c RuntimeConfig) raftConfig(shardID, replicaID uint64, nonVoting bool) dbconfig.Config {
	return dbconfig.Config{
		ShardID: shardID, ReplicaID: replicaID, CheckQuorum: true, PreVote: true,
		ElectionRTT: c.Tuning.ElectionRTT, HeartbeatRTT: c.Tuning.HeartbeatRTT,
		SnapshotEntries: c.Tuning.SnapshotEntries, CompactionOverhead: c.Tuning.CompactionOverhead,
		OrderedConfigChange: true, MaxInMemLogSize: c.Tuning.MaxInMemLogBytes,
		SnapshotCompressionType: dbconfig.Snappy, IsNonVoting: nonVoting,
	}
}

func (c RuntimeConfig) digest(registryLayout RegistryLayout, member RegistryMember) (string, error) {
	value := struct {
		Version        uint32        `json:"version"`
		DeploymentID   uint64        `json:"deployment_id"`
		RaftAddress    string        `json:"raft_address"`
		NodeHostDir    string        `json:"nodehost_dir"`
		WALDir         string        `json:"wal_dir"`
		StateEngineDir string        `json:"state_engine_dir"`
		RuntimeTuning  RuntimeTuning `json:"runtime_tuning"`
		MutualTLS      bool          `json:"mutual_tls"`
		StaticRegistry bool          `json:"static_registry"`
	}{
		Version: runtimeConfigVersion, DeploymentID: deploymentID(registryLayout.ClusterID, registryLayout.RegistryGeneration),
		RaftAddress: member.RaftEndpoint, NodeHostDir: c.NodeHostDir, WALDir: c.WALDir,
		StateEngineDir: c.StateEngineDir,
		RuntimeTuning:  c.Tuning, MutualTLS: true, StaticRegistry: true,
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func (c RuntimeConfig) attestStorage() error {
	directories := map[string]struct{}{
		filepath.Clean(c.NodeHostDir):           {},
		filepath.Clean(c.StateEngineDir):        {},
		filepath.Dir(c.RegistryLayoutGuardPath): {},
		filepath.Dir(c.EnrollmentPath):          {},
	}
	if c.WALDir != "" {
		directories[filepath.Clean(c.WALDir)] = struct{}{}
	}
	paths := make([]string, 0, len(directories))
	for path := range directories {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return err
		}
	}
	if err := c.StorageAttestor.VerifyEncrypted(paths...); err != nil {
		return fmt.Errorf("raftstore: encrypted storage attestation failed: %w", err)
	}
	return nil
}

func deploymentID(clusterID, registryGeneration string) uint64 {
	digest := sha256.Sum256([]byte("kuasar-raft-deployment-v1\x00" + clusterID + "\x00" + registryGeneration))
	value := binary.BigEndian.Uint64(digest[:8])
	if value == 0 {
		return 1
	}
	return value
}

func pathWithin(directory, path string) bool {
	relative, err := filepath.Rel(filepath.Clean(directory), filepath.Clean(path))
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}
