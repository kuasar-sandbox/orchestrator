package raftstore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

const localEnrollmentVersion = uint32(1)

type EnrollmentMode string

const (
	EnrollmentBootstrap EnrollmentMode = "BOOTSTRAP"
	EnrollmentJoin      EnrollmentMode = "JOIN"
)

type ReplicaStartPlan string

const (
	ReplicaInitial ReplicaStartPlan = "INITIAL"
	ReplicaJoin    ReplicaStartPlan = "JOIN"
)

type ReplicaLocalState string

const (
	ReplicaPlanned  ReplicaLocalState = "PLANNED"
	ReplicaStarting ReplicaLocalState = "STARTING"
	ReplicaActive   ReplicaLocalState = "ACTIVE"
	ReplicaRemoving ReplicaLocalState = "REMOVING"
	ReplicaRemoved  ReplicaLocalState = "REMOVED"
)

type LocalReplicaEnrollment struct {
	ShardID    uint64            `json:"shard_id"`
	ReplicaID  uint64            `json:"replica_id"`
	StartPlan  ReplicaStartPlan  `json:"start_plan"`
	NonVoting  bool              `json:"non_voting"`
	LocalState ReplicaLocalState `json:"local_state"`
}

type LocalEnrollment struct {
	Version               uint32                   `json:"version"`
	ClusterID             string                   `json:"cluster_id"`
	RegistryGeneration    string                   `json:"registry_generation"`
	MemberID              string                   `json:"member_id"`
	DeploymentID          uint64                   `json:"deployment_id"`
	RaftAddress           string                   `json:"raft_address"`
	NodeHostDir           string                   `json:"nodehost_dir"`
	WALDir                string                   `json:"wal_dir"`
	StateEngineDir        string                   `json:"state_engine_dir"`
	RuntimeConfigDigest   string                   `json:"runtime_config_digest"`
	RegistryLayoutVersion uint64                   `json:"registry_layout_version"`
	RegistryLayoutDigest  string                   `json:"registry_layout_digest"`
	Mode                  EnrollmentMode           `json:"mode"`
	Replicas              []LocalReplicaEnrollment `json:"replicas"`
}

func (e LocalEnrollment) Validate() error {
	if e.Version != localEnrollmentVersion || e.ClusterID == "" || e.RegistryGeneration == "" ||
		e.MemberID == "" || e.DeploymentID == 0 || e.RaftAddress == "" ||
		e.NodeHostDir == "" || e.StateEngineDir == "" || !isSHA256(e.RuntimeConfigDigest) || e.RegistryLayoutVersion == 0 ||
		!isSHA256(e.RegistryLayoutDigest) || len(e.Replicas) == 0 {
		return errors.New("raftstore: incomplete local Registry enrollment")
	}
	if e.Mode != EnrollmentBootstrap && e.Mode != EnrollmentJoin {
		return errors.New("raftstore: invalid local enrollment mode")
	}
	for index, replica := range e.Replicas {
		if replica.ReplicaID == 0 || replica.ShardID == 0 ||
			index > 0 && replica.ShardID <= e.Replicas[index-1].ShardID {
			return errors.New("raftstore: local replica enrollment is not unique and ordered")
		}
		if replica.StartPlan != ReplicaInitial && replica.StartPlan != ReplicaJoin {
			return errors.New("raftstore: invalid local replica start plan")
		}
		if replica.StartPlan == ReplicaInitial && (e.Mode != EnrollmentBootstrap || replica.NonVoting) {
			return errors.New("raftstore: local replica plan contradicts enrollment mode")
		}
		switch replica.LocalState {
		case ReplicaPlanned, ReplicaStarting, ReplicaActive, ReplicaRemoving, ReplicaRemoved:
		default:
			return errors.New("raftstore: invalid local replica state")
		}
	}
	return nil
}

func newLocalEnrollment(
	mode EnrollmentMode,
	registryLayout RegistryLayout,
	digest string,
	member RegistryMember,
	config RuntimeConfig,
) (LocalEnrollment, error) {
	if err := registryLayout.Validate(); err != nil {
		return LocalEnrollment{}, err
	}
	configDigest, err := config.digest(registryLayout, member)
	if err != nil {
		return LocalEnrollment{}, err
	}
	startPlan := ReplicaInitial
	nonVoting := false
	if mode == EnrollmentJoin {
		startPlan = ReplicaJoin
		nonVoting = true
	}
	replicas := localReplicas(registryLayout, member, startPlan, nonVoting)
	enrollment := LocalEnrollment{
		Version: localEnrollmentVersion, ClusterID: registryLayout.ClusterID,
		RegistryGeneration: registryLayout.RegistryGeneration, MemberID: member.MemberID,
		DeploymentID: deploymentID(registryLayout.ClusterID, registryLayout.RegistryGeneration),
		RaftAddress:  member.RaftEndpoint, NodeHostDir: config.NodeHostDir, WALDir: config.WALDir,
		StateEngineDir:      config.StateEngineDir,
		RuntimeConfigDigest: configDigest, RegistryLayoutVersion: registryLayout.RegistryLayoutVersion,
		RegistryLayoutDigest: digest, Mode: mode, Replicas: replicas,
	}
	if err := enrollment.Validate(); err != nil {
		return LocalEnrollment{}, err
	}
	return enrollment, nil
}

func (e LocalEnrollment) Matches(registryLayout RegistryLayout, digest string, member RegistryMember, config RuntimeConfig) error {
	if err := e.Validate(); err != nil {
		return err
	}
	configDigest, err := config.digest(registryLayout, member)
	if err != nil {
		return err
	}
	if e.ClusterID != registryLayout.ClusterID || e.RegistryGeneration != registryLayout.RegistryGeneration ||
		e.MemberID != member.MemberID ||
		e.DeploymentID != deploymentID(registryLayout.ClusterID, registryLayout.RegistryGeneration) ||
		e.RaftAddress != member.RaftEndpoint || e.NodeHostDir != config.NodeHostDir || e.WALDir != config.WALDir ||
		e.StateEngineDir != config.StateEngineDir ||
		e.RuntimeConfigDigest != configDigest {
		return errors.New("raftstore: runtime identity differs from durable local enrollment")
	}
	if registryLayout.RegistryLayoutVersion < e.RegistryLayoutVersion || registryLayout.RegistryLayoutVersion == e.RegistryLayoutVersion && digest != e.RegistryLayoutDigest {
		return errors.New("raftstore: local enrollment rejected registryLayout rollback or equivocation")
	}
	return nil
}

func localReplicas(registryLayout RegistryLayout, member RegistryMember, plan ReplicaStartPlan, nonVoting bool) []LocalReplicaEnrollment {
	replicas := make([]LocalReplicaEnrollment, 0)
	if placement, found := replicaPlacementForMember(registryLayout.SystemReplicas, member.MemberID); found {
		replicas = append(replicas, LocalReplicaEnrollment{
			ShardID: SystemRaftShardID, ReplicaID: placement.ReplicaID,
			StartPlan: plan, NonVoting: nonVoting, LocalState: ReplicaPlanned,
		})
	}
	for _, shard := range registryLayout.DataShards {
		if placement, found := replicaPlacementForMember(shard.Replicas, member.MemberID); found {
			replicas = append(replicas, LocalReplicaEnrollment{
				ShardID: DataRaftShardID(shard.ShardID), ReplicaID: placement.ReplicaID,
				StartPlan: plan, NonVoting: nonVoting, LocalState: ReplicaPlanned,
			})
		}
	}
	sort.Slice(replicas, func(i, j int) bool { return replicas[i].ShardID < replicas[j].ShardID })
	return replicas
}

func replicaPlacementForMember(replicas []ReplicaPlacement, memberID string) (ReplicaPlacement, bool) {
	for _, replica := range replicas {
		if replica.MemberID == memberID {
			return replica, true
		}
	}
	return ReplicaPlacement{}, false
}

type EnrollmentStore struct{ Path string }

func (s EnrollmentStore) Load() (*LocalEnrollment, error) {
	raw, err := os.ReadFile(s.Path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var enrollment LocalEnrollment
	if err := decoder.Decode(&enrollment); err != nil {
		return nil, fmt.Errorf("raftstore: decode local enrollment: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("raftstore: local enrollment contains trailing JSON")
	}
	if err := enrollment.Validate(); err != nil {
		return nil, err
	}
	return &enrollment, nil
}

func (s EnrollmentStore) Store(enrollment LocalEnrollment) error {
	if err := enrollment.Validate(); err != nil {
		return err
	}
	raw, err := json.Marshal(enrollment)
	if err != nil {
		return err
	}
	directory := filepath.Dir(s.Path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".enrollment-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(raw); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, s.Path); err != nil {
		return err
	}
	dir, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func directoryEmpty(path string) (bool, error) {
	directory, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	defer directory.Close()
	_, err = directory.Readdirnames(1)
	if errors.Is(err, io.EOF) {
		return true, nil
	}
	return false, err
}
