package controlplane

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/raftstore"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/session"
)

type RecoveryCommandSender interface {
	SendRecoveryCommand(context.Context, session.ServeIdentity, string, uint64, string, *routesync.Command) (routesync.CmdAck, bool, error)
}

type RecoveryCoordinatorConfig struct {
	Interval           time.Duration
	Workers            int
	PerNodeWorkers     int
	PageObjects        uint32
	PageBytes          uint32
	MaxNodeReportBytes uint64
	BytesPerSecond     uint64
	LookupPage         uint32
}

func DefaultRecoveryCoordinatorConfig() RecoveryCoordinatorConfig {
	return RecoveryCoordinatorConfig{
		Interval: 250 * time.Millisecond, Workers: 8, PerNodeWorkers: 1,
		PageObjects: 64, PageBytes: 512 << 10, MaxNodeReportBytes: 64 << 20,
		BytesPerSecond: 16 << 20, LookupPage: 256,
	}
}

type RecoveryCoordinator struct {
	store       *RaftStore
	mesh        *RecoveryMesh
	directory   *session.Directory
	commands    RecoveryCommandSender
	config      RecoveryCoordinatorConfig
	log         *slog.Logger
	operationMu sync.Mutex
}

func NewRecoveryCoordinator(
	store *RaftStore,
	mesh *RecoveryMesh,
	directory *session.Directory,
	commands RecoveryCommandSender,
	config RecoveryCoordinatorConfig,
	log *slog.Logger,
) (*RecoveryCoordinator, error) {
	if store == nil || mesh == nil || directory == nil || commands == nil {
		return nil, errors.New("controlplane: recovery coordinator requires consensus, mesh, Directory, and commands")
	}
	defaults := DefaultRecoveryCoordinatorConfig()
	if config.Interval <= 0 {
		config.Interval = defaults.Interval
	}
	if config.Workers <= 0 || config.Workers > 256 {
		config.Workers = defaults.Workers
	}
	if config.PerNodeWorkers <= 0 || config.PerNodeWorkers > config.Workers {
		config.PerNodeWorkers = defaults.PerNodeWorkers
	}
	if config.PageObjects == 0 || config.PageObjects > routesync.MaxRecoveryPageObjects {
		config.PageObjects = defaults.PageObjects
	}
	if config.PageBytes == 0 || config.PageBytes > routesync.MaxRecoveryPageBytes {
		config.PageBytes = defaults.PageBytes
	}
	if config.MaxNodeReportBytes == 0 {
		config.MaxNodeReportBytes = defaults.MaxNodeReportBytes
	}
	if config.BytesPerSecond < uint64(config.PageBytes) {
		config.BytesPerSecond = defaults.BytesPerSecond
	}
	if config.LookupPage == 0 || config.LookupPage > 4096 {
		config.LookupPage = defaults.LookupPage
	}
	if log == nil {
		log = slog.Default()
	}
	return &RecoveryCoordinator{
		store: store, mesh: mesh, directory: directory, commands: commands, config: config, log: log,
	}, nil
}

func (c *RecoveryCoordinator) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.config.Interval)
	defer ticker.Stop()
	for {
		if err := c.Step(ctx); err != nil && ctx.Err() == nil {
			c.log.Warn("Registry recovery coordinator step", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (c *RecoveryCoordinator) Step(ctx context.Context) error {
	c.operationMu.Lock()
	defer c.operationMu.Unlock()
	return c.step(ctx)
}

func (c *RecoveryCoordinator) step(ctx context.Context) error {
	leader, err := c.store.LocalRecoveryCoordinator()
	if err != nil || !leader {
		return err
	}
	state, err := c.store.ReadSystem(ctx)
	if err != nil || state.Recovery == nil {
		return err
	}
	recovery := *state.Recovery
	switch recovery.Phase {
	case raftstore.RecoveryPreparing:
		return c.prepare(ctx, recovery)
	case raftstore.RecoveryCollecting:
		return c.collect(ctx, state, recovery)
	case raftstore.RecoveryReconciling:
		return c.reconcile(ctx, state, recovery)
	case raftstore.RecoveryFinalizing:
		return c.finalize(ctx, recovery)
	default:
		return errors.New("controlplane: unknown recovery phase")
	}
}

func (c *RecoveryCoordinator) ResolveRecoveryNode(ctx context.Context, request ResolveRecoveryNodeRequest) error {
	c.operationMu.Lock()
	defer c.operationMu.Unlock()

	leader, err := c.store.LocalRecoveryCoordinator()
	if err != nil {
		return err
	}
	if !leader {
		return errors.New("controlplane: recovery node resolution requires the local System Group leader")
	}
	state, err := c.store.ReadSystem(ctx)
	if err != nil {
		return err
	}
	if !operatorRegistryServeIdentityMatches(state, request.RegistryServeIdentity) || state.Recovery == nil {
		return errors.New("controlplane: recovery node resolution targets another epoch")
	}
	progress, found := state.Recovery.Nodes[request.NodeID]
	if !found {
		return errors.New("controlplane: recovery node is not expected")
	}

	switch request.Resolution {
	case string(raftstore.RecoveryNodeMissing):
		return c.resolveRecoveryNodeMissing(ctx, *state.Recovery, progress, request)
	case string(raftstore.RecoveryNodeQuarantined):
		return c.resolveRecoveryNodeQuarantined(ctx, *state.Recovery, progress, request)
	default:
		return errors.New("controlplane: recovery resolution must be MISSING or QUARANTINED")
	}
}

func (c *RecoveryCoordinator) resolveRecoveryNodeMissing(
	ctx context.Context,
	recovery raftstore.RecoveryEpoch,
	progress raftstore.RecoveryNodeProgress,
	request ResolveRecoveryNodeRequest,
) error {
	if progress.State == raftstore.RecoveryNodeMissing {
		if progress.ResolutionProofDigest == request.ProofDigest && progress.ResolutionReason == request.Reason {
			return nil
		}
		return errors.New("controlplane: recovery node was resolved MISSING with different evidence")
	}
	if progress.State != raftstore.RecoveryNodeExpected && progress.State != raftstore.RecoveryNodeCollecting {
		return errors.New("controlplane: only an unreported node can be resolved MISSING")
	}
	if progress.State == raftstore.RecoveryNodeCollecting {
		reset := raftstore.RecoveryNodeStagingReset{
			RecoveryEpoch: recovery.Epoch, NodeID: progress.NodeID, NodeEpoch: progress.NodeEpoch,
			SessionSeq: progress.SessionSeq, Terminal: true,
		}
		if err := c.forEachShard(ctx, func(ctx context.Context, shardID uint32) error {
			result, err := c.mesh.Apply(ctx, raftstore.DataCommand{
				Type: raftstore.DataResetRecoveryNode,
				Identity: raftstore.ShardRequestIdentity{
					PermitIdentity: recoveryPermitIdentity(recovery), ShardID: shardID,
				},
				RecoveryReset: &reset,
			})
			return recoveryApplyError(result, err)
		}); err != nil {
			return err
		}
	}
	if err := c.verifyRecoveryNodeProgress(ctx, recovery, progress); err != nil {
		return err
	}
	return c.store.UpdateRecoveryNode(ctx, raftstore.RecoveryNodeUpdate{
		NodeID: progress.NodeID, EnrollmentID: progress.EnrollmentID, NodeEpoch: progress.NodeEpoch,
		From: progress.State, To: raftstore.RecoveryNodeMissing,
		ResolutionProofDigest: request.ProofDigest, ResolutionReason: request.Reason,
	})
}

func (c *RecoveryCoordinator) resolveRecoveryNodeQuarantined(
	ctx context.Context,
	recovery raftstore.RecoveryEpoch,
	progress raftstore.RecoveryNodeProgress,
	request ResolveRecoveryNodeRequest,
) error {
	if progress.State == raftstore.RecoveryNodeQuarantined {
		if progress.ResolutionProofDigest == request.ProofDigest && progress.ResolutionReason == request.Reason {
			return nil
		}
		return errors.New("controlplane: recovery node was resolved QUARANTINED with different evidence")
	}
	if progress.State != raftstore.RecoveryNodeReported {
		return errors.New("controlplane: only a complete report can be quarantined")
	}
	records := make([]locatedRecoveryRecord, 0)
	var recordsMu sync.Mutex
	if err := c.forEachRecoveryRecord(ctx, recovery, func(_ context.Context, located locatedRecoveryRecord) error {
		if located.record.NodeID != progress.NodeID {
			return nil
		}
		if located.record.NodeEpoch != progress.NodeEpoch || located.record.SessionSeq != progress.SessionSeq ||
			located.record.ReportDigest != progress.ReportDigest {
			return errors.New("recovery record does not match the reported node identity")
		}
		if located.record.State == raftstore.RecoveryObjectActivated {
			return errors.New("an activated recovery projection cannot be retracted by node quarantine")
		}
		recordsMu.Lock()
		records = append(records, located)
		recordsMu.Unlock()
		return nil
	}); err != nil {
		return err
	}
	if uint64(len(records)) != progress.ReportedObjects {
		return fmt.Errorf(
			"controlplane: recovery node report contains %d objects but %d exact records were found",
			progress.ReportedObjects,
			len(records),
		)
	}
	for _, located := range records {
		if err := c.quarantineObject(ctx, recovery, located, request.Reason); err != nil {
			return err
		}
	}
	if err := c.verifyRecoveryNodeProgress(ctx, recovery, progress); err != nil {
		return err
	}
	return c.store.UpdateRecoveryNode(ctx, raftstore.RecoveryNodeUpdate{
		NodeID: progress.NodeID, EnrollmentID: progress.EnrollmentID, NodeEpoch: progress.NodeEpoch,
		From: raftstore.RecoveryNodeReported, To: raftstore.RecoveryNodeQuarantined,
		SessionSeq: progress.SessionSeq, ReportDigest: progress.ReportDigest,
		ReportedObjects: progress.ReportedObjects, ConflictObjects: progress.ReportedObjects,
		ResolutionProofDigest: request.ProofDigest, ResolutionReason: request.Reason,
	})
}

func (c *RecoveryCoordinator) verifyRecoveryNodeProgress(
	ctx context.Context,
	recovery raftstore.RecoveryEpoch,
	want raftstore.RecoveryNodeProgress,
) error {
	state, err := c.store.ReadSystem(ctx)
	if err != nil {
		return err
	}
	if state.Recovery == nil || state.Recovery.Epoch != recovery.Epoch {
		return errors.New("controlplane: recovery epoch changed during node resolution")
	}
	current, found := state.Recovery.Nodes[want.NodeID]
	if !found || current.NodeID != want.NodeID || current.EnrollmentID != want.EnrollmentID ||
		current.NodeEpoch != want.NodeEpoch || current.SessionSeq != want.SessionSeq || current.State != want.State ||
		current.ReportDigest != want.ReportDigest || current.ReportedObjects != want.ReportedObjects {
		return errors.New("controlplane: recovery node progress changed during resolution")
	}
	return nil
}

func (c *RecoveryCoordinator) prepare(ctx context.Context, recovery raftstore.RecoveryEpoch) error {
	if !recovery.PermitDrainComplete {
		_, err := c.store.ConfirmRecoveryPermitDrain(ctx)
		return err
	}
	target := recoveryPermitIdentity(recovery)
	start := raftstore.DataRecoveryState{
		RecoveryEpoch: recovery.Epoch, SourceClusterID: recovery.SourceClusterID,
		SourceRegistryGeneration:   recovery.SourceRegistryGeneration,
		SourceRegistryLayoutDigest: recovery.SourceRegistryLayoutDigest, Target: target,
	}
	err := c.forEachShard(ctx, func(ctx context.Context, shardID uint32) error {
		result, err := c.mesh.Apply(ctx, raftstore.DataCommand{
			Type:          raftstore.DataBeginRecovery,
			Identity:      raftstore.ShardRequestIdentity{PermitIdentity: target, ShardID: shardID},
			RecoveryStart: &start,
		})
		return recoveryApplyError(result, err)
	})
	if err != nil {
		return err
	}
	_, err = c.store.AdvanceRecovery(ctx, raftstore.RecoveryPreparing, raftstore.RecoveryCollecting)
	return err
}

func (c *RecoveryCoordinator) collect(
	ctx context.Context,
	state raftstore.SystemState,
	recovery raftstore.RecoveryEpoch,
) error {
	nodeIDs := sortedRecoveryNodes(recovery.Nodes)
	sem := make(chan struct{}, c.config.Workers)
	var workers sync.WaitGroup
	var joined error
	var resultMu sync.Mutex
	for _, nodeID := range nodeIDs {
		progress := recovery.Nodes[nodeID]
		if progress.State != raftstore.RecoveryNodeExpected && progress.State != raftstore.RecoveryNodeCollecting {
			continue
		}
		enrollment := state.NodeEnrollments[nodeID]
		sem <- struct{}{}
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer func() { <-sem }()
			if err := c.collectNode(ctx, recovery, enrollment, progress); err != nil {
				resultMu.Lock()
				joined = errors.Join(joined, fmt.Errorf("node %s: %w", nodeID, err))
				resultMu.Unlock()
			}
		}()
	}
	workers.Wait()
	if joined != nil {
		return joined
	}
	current, err := c.store.ReadSystem(ctx)
	if err != nil || current.Recovery == nil || current.Recovery.Phase != raftstore.RecoveryCollecting {
		return err
	}
	if recoveryNodesCollected(current.Recovery.Nodes) {
		_, err = c.store.AdvanceRecovery(ctx, raftstore.RecoveryCollecting, raftstore.RecoveryReconciling)
	}
	return err
}

func (c *RecoveryCoordinator) collectNode(
	ctx context.Context,
	recovery raftstore.RecoveryEpoch,
	enrollment raftstore.NodeEnrollmentRecord,
	progress raftstore.RecoveryNodeProgress,
) error {
	entry, available := c.directory.Lookup(progress.NodeID)
	if !available || entry.NodeEpoch != progress.NodeEpoch {
		return nil
	}
	if progress.State == raftstore.RecoveryNodeExpected {
		if err := c.store.UpdateRecoveryNode(ctx, raftstore.RecoveryNodeUpdate{
			NodeID: progress.NodeID, EnrollmentID: progress.EnrollmentID, NodeEpoch: progress.NodeEpoch,
			From: raftstore.RecoveryNodeExpected, To: raftstore.RecoveryNodeCollecting,
			SessionSeq: entry.SessionSeq,
		}); err != nil {
			return err
		}
	} else if progress.State == raftstore.RecoveryNodeCollecting && progress.SessionSeq != entry.SessionSeq {
		if err := c.store.UpdateRecoveryNode(ctx, raftstore.RecoveryNodeUpdate{
			NodeID: progress.NodeID, EnrollmentID: progress.EnrollmentID, NodeEpoch: progress.NodeEpoch,
			From: raftstore.RecoveryNodeCollecting, To: raftstore.RecoveryNodeCollecting,
			SessionSeq: entry.SessionSeq,
		}); err != nil {
			return err
		}
		return nil
	}
	pageRequest := routesync.RecoveryReportRequest{
		RecoveryEpoch: recovery.Epoch, SourceClusterID: recovery.SourceClusterID,
		SourceRegistryGeneration:   recovery.SourceRegistryGeneration,
		SourceRegistryLayoutDigest: recovery.SourceRegistryLayoutDigest,
		TargetRegistryGeneration:   recovery.TargetRegistryGeneration,
		TargetRegistryLayoutDigest: recovery.TargetRegistryLayoutDigest,
		Limit:                      c.config.PageObjects, MaxBytes: c.config.PageBytes,
	}
	identity := recoveryServeIdentity(recovery)
	facts := make([]routesync.RecoveryExecutionFact, 0)
	reportBytes := uint64(2) // JSON array brackets.
	var reportDigest string
	var reportSession uint64
	transferStarted := time.Now()
	var transferredBytes uint64
	for {
		command := &routesync.Command{CmdID: newCommandID(), Kind: routesync.CmdCollectRecovery, Recovery: &pageRequest}
		ack, _, err := c.commands.SendRecoveryCommand(
			ctx, identity, progress.NodeID, progress.NodeEpoch, enrollment.DataEndpoint, command,
		)
		if err != nil {
			return err
		}
		if ack.Status != routesync.AckAccepted || ack.Recovery == nil {
			return fmt.Errorf("node rejected recovery report: %s", ack.Reason)
		}
		page := *ack.Recovery
		if err := page.ValidateFor(pageRequest, progress.NodeID, progress.NodeEpoch, entry.SessionSeq); err != nil {
			return err
		}
		encodedPage, err := json.Marshal(page)
		if err != nil {
			return err
		}
		transferredBytes += uint64(len(encodedPage))
		if err := waitForRecoveryByteRate(ctx, transferStarted, transferredBytes, c.config.BytesPerSecond); err != nil {
			return err
		}
		if reportDigest == "" {
			reportDigest = page.ReportDigest
			reportSession = entry.SessionSeq
			pageRequest.ExpectedReportDigest = reportDigest
		} else if page.ReportDigest != reportDigest {
			return errors.New("node report digest changed during collection")
		}
		for _, fact := range page.Objects {
			encoded, err := json.Marshal(fact)
			if err != nil {
				return err
			}
			if len(facts) != 0 {
				reportBytes++
			}
			reportBytes += uint64(len(encoded))
			if reportBytes > c.config.MaxNodeReportBytes {
				return errors.New("node recovery report exceeds configured byte budget")
			}
			facts = append(facts, fact)
		}
		if page.Complete {
			if uint64(len(facts)) != page.TotalObjects {
				return errors.New("node recovery report object count changed")
			}
			break
		}
		pageRequest.Offset = page.NextOffset
	}
	wantDigest, err := routesync.CanonicalRecoveryReportDigest(facts)
	if err != nil || wantDigest != reportDigest {
		return errors.Join(err, errors.New("node recovery report digest mismatch"))
	}
	records := make([]raftstore.RecoveryObjectRecord, 0, len(facts))
	affectedShards := make(map[uint32]struct{})
	var affectedShardsMu sync.Mutex
	for _, fact := range facts {
		page := routesync.RecoveryReportPage{
			RecoveryEpoch: recovery.Epoch, SourceClusterID: recovery.SourceClusterID,
			SourceRegistryGeneration:   recovery.SourceRegistryGeneration,
			SourceRegistryLayoutDigest: recovery.SourceRegistryLayoutDigest,
			TargetRegistryGeneration:   recovery.TargetRegistryGeneration,
			TargetRegistryLayoutDigest: recovery.TargetRegistryLayoutDigest,
			NodeID:                     progress.NodeID, NodeEpoch: progress.NodeEpoch, SessionSeq: reportSession,
			ReportDigest: reportDigest, TotalObjects: uint64(len(facts)),
		}
		record, err := recoveryRecordFromFact(recovery, page, fact)
		if err != nil {
			return err
		}
		records = append(records, record)
		affectedShards[recoveryRecordShard(c.store, record)] = struct{}{}
	}
	if err := c.forEachRecoveryRecord(ctx, recovery, func(_ context.Context, located locatedRecoveryRecord) error {
		if located.record.NodeID == progress.NodeID && located.record.NodeEpoch == progress.NodeEpoch {
			affectedShardsMu.Lock()
			affectedShards[located.shardID] = struct{}{}
			affectedShardsMu.Unlock()
		}
		return nil
	}); err != nil {
		return err
	}
	shards := make([]uint32, 0, len(affectedShards))
	for shardID := range affectedShards {
		shards = append(shards, shardID)
	}
	slices.Sort(shards)
	reset := raftstore.RecoveryNodeStagingReset{
		RecoveryEpoch: recovery.Epoch, NodeID: progress.NodeID, NodeEpoch: progress.NodeEpoch,
		SessionSeq: reportSession,
	}
	if err := c.forEachRecoveryShard(ctx, shards, func(ctx context.Context, shardID uint32) error {
		result, err := c.mesh.Apply(ctx, raftstore.DataCommand{
			Type: raftstore.DataResetRecoveryNode,
			Identity: raftstore.ShardRequestIdentity{
				PermitIdentity: recoveryPermitIdentity(recovery), ShardID: shardID,
			},
			RecoveryReset: &reset,
		})
		return recoveryApplyError(result, err)
	}); err != nil {
		return err
	}
	for _, record := range records {
		identity := raftstore.ShardRequestIdentity{
			PermitIdentity: recoveryPermitIdentity(recovery), ShardID: recoveryRecordShard(c.store, record),
		}
		result, err := c.mesh.Apply(ctx, raftstore.DataCommand{
			Type: raftstore.DataStageRecovery, Identity: identity, RecoveryRecord: &record,
		})
		if err := recoveryApplyError(result, err); err != nil {
			return err
		}
	}
	return c.store.UpdateRecoveryNode(ctx, raftstore.RecoveryNodeUpdate{
		NodeID: progress.NodeID, EnrollmentID: progress.EnrollmentID, NodeEpoch: progress.NodeEpoch,
		From: raftstore.RecoveryNodeCollecting, To: raftstore.RecoveryNodeReported,
		SessionSeq: reportSession, ReportDigest: reportDigest, ReportedObjects: uint64(len(facts)),
	})
}

func waitForRecoveryByteRate(ctx context.Context, started time.Time, transferred, bytesPerSecond uint64) error {
	if transferred == 0 || bytesPerSecond == 0 {
		return nil
	}
	want := time.Duration((transferred*uint64(time.Second) + bytesPerSecond - 1) / bytesPerSecond)
	wait := want - time.Since(started)
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *RecoveryCoordinator) reconcile(
	ctx context.Context,
	state raftstore.SystemState,
	recovery raftstore.RecoveryEpoch,
) error {
	nodeSlots := make(map[string]chan struct{}, len(recovery.Nodes))
	for nodeID := range recovery.Nodes {
		nodeSlots[nodeID] = make(chan struct{}, c.config.PerNodeWorkers)
	}
	identity := recoveryServeIdentity(recovery)
	err := c.forEachRecoveryRecord(ctx, recovery, func(ctx context.Context, located locatedRecoveryRecord) error {
		slot, found := nodeSlots[located.record.NodeID]
		if !found {
			return fmt.Errorf("record references node %q outside the recovery epoch", located.record.NodeID)
		}
		select {
		case slot <- struct{}{}:
			defer func() { <-slot }()
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := c.reconcileRecord(ctx, recovery, identity, located); err != nil {
			return fmt.Errorf(
				"%v %s/%s on node %s: %w",
				located.record.Kind, located.record.Group, located.record.ObjectID, located.record.NodeID, err,
			)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return c.updateReconciledNodes(ctx, state, recovery)
}

func (c *RecoveryCoordinator) reconcileRecord(
	ctx context.Context,
	recovery raftstore.RecoveryEpoch,
	identity session.ServeIdentity,
	located locatedRecoveryRecord,
) error {
	record := located.record
	switch record.State {
	case raftstore.RecoveryObjectStaged:
		command, err := recoveryRebindCommand(record)
		if err != nil {
			return err
		}
		ack, _, sendErr := c.commands.SendRecoveryCommand(
			ctx, identity, record.NodeID, record.NodeEpoch, record.TargetBinding.DataEndpoint, command,
		)
		if sendErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return nil
		}
		if ack.Status != routesync.AckAccepted {
			switch ack.Outcome {
			case routesync.DispatchSessionMoved, routesync.DispatchUnknown:
				return nil
			case routesync.DispatchWrongBinding, routesync.DispatchConflict:
				return c.quarantineObject(ctx, recovery, located, ack.Reason)
			default:
				return errors.New("node returned an invalid recovery rebind outcome")
			}
		}
		route, build, err := refreshedRecoveryProjection(record, ack)
		if err != nil {
			if quarantineErr := c.quarantineObject(ctx, recovery, located, err.Error()); quarantineErr != nil {
				return errors.Join(err, quarantineErr)
			}
			return nil
		}
		update := recoveryObjectUpdate(record)
		update.Route, update.Build = route, build
		result, err := c.mesh.Apply(ctx, raftstore.DataCommand{
			Type: raftstore.DataAckRecovery, Identity: recoveryShardIdentity(recovery, located.shardID, false),
			RecoveryUpdate: &update,
		})
		if err := recoveryApplyError(result, err); err != nil {
			return fmt.Errorf("commit rebind acknowledgement on shard %d: %w", located.shardID, err)
		}
		record.Route, record.Build, record.State = route, build, raftstore.RecoveryObjectRebound
		record.EventSeq = recoveryProjectionEventSeq(route, build)
		fallthrough
	case raftstore.RecoveryObjectRebound:
		if record.Kind == clusterstate.ExecutionKindBuild {
			return c.activateRecoveryRecord(ctx, recovery, located.shardID, record)
		}
		command, err := recoveryEventAckCommand(record)
		if err != nil {
			return err
		}
		ack, _, sendErr := c.commands.SendRecoveryCommand(
			ctx, identity, record.NodeID, record.NodeEpoch, record.TargetBinding.DataEndpoint, command,
		)
		if sendErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return nil
		}
		if ack.Status != routesync.AckAccepted {
			switch ack.Outcome {
			case routesync.DispatchSessionMoved, routesync.DispatchUnknown:
				return nil
			case routesync.DispatchWrongBinding, routesync.DispatchConflict:
				return c.quarantineObject(ctx, recovery, located, ack.Reason)
			default:
				return errors.New("node returned an invalid recovery event ACK outcome")
			}
		}
		refreshedRoute, refreshedBuild, err := refreshedRecoveryProjection(record, ack)
		if err != nil {
			if quarantineErr := c.quarantineObject(ctx, recovery, located, err.Error()); quarantineErr != nil {
				return errors.Join(err, quarantineErr)
			}
			return nil
		}
		refreshedSeq := recoveryProjectionEventSeq(refreshedRoute, refreshedBuild)
		update := recoveryObjectUpdate(record)
		update.Route, update.Build = refreshedRoute, refreshedBuild
		result, err := c.mesh.Apply(ctx, raftstore.DataCommand{
			Type: raftstore.DataAckRecovery, Identity: recoveryShardIdentity(recovery, located.shardID, false),
			RecoveryUpdate: &update,
		})
		if err := recoveryApplyError(result, err); err != nil {
			return fmt.Errorf("commit durable event on shard %d: %w", located.shardID, err)
		}
		if refreshedSeq > record.EventSeq {
			return nil
		}
		return c.activateRecoveryRecord(ctx, recovery, located.shardID, record)
	}
	return nil
}

func (c *RecoveryCoordinator) activateRecoveryRecord(
	ctx context.Context,
	recovery raftstore.RecoveryEpoch,
	shardID uint32,
	record raftstore.RecoveryObjectRecord,
) error {
	update := recoveryObjectUpdate(record)
	result, err := c.mesh.Apply(ctx, raftstore.DataCommand{
		Type: raftstore.DataActivateRecovery, Identity: recoveryShardIdentity(recovery, shardID, false),
		RecoveryUpdate: &update,
	})
	if err := recoveryApplyError(result, err); err != nil {
		return fmt.Errorf("activate projection on shard %d: %w", shardID, err)
	}
	return nil
}

func (c *RecoveryCoordinator) finalize(ctx context.Context, recovery raftstore.RecoveryEpoch) error {
	final := raftstore.RecoveryFinalization{
		RecoveryEpoch: recovery.Epoch, SourceRegistryGeneration: recovery.SourceRegistryGeneration,
		SourceRegistryLayoutDigest: recovery.SourceRegistryLayoutDigest,
	}
	err := c.forEachShard(ctx, func(ctx context.Context, shardID uint32) error {
		result, err := c.mesh.Apply(ctx, raftstore.DataCommand{
			Type:     raftstore.DataFinalizeRecovery,
			Identity: recoveryShardIdentity(recovery, shardID, true), RecoveryFinal: &final,
		})
		return recoveryApplyError(result, err)
	})
	if err != nil {
		return err
	}
	_, err = c.store.AdvanceRecovery(ctx, raftstore.RecoveryFinalizing, raftstore.RecoveryClosed)
	return err
}

type locatedRecoveryRecord struct {
	shardID uint32
	record  raftstore.RecoveryObjectRecord
}

func (c *RecoveryCoordinator) forEachRecoveryRecord(
	ctx context.Context,
	recovery raftstore.RecoveryEpoch,
	visit func(context.Context, locatedRecoveryRecord) error,
) error {
	if visit == nil {
		return errors.New("controlplane: recovery record visitor is required")
	}
	return c.forEachShard(ctx, func(ctx context.Context, shardID uint32) error {
		after := ""
		for {
			page, err := c.mesh.Read(ctx, raftstore.RecoveryLookup{
				Identity: recoveryShardIdentity(recovery, shardID, false),
				AfterKey: after, Limit: c.config.LookupPage,
			})
			if err != nil {
				return err
			}
			if !page.Available {
				return errors.New(page.Reason)
			}
			for _, record := range page.Records {
				if err := visit(ctx, locatedRecoveryRecord{shardID: shardID, record: record}); err != nil {
					return err
				}
			}
			if page.NextKey == "" {
				return nil
			}
			if page.NextKey <= after {
				return errors.New("recovery lookup returned a non-advancing cursor")
			}
			after = page.NextKey
		}
	})
}

func (c *RecoveryCoordinator) updateReconciledNodes(
	ctx context.Context,
	state raftstore.SystemState,
	recovery raftstore.RecoveryEpoch,
) error {
	type counts struct{ resolved, conflicts, unresolved uint64 }
	byNode := make(map[string]counts)
	var countsMu sync.Mutex
	err := c.forEachRecoveryRecord(ctx, recovery, func(_ context.Context, located locatedRecoveryRecord) error {
		countsMu.Lock()
		defer countsMu.Unlock()
		current := byNode[located.record.NodeID]
		switch located.record.State {
		case raftstore.RecoveryObjectActivated, raftstore.RecoveryObjectTerminal:
			current.resolved++
		case raftstore.RecoveryObjectQuarantined:
			current.conflicts++
		default:
			current.unresolved++
		}
		byNode[located.record.NodeID] = current
		return nil
	})
	if err != nil {
		return err
	}
	for _, nodeID := range sortedRecoveryNodes(recovery.Nodes) {
		progress := recovery.Nodes[nodeID]
		if progress.State != raftstore.RecoveryNodeReported {
			continue
		}
		count := byNode[nodeID]
		if count.unresolved != 0 || count.resolved+count.conflicts != progress.ReportedObjects {
			continue
		}
		to := raftstore.RecoveryNodeReconciled
		if count.conflicts != 0 {
			to = raftstore.RecoveryNodeQuarantined
		}
		update := raftstore.RecoveryNodeUpdate{
			NodeID: nodeID, EnrollmentID: progress.EnrollmentID, NodeEpoch: progress.NodeEpoch,
			From: raftstore.RecoveryNodeReported, To: to, SessionSeq: progress.SessionSeq,
			ReportDigest: progress.ReportDigest, ReportedObjects: progress.ReportedObjects,
			ResolvedObjects: count.resolved, ConflictObjects: count.conflicts,
		}
		if to == raftstore.RecoveryNodeQuarantined {
			update.ResolutionProofDigest = recoveryNodeConflictProof(progress, count.resolved, count.conflicts)
			update.ResolutionReason = "one or more reported executions are quarantined"
		}
		if err := c.store.UpdateRecoveryNode(ctx, update); err != nil {
			return err
		}
	}
	current, err := c.store.ReadSystem(ctx)
	if err != nil || current.Recovery == nil {
		return err
	}
	if recoveryNodesReconciled(current.Recovery.Nodes) {
		_, err = c.store.AdvanceRecovery(ctx, raftstore.RecoveryReconciling, raftstore.RecoveryFinalizing)
	}
	return err
}

func recoveryNodeConflictProof(progress raftstore.RecoveryNodeProgress, resolved, conflicts uint64) string {
	encoded, _ := json.Marshal(struct {
		Domain          string `json:"domain"`
		NodeID          string `json:"node_id"`
		NodeEpoch       uint64 `json:"node_epoch"`
		ReportDigest    string `json:"report_digest"`
		ReportedObjects uint64 `json:"reported_objects"`
		ResolvedObjects uint64 `json:"resolved_objects"`
		ConflictObjects uint64 `json:"conflict_objects"`
	}{
		Domain: "kuasar-recovery-node-conflict-v1", NodeID: progress.NodeID, NodeEpoch: progress.NodeEpoch,
		ReportDigest: progress.ReportDigest, ReportedObjects: progress.ReportedObjects,
		ResolvedObjects: resolved, ConflictObjects: conflicts,
	})
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func (c *RecoveryCoordinator) quarantineObject(
	ctx context.Context,
	recovery raftstore.RecoveryEpoch,
	located locatedRecoveryRecord,
	reason string,
) error {
	if reason == "" {
		reason = "node rejected exact recovery Binding"
	}
	update := recoveryObjectUpdate(located.record)
	update.Reason = reason
	result, err := c.mesh.Apply(ctx, raftstore.DataCommand{
		Type:     raftstore.DataQuarantineRecovery,
		Identity: recoveryShardIdentity(recovery, located.shardID, false), RecoveryUpdate: &update,
	})
	return recoveryApplyError(result, err)
}

func (c *RecoveryCoordinator) forEachShard(ctx context.Context, run func(context.Context, uint32) error) error {
	shards := make([]uint32, c.store.VirtualShardCount())
	for shardID := range shards {
		shards[shardID] = uint32(shardID)
	}
	return c.forEachRecoveryShard(ctx, shards, run)
}

func (c *RecoveryCoordinator) forEachRecoveryShard(
	ctx context.Context,
	shards []uint32,
	run func(context.Context, uint32) error,
) error {
	sem := make(chan struct{}, c.config.Workers)
	var workers sync.WaitGroup
	var resultMu sync.Mutex
	var joined error
	for _, shardID := range shards {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			workers.Wait()
			return ctx.Err()
		}
		workers.Add(1)
		go func(shardID uint32) {
			defer workers.Done()
			defer func() { <-sem }()
			if err := run(ctx, shardID); err != nil {
				resultMu.Lock()
				joined = errors.Join(joined, fmt.Errorf("shard %d: %w", shardID, err))
				resultMu.Unlock()
			}
		}(shardID)
	}
	workers.Wait()
	return joined
}

func recoveryPermitIdentity(recovery raftstore.RecoveryEpoch) raftstore.PermitIdentity {
	return raftstore.PermitIdentity{
		ClusterID: recovery.SourceClusterID, RegistryGeneration: recovery.TargetRegistryGeneration,
		SystemEpoch: recovery.Epoch, RegistryLayoutDigest: recovery.TargetRegistryLayoutDigest,
	}
}

func recoveryServeIdentity(recovery raftstore.RecoveryEpoch) session.ServeIdentity {
	identity := recoveryPermitIdentity(recovery)
	return session.ServeIdentity{
		ClusterID: identity.ClusterID, RegistryGeneration: identity.RegistryGeneration,
		SystemEpoch: identity.SystemEpoch, RegistryLayoutDigest: identity.RegistryLayoutDigest,
	}
}

func recoveryShardIdentity(recovery raftstore.RecoveryEpoch, shardID uint32, final bool) raftstore.ShardRequestIdentity {
	identity := recoveryPermitIdentity(recovery)
	if final {
		identity.SystemEpoch++
	}
	return raftstore.ShardRequestIdentity{PermitIdentity: identity, ShardID: shardID}
}

func recoveryRecordShard(store *RaftStore, record raftstore.RecoveryObjectRecord) uint32 {
	registryLayout, _ := store.RegistryLayoutSnapshot()
	if record.Kind == clusterstate.ExecutionKindSandbox {
		_, shardID, _ := clusterstate.RouteShardFor(record.Group, record.RouteKey, registryLayout.RouteBucketCount, registryLayout.VirtualShardCount)
		return shardID
	}
	_, shardID, _ := clusterstate.BuildShardFor(record.Group, record.ObjectID, registryLayout.BuildBucketCount, registryLayout.VirtualShardCount)
	return shardID
}

func recoveryRebindCommand(record raftstore.RecoveryObjectRecord) (*routesync.Command, error) {
	binding, err := clusterstate.DecodeExecutionBinding(record.TargetBinding.OpaqueBinding)
	if err != nil {
		return nil, err
	}
	command := &routesync.Command{
		CmdID: newCommandID(), Kind: routesync.CmdRebindExecution,
		RegistryGeneration: record.TargetBinding.RegistryGeneration,
		Binding:            record.TargetBinding.OpaqueBinding, BindingDigest: record.TargetBinding.BindingDigest,
		OldBindingDigest:   record.SourceBindingDigest,
		DemandDigest:       hex.EncodeToString(binding.DemandDigest[:]),
		DispatchSpecDigest: hex.EncodeToString(binding.DispatchSpecDigest[:]),
		Group:              record.Group, RouteKey: record.RouteKey,
	}
	if record.Kind == clusterstate.ExecutionKindSandbox {
		command.SID = record.ObjectID
	} else {
		command.BuildID = record.ObjectID
	}
	return command, nil
}

func recoveryEventAckCommand(record raftstore.RecoveryObjectRecord) (*routesync.Command, error) {
	if record.Kind != clusterstate.ExecutionKindSandbox || record.State != raftstore.RecoveryObjectRebound || record.EventSeq == 0 {
		return nil, errors.New("controlplane: recovery event ACK requires a rebound target projection")
	}
	command := &routesync.Command{
		CmdID: newCommandID(), Kind: routesync.CmdAckRecoveryEvent,
		RegistryGeneration: record.TargetBinding.RegistryGeneration,
		BindingDigest:      record.TargetBinding.BindingDigest,
		EventAck: &routesync.EventAck{
			ObjectKind: "sandbox", ObjectID: record.ObjectID,
			RegistryGeneration: record.TargetBinding.RegistryGeneration,
			BindingDigest:      record.TargetBinding.BindingDigest, EventSeq: record.EventSeq,
		},
	}
	command.SID = record.ObjectID
	return command, nil
}

func recoveryProjectionEventSeq(route *clusterstate.RouteWorkflowRecord, _ *clusterstate.BuildRecord) uint64 {
	if route != nil {
		if route.Ready != nil {
			return route.Ready.LastEventSeq
		}
		if route.Paused != nil {
			return route.Paused.Execution.LastEventSeq
		}
	}
	return 0
}

func recoveryObjectUpdate(record raftstore.RecoveryObjectRecord) raftstore.RecoveryObjectUpdate {
	return raftstore.RecoveryObjectUpdate{
		Kind: record.Kind, Group: record.Group, RouteKey: record.RouteKey, ObjectID: record.ObjectID,
		RecoveryEpoch: record.RecoveryEpoch, ReportDigest: record.ReportDigest,
		SourceBindingDigest: record.SourceBindingDigest,
		TargetBindingDigest: record.TargetBinding.BindingDigest,
	}
}

func recoveryApplyError(result raftstore.DataApplyResult, err error) error {
	if err != nil {
		return err
	}
	if result.Conflict || !result.Applied {
		return errors.New(result.Reason)
	}
	return nil
}

func recoveryNodesCollected(nodes map[string]raftstore.RecoveryNodeProgress) bool {
	for _, progress := range nodes {
		if progress.State != raftstore.RecoveryNodeReported && progress.State != raftstore.RecoveryNodeMissing &&
			progress.State != raftstore.RecoveryNodeQuarantined {
			return false
		}
	}
	return true
}

func recoveryNodesReconciled(nodes map[string]raftstore.RecoveryNodeProgress) bool {
	for _, progress := range nodes {
		if progress.State != raftstore.RecoveryNodeReconciled && progress.State != raftstore.RecoveryNodeMissing &&
			progress.State != raftstore.RecoveryNodeQuarantined {
			return false
		}
	}
	return true
}

func sortedRecoveryNodes(nodes map[string]raftstore.RecoveryNodeProgress) []string {
	result := make([]string, 0, len(nodes))
	for nodeID := range nodes {
		result = append(result, nodeID)
	}
	slices.Sort(result)
	return result
}
