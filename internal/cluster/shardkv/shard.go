package shardkv

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"sync"
)

type Shard struct {
	store     *Store
	namespace Namespace
	shard     ShardKey
}

type RecordSet struct {
	store     *Store
	namespace Namespace
	shard     ShardKey
	name      RecordSetName
}

func (s *Shard) Namespace() Namespace { return s.namespace }

func (s *Shard) Key() ShardKey { return s.shard }

func (s *Shard) RecordSet(name RecordSetName) (*RecordSet, error) {
	if name == "" {
		return nil, ErrInvalidView
	}
	return &RecordSet{store: s.store, namespace: s.namespace, shard: s.shard, name: name}, nil
}

func (s *RecordSet) Namespace() Namespace { return s.namespace }

func (s *RecordSet) ShardKey() ShardKey { return s.shard }

func (s *RecordSet) Name() RecordSetName { return s.name }

func (s *Shard) View(ctx context.Context) (ShardView, error) {
	if err := ctx.Err(); err != nil {
		return ShardView{}, err
	}
	view, err := s.store.resolver.ResolveShard(s.namespace, s.shard)
	if err != nil {
		return ShardView{}, err
	}
	return view, validateView(view)
}

func (s *Shard) LocalOwner(ctx context.Context) (bool, error) {
	view, err := s.View(ctx)
	if err != nil {
		return false, err
	}
	return localInWriteView(view, s.store.local), nil
}

func (s *RecordSet) View(ctx context.Context) (ShardView, error) {
	if err := ctx.Err(); err != nil {
		return ShardView{}, err
	}
	view, err := s.store.resolver.ResolveShard(s.namespace, s.shard)
	if err != nil {
		return ShardView{}, err
	}
	return view, validateView(view)
}

func (s *RecordSet) LocalOwner(ctx context.Context) (bool, error) {
	view, err := s.View(ctx)
	if err != nil {
		return false, err
	}
	return localInWriteView(view, s.store.local), nil
}

func (s *RecordSet) Get(ctx context.Context, key RecordKey, opts ...ReadOptions) (Record, bool, error) {
	best, found, err := s.GetRecord(ctx, key, opts...)
	if err != nil || !found || best.Deleted {
		return Record{}, false, err
	}
	return best, true, nil
}

func (s *RecordSet) GetRecord(ctx context.Context, key RecordKey, opts ...ReadOptions) (Record, bool, error) {
	readOpt, err := normalizeReadOptions(opts)
	if err != nil {
		return Record{}, false, err
	}
	if readOpt.Policy == ReadLocal || readOpt.MinRev > 0 {
		view, err := s.View(ctx)
		if err != nil {
			return Record{}, false, err
		}
		rec, found, ready, err := s.getLocalReadyRecord(view, key, readOpt.MinRev)
		if ready {
			return rec, found, nil
		}
		if readOpt.Policy == ReadLocal {
			if err != nil {
				return Record{}, false, err
			}
			return Record{}, false, ErrLocalViewBehind
		}
	}
	return s.getRecordQuorum(ctx, key)
}

func normalizeReadOptions(opts []ReadOptions) (ReadOptions, error) {
	if len(opts) == 0 {
		return ReadOptions{}, nil
	}
	opt := opts[len(opts)-1]
	switch opt.Policy {
	case ReadDefault, ReadLocal:
		return opt, nil
	default:
		return ReadOptions{}, ErrInvalidView
	}
}

func (s *RecordSet) getLocalReadyRecord(view ShardView, key RecordKey, minRev uint64) (Record, bool, bool, error) {
	if !localInWriteView(view, s.store.local) {
		return Record{}, false, false, ErrInvalidView
	}
	local, err := s.store.getLocalShardNoTouch(s.namespace, s.shard)
	if err != nil {
		return Record{}, false, false, err
	}
	localSet := local.recordSetNoTouch(s.name)
	if localSet == nil {
		return Record{}, false, false, nil
	}
	rec, found, _, ready := localSet.readReady(view.Label, key, minRev)
	return rec, found, ready, nil
}

func (s *RecordSet) getRecordQuorum(ctx context.Context, key RecordKey) (Record, bool, error) {
	for attempt := 0; attempt < s.store.maxAttempts; attempt++ {
		view, err := s.View(ctx)
		if err != nil {
			return Record{}, false, err
		}
		reads, success := s.read(ctx, view, key)
		if !quorumSatisfied(success, view.WriteSets) {
			return Record{}, false, ErrQuorum
		}
		best, found := highest(reads)
		if found && recordVisibleQuorum(best, reads, view.WriteSets) {
			s.repairRecordBestEffort(ctx, view, best, recordRepairTargets(best, reads))
			return cloneRecord(best), true, nil
		}
		if !found && quorumSatisfied(success, view.ReadSets) {
			return Record{}, false, nil
		}
		records, head, certificate, ok, err := s.fetchCommittedSnapshot(ctx, view)
		if err != nil {
			return Record{}, false, err
		}
		if !ok {
			return Record{}, false, ErrQuorum
		}
		s.installSnapshotBestEffort(ctx, view, records, head, certificate)
		rec, found := recordFromSnapshot(records, key)
		if !found {
			return Record{}, false, nil
		}
		return cloneRecord(rec), true, nil
	}
	return Record{}, false, ErrConflict
}

func (s *RecordSet) CAS(ctx context.Context, key RecordKey, expectRev uint64, value []byte) (Record, bool, error) {
	return s.cas(ctx, key, expectRev, value, false)
}

func (s *RecordSet) Delete(ctx context.Context, key RecordKey, expectRev uint64) (Record, bool, error) {
	return s.cas(ctx, key, expectRev, nil, true)
}

func (s *RecordSet) DeleteValue(ctx context.Context, key RecordKey, expectRev uint64, value []byte) (Record, bool, error) {
	return s.cas(ctx, key, expectRev, value, true)
}

func (s *RecordSet) cas(ctx context.Context, key RecordKey, expectRev uint64, value []byte, deleted bool) (Record, bool, error) {
	roundKey := recordSetRoundKey(s.namespace, s.shard, s.name)
	unlock := s.store.lockRecordSet(roundKey)
	defer unlock()
	for attempt := 0; attempt < s.store.maxAttempts; attempt++ {
		view, err := s.View(ctx)
		if err != nil {
			return Record{}, false, err
		}
		ballot := s.store.nextBallot(roundKey, Ballot{})
		reads, prepared, _, promised := s.prepare(ctx, view, key, ballot)
		if !promised.IsZero() && !promised.Less(ballot) {
			s.store.bumpRound(roundKey, promised)
			continue
		}
		if !quorumSatisfied(prepared, view.WriteSets) {
			return Record{}, false, ErrQuorum
		}
		records, head, certificate, ok, err := s.fetchCommittedSnapshot(ctx, view)
		if err != nil {
			return Record{}, false, err
		}
		if !ok {
			return Record{}, false, ErrQuorum
		}
		installed := s.installSnapshotBestEffort(ctx, view, records, head, certificate)
		eligible := intersectMembers(prepared, installed)
		if !quorumSatisfied(eligible, view.WriteSets) {
			return Record{}, false, ErrQuorum
		}
		if pending, found := highestAccepted(reads, head+1); found {
			pending.Meta.Ballot = ballot
			accepted, responded, promised := s.accept(ctx, view, pending.Key, pending, ballot, eligible)
			if !quorumSatisfied(accepted, view.WriteSets) {
				if !promised.IsZero() && !promised.Less(ballot) {
					s.store.bumpRound(roundKey, promised)
					continue
				}
				if quorumSatisfied(responded, view.WriteSets) {
					continue
				}
				return Record{}, false, ErrQuorum
			}
			committed := mergeRecord(records, pending)
			commitCertificate, err := makeCommitCertificate(head+1, committed, ballot, accepted, view.WriteSets)
			if err != nil {
				return Record{}, false, err
			}
			committedMembers := s.installSnapshotBestEffort(ctx, view, committed, head+1, commitCertificate)
			if !quorumSatisfied(committedMembers, view.WriteSets) {
				return Record{}, false, ErrQuorum
			}
			continue
		}
		cur, physicalFound := recordFromSnapshot(records, key)
		logicalFound := physicalFound && !cur.Deleted
		if !matchRev(logicalFound, cur.Meta.Rev, expectRev) {
			return cloneRecord(cur), false, nil
		}
		if deleted && !logicalFound {
			return Record{}, false, nil
		}
		next := Record{
			Namespace: s.namespace, Shard: s.shard, RecordSet: s.name, Key: key,
			Value: append([]byte(nil), value...), Deleted: deleted,
			Meta: RecordMeta{Ballot: ballot, Rev: head + 1, UpdatedAt: s.store.now()},
		}
		accepted, responded, promised := s.accept(ctx, view, key, next, ballot, eligible)
		if !quorumSatisfied(accepted, view.WriteSets) {
			if !promised.IsZero() && !promised.Less(ballot) {
				s.store.bumpRound(roundKey, promised)
				continue
			}
			if quorumSatisfied(responded, view.WriteSets) {
				continue
			}
			return Record{}, false, ErrQuorum
		}
		committed := mergeRecord(records, next)
		commitCertificate, err := makeCommitCertificate(head+1, committed, ballot, accepted, view.WriteSets)
		if err != nil {
			return Record{}, false, err
		}
		committedMembers := s.installSnapshotBestEffort(ctx, view, committed, head+1, commitCertificate)
		if !quorumSatisfied(committedMembers, view.WriteSets) {
			return Record{}, false, ErrQuorum
		}
		if deleted {
			return Record{}, true, nil
		}
		return cloneRecord(next), true, nil
	}
	return Record{}, false, ErrConflict
}

func (s *RecordSet) EnsureReady(ctx context.Context) error {
	view, err := s.View(ctx)
	if err != nil {
		return err
	}
	if !localInWriteView(view, s.store.local) {
		return ErrInvalidView
	}
	local, err := s.store.getLocalShard(s.namespace, s.shard)
	if err != nil {
		return err
	}
	localSet := local.recordSet(s.name, s.store.now())
	records, head, certificate, ok, err := s.fetchCommittedSnapshot(ctx, view)
	if err != nil {
		return err
	}
	if !ok {
		return ErrQuorum
	}
	if localSet.ready(view.Label, head) {
		return nil
	}
	for _, rec := range records {
		localSet.repair(rec, s.store.now())
	}
	localSet.install(records, head, certificate, s.store.now())
	s.installSnapshotBestEffort(ctx, view, records, head, certificate)
	localSet.markReady(view.Label, head)
	return nil
}

func (s *RecordSet) Snapshot(ctx context.Context) (Snapshot, error) {
	if err := s.EnsureReady(ctx); err != nil {
		return Snapshot{}, err
	}
	view, err := s.View(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	local, err := s.store.getLocalShard(s.namespace, s.shard)
	if err != nil {
		return Snapshot{}, err
	}
	return local.recordSet(s.name, s.store.now()).snapshot(view.Label), nil
}

func (s *RecordSet) Watch(ctx context.Context, token string) (Watch, error) {
	if err := s.EnsureReady(ctx); err != nil {
		return Watch{}, err
	}
	view, err := s.View(ctx)
	if err != nil {
		return Watch{}, err
	}
	local, err := s.store.getLocalShard(s.namespace, s.shard)
	if err != nil {
		return Watch{}, err
	}
	return local.recordSet(s.name, s.store.now()).watch(ctx, view.Label, token), nil
}

func (s *RecordSet) WatchSince(ctx context.Context, fromRev uint64) (Watch, error) {
	if err := s.EnsureReady(ctx); err != nil {
		return Watch{}, err
	}
	view, err := s.View(ctx)
	if err != nil {
		return Watch{}, err
	}
	local, err := s.store.getLocalShard(s.namespace, s.shard)
	if err != nil {
		return Watch{}, err
	}
	return local.recordSet(s.name, s.store.now()).watchSince(ctx, view.Label, fromRev)
}

func (s *RecordSet) read(ctx context.Context, view ShardView, key RecordKey) ([]recordRead, map[MemberID]bool) {
	req := Request{Op: OpRead, Namespace: s.namespace, Shard: s.shard, RecordSet: s.name, Key: key}
	type result struct {
		member   MemberID
		response Response
		err      error
	}
	members := readMembers(view)
	ch := make(chan result, len(members))
	for _, member := range members {
		member := member
		go func() {
			resp, err := s.call(ctx, view.Label, member, req)
			ch <- result{member: member, response: resp, err: err}
		}()
	}
	reads := make([]recordRead, 0, len(members))
	success := map[MemberID]bool{}
	for range members {
		res := <-ch
		if res.err != nil || !res.response.OK {
			continue
		}
		success[res.member] = true
		reads = append(reads, recordRead{member: res.member, record: res.response.Record, found: res.response.Found, head: res.response.Rev})
	}
	return reads, success
}

func (s *RecordSet) prepare(ctx context.Context, view ShardView, key RecordKey, ballot Ballot) ([]recordRead, map[MemberID]bool, map[MemberID]uint64, Ballot) {
	req := Request{Op: OpPrepare, Namespace: s.namespace, Shard: s.shard, RecordSet: s.name, Key: key, Ballot: ballot}
	type result struct {
		member   MemberID
		response Response
		err      error
	}
	members := writeMembers(view)
	ch := make(chan result, len(members))
	for _, member := range members {
		member := member
		go func() {
			resp, err := s.call(ctx, view.Label, member, req)
			ch <- result{member: member, response: resp, err: err}
		}()
	}
	reads := make([]recordRead, 0, len(members))
	prepared := map[MemberID]bool{}
	heads := map[MemberID]uint64{}
	var maxPromised Ballot
	for range members {
		res := <-ch
		if res.err != nil {
			continue
		}
		if !res.response.OK {
			if maxPromised.Less(res.response.Promised) {
				maxPromised = res.response.Promised
			}
			continue
		}
		prepared[res.member] = true
		heads[res.member] = res.response.Rev
		reads = append(reads, recordRead{
			member: res.member, record: res.response.Record, found: res.response.Found, head: res.response.Rev,
			accepted: res.response.Accepted, acceptedFound: res.response.AcceptedFound,
		})
	}
	return reads, prepared, heads, maxPromised
}

func (s *RecordSet) accept(ctx context.Context, view ShardView, key RecordKey, rec Record, ballot Ballot, prepared map[MemberID]bool) (map[MemberID]bool, map[MemberID]bool, Ballot) {
	req := Request{Op: OpAccept, Namespace: s.namespace, Shard: s.shard, RecordSet: s.name, Key: key, Ballot: ballot, Record: rec}
	type result struct {
		member   MemberID
		response Response
		err      error
	}
	ch := make(chan result, len(prepared))
	for member := range prepared {
		member := member
		go func() {
			resp, err := s.call(ctx, view.Label, member, req)
			ch <- result{member: member, response: resp, err: err}
		}()
	}
	accepted := map[MemberID]bool{}
	responded := map[MemberID]bool{}
	var maxPromised Ballot
	for range prepared {
		res := <-ch
		if res.err != nil {
			continue
		}
		responded[res.member] = true
		if !res.response.OK {
			if maxPromised.Less(res.response.Promised) {
				maxPromised = res.response.Promised
			}
			continue
		}
		accepted[res.member] = true
	}
	return accepted, responded, maxPromised
}

func (s *RecordSet) repairRecordBestEffort(ctx context.Context, view ShardView, rec Record, targets map[MemberID]bool) {
	repairCtx := context.WithoutCancel(ctx)
	if _, ok := repairCtx.Deadline(); !ok {
		var cancel context.CancelFunc
		repairCtx, cancel = context.WithTimeout(repairCtx, s.store.repairTimeout)
		defer cancel()
	}
	members := writeMembers(view)
	var wg sync.WaitGroup
	for _, member := range members {
		member := member
		if targets != nil && !targets[member] {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = s.call(repairCtx, view.Label, member, Request{Op: OpRepair, Namespace: s.namespace, Shard: s.shard, RecordSet: s.name, Record: rec})
		}()
	}
	wg.Wait()
}

func (s *RecordSet) catchUpShard(ctx context.Context, view ShardView) error {
	records, head, certificate, ok, err := s.fetchCommittedSnapshot(ctx, view)
	if err != nil {
		return err
	}
	if !ok {
		return ErrQuorum
	}
	s.installSnapshotBestEffort(ctx, view, records, head, certificate)
	return nil
}

func (s *RecordSet) fetchCommittedSnapshot(ctx context.Context, view ShardView) ([]Record, uint64, CommitCertificate, bool, error) {
	req := Request{Op: OpSnapshot, Namespace: s.namespace, Shard: s.shard, RecordSet: s.name}
	type result struct {
		member      MemberID
		records     []Record
		certificate CommitCertificate
		rev         uint64
		err         error
	}
	members := readMembers(view)
	ch := make(chan result, len(members))
	for _, member := range members {
		member := member
		go func() {
			resp, err := s.call(ctx, view.Label, member, req)
			if err != nil {
				ch <- result{member: member, err: err}
				return
			}
			ch <- result{member: member, records: resp.Records, certificate: resp.Certificate, rev: resp.Rev}
		}()
	}
	success := map[MemberID]bool{}
	snapshots := make([]snapshotRead, 0, len(members))
	for range members {
		res := <-ch
		if res.err != nil {
			continue
		}
		success[res.member] = true
		snapshots = append(snapshots, snapshotRead{member: res.member, records: res.records, certificate: res.certificate, rev: res.rev})
	}
	if !quorumSatisfied(success, view.WriteSets) {
		return nil, 0, CommitCertificate{}, false, nil
	}
	return chooseCommittedSnapshot(snapshots, view.ReadSets)
}

func (s *RecordSet) fetchAllShardRecords(ctx context.Context, view ShardView) ([]Record, uint64, bool, error) {
	records, rev, _, ok, err := s.fetchCommittedSnapshot(ctx, view)
	return records, rev, ok, err
}

func (s *RecordSet) installSnapshotBestEffort(ctx context.Context, view ShardView, records []Record, rev uint64, certificate CommitCertificate, skipLocal ...bool) map[MemberID]bool {
	installCtx := context.WithoutCancel(ctx)
	if _, ok := installCtx.Deadline(); !ok {
		var cancel context.CancelFunc
		installCtx, cancel = context.WithTimeout(installCtx, s.store.repairTimeout)
		defer cancel()
	}
	omitLocal := len(skipLocal) > 0 && skipLocal[0]
	members := writeMembers(view)
	var mu sync.Mutex
	okMembers := map[MemberID]bool{}
	var wg sync.WaitGroup
	for _, member := range members {
		member := member
		if omitLocal && member == s.store.local {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := s.call(installCtx, view.Label, member, Request{
				Op: OpInstall, Namespace: s.namespace, Shard: s.shard, RecordSet: s.name,
				Records: cloneRecords(records), Rev: rev, Certificate: cloneCommitCertificate(certificate),
			})
			if err != nil || !resp.OK {
				return
			}
			mu.Lock()
			okMembers[member] = true
			mu.Unlock()
		}()
	}
	wg.Wait()
	return okMembers
}

func (s *RecordSet) call(ctx context.Context, label string, member MemberID, req Request) (Response, error) {
	if err := ctx.Err(); err != nil {
		return Response{}, err
	}
	req.Label = label
	if member == s.store.local {
		return s.store.Handle(ctx, req)
	}
	if s.store.ready != nil && !s.store.ready.Ready(label, member) {
		return Response{}, ErrReplicaUnavailable
	}
	if s.store.transport == nil {
		return Response{}, ErrReplicaUnavailable
	}
	resp, err := s.store.transport.Call(ctx, member, req)
	if err != nil {
		return Response{}, err
	}
	if resp.Error != "" {
		return Response{}, errors.New(resp.Error)
	}
	return resp, nil
}

func localInWriteView(view ShardView, local MemberID) bool {
	for _, member := range writeMembers(view) {
		if member == local {
			return true
		}
	}
	return false
}

func matchRev(found bool, rev, expect uint64) bool {
	if !found {
		return expect == 0
	}
	return rev == expect
}

func recordVisibleQuorum(rec Record, reads []recordRead, sets []ShardMemberSet) bool {
	ok := map[MemberID]bool{}
	for _, read := range reads {
		if read.found && sameRecordVersion(read.record, rec) {
			ok[read.member] = true
		}
	}
	return quorumSatisfied(ok, sets)
}

func recordRepairTargets(rec Record, reads []recordRead) map[MemberID]bool {
	targets := map[MemberID]bool{}
	for _, read := range reads {
		if !read.found || !sameRecordVersion(read.record, rec) {
			targets[read.member] = true
		}
	}
	return targets
}

func intersectMembers(a, b map[MemberID]bool) map[MemberID]bool {
	out := map[MemberID]bool{}
	for member := range a {
		if b[member] {
			out[member] = true
		}
	}
	return out
}

func sameRecordVersion(a, b Record) bool {
	return a.Namespace == b.Namespace &&
		a.Shard == b.Shard &&
		a.RecordSet == b.RecordSet &&
		a.Key == b.Key &&
		a.Deleted == b.Deleted &&
		a.Meta.Rev == b.Meta.Rev &&
		a.Meta.Ballot == b.Meta.Ballot &&
		string(a.Value) == string(b.Value)
}

func recordFromSnapshot(records []Record, key RecordKey) (Record, bool) {
	for _, rec := range records {
		if rec.Key == key {
			return cloneRecord(rec), true
		}
	}
	return Record{}, false
}

func mergeRecord(records []Record, rec Record) []Record {
	out := cloneRecords(records)
	for i := range out {
		if out[i].Key == rec.Key {
			out[i] = cloneRecord(rec)
			return normalizeSnapshotRecords(out)
		}
	}
	out = append(out, cloneRecord(rec))
	return normalizeSnapshotRecords(out)
}

type snapshotRead struct {
	member      MemberID
	records     []Record
	certificate CommitCertificate
	rev         uint64
}

func chooseCommittedSnapshot(snapshots []snapshotRead, sets []ShardMemberSet) ([]Record, uint64, CommitCertificate, bool, error) {
	type group struct {
		records     []Record
		certificate CommitCertificate
		rev         uint64
		members     map[MemberID]bool
		key         string
	}
	groups := map[string]*group{}
	var candidates []*group
	for _, snap := range snapshots {
		records := normalizeSnapshotRecords(snap.records)
		key, err := snapshotKey(snap.rev, records)
		if err != nil {
			return nil, 0, CommitCertificate{}, false, err
		}
		g := groups[key]
		if g == nil {
			g = &group{records: records, certificate: snap.certificate, rev: snap.rev, members: map[MemberID]bool{}, key: key}
			groups[key] = g
		}
		g.members[snap.member] = true
		if validCommitCertificate(snap.certificate, snap.rev, records, sets) {
			candidates = append(candidates, &group{records: records, certificate: snap.certificate, rev: snap.rev, members: map[MemberID]bool{snap.member: true}, key: key})
		}
	}
	for _, g := range groups {
		if !quorumSatisfied(g.members, sets) {
			continue
		}
		candidates = append(candidates, g)
	}
	var best *group
	for _, g := range candidates {
		if best == nil || g.rev > best.rev {
			best = g
			continue
		}
		if best.rev == g.rev && best.key != g.key {
			return nil, 0, CommitCertificate{}, false, ErrConflict
		}
	}
	if best == nil {
		return nil, 0, CommitCertificate{}, false, nil
	}
	return cloneRecords(best.records), best.rev, cloneCommitCertificate(best.certificate), true, nil
}

func highestAccepted(reads []recordRead, rev uint64) (Record, bool) {
	var out Record
	found := false
	for _, read := range reads {
		if !read.acceptedFound || read.accepted.Meta.Rev != rev {
			continue
		}
		if !found || recordNewer(read.accepted, out) {
			out = cloneRecord(read.accepted)
			found = true
		}
	}
	return out, found
}

func makeCommitCertificate(rev uint64, records []Record, ballot Ballot, members map[MemberID]bool, sets []ShardMemberSet) (CommitCertificate, error) {
	digest, err := snapshotDigest(rev, normalizeSnapshotRecords(records))
	if err != nil {
		return CommitCertificate{}, err
	}
	out := CommitCertificate{Rev: rev, Digest: digest, Ballot: ballot}
	for _, set := range sets {
		if set.Label == "" || !quorumSatisfiedSet(members, set) {
			continue
		}
		out.Labels = append(out.Labels, set.Label)
	}
	sort.Strings(out.Labels)
	for member := range members {
		if member != "" {
			out.Members = append(out.Members, member)
		}
	}
	sort.Slice(out.Members, func(i, j int) bool { return out.Members[i] < out.Members[j] })
	return out, nil
}

func validCommitCertificate(certificate CommitCertificate, rev uint64, records []Record, sets []ShardMemberSet) bool {
	if certificate.IsZero() {
		return false
	}
	if certificate.Rev != rev || certificate.Digest == "" || len(certificate.Members) == 0 {
		return false
	}
	digest, err := snapshotDigest(rev, normalizeSnapshotRecords(records))
	if err != nil || digest != certificate.Digest {
		return false
	}
	ok := map[MemberID]bool{}
	for _, member := range certificate.Members {
		if member != "" {
			ok[member] = true
		}
	}
	if len(certificate.Labels) == 0 {
		return quorumSatisfied(ok, sets)
	}
	labels := map[string]bool{}
	for _, label := range certificate.Labels {
		if label != "" {
			labels[label] = true
		}
	}
	for _, set := range sets {
		if labels[set.Label] && quorumSatisfiedSet(ok, set) {
			return true
		}
	}
	return false
}

func normalizeSnapshotRecords(records []Record) []Record {
	out := cloneRecords(records)
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

func snapshotKey(rev uint64, records []Record) (string, error) {
	type recordKey struct {
		Key     RecordKey `json:"key"`
		Value   []byte    `json:"value,omitempty"`
		Deleted bool      `json:"deleted,omitempty"`
		Ballot  Ballot    `json:"ballot"`
		Rev     uint64    `json:"rev"`
	}
	keys := make([]recordKey, 0, len(records))
	for _, rec := range records {
		// Tombstones are retention artifacts. The committed logical state at a
		// recordSet revision is identical whether an expired deleted record is
		// still retained locally or has already been compacted.
		if rec.Deleted {
			continue
		}
		keys = append(keys, recordKey{
			Key: rec.Key, Value: rec.Value, Deleted: rec.Deleted,
			Ballot: rec.Meta.Ballot, Rev: rec.Meta.Rev,
		})
	}
	raw, err := json.Marshal(struct {
		Rev     uint64      `json:"rev"`
		Records []recordKey `json:"records"`
	}{Rev: rev, Records: keys})
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func snapshotDigest(rev uint64, records []Record) (string, error) {
	key, err := snapshotKey(rev, records)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:]), nil
}

func cloneRecords(records []Record) []Record {
	out := make([]Record, len(records))
	for i, rec := range records {
		out[i] = cloneRecord(rec)
	}
	return out
}
