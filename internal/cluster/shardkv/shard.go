package shardkv

import (
	"context"
	"errors"
	"sync"
)

type Shard struct {
	store     *Store
	namespace Namespace
	shard     ShardKey
}

func (s *Shard) Namespace() Namespace { return s.namespace }

func (s *Shard) Key() ShardKey { return s.shard }

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

func (s *Shard) Get(ctx context.Context, key RecordKey) (Record, bool, error) {
	best, found, err := s.GetRecord(ctx, key)
	if err != nil || !found || best.Deleted {
		return Record{}, false, err
	}
	return best, true, nil
}

func (s *Shard) GetRecord(ctx context.Context, key RecordKey) (Record, bool, error) {
	for attempt := 0; attempt < s.store.maxAttempts; attempt++ {
		view, err := s.View(ctx)
		if err != nil {
			return Record{}, false, err
		}
		ballot := s.store.nextBallot(recordRoundKey(s.namespace, s.shard, key), Ballot{})
		reads, prepared, promised := s.prepare(ctx, view, key, ballot)
		if !promised.IsZero() && !promised.Less(ballot) {
			s.store.bumpRound(recordRoundKey(s.namespace, s.shard, key), promised)
			continue
		}
		if !quorumSatisfied(prepared, view.Sets) {
			return Record{}, false, ErrQuorum
		}
		best, found := highest(reads)
		if found {
			best.Meta.Ballot = ballot
			accepted, promised := s.accept(ctx, view, key, best, ballot, prepared)
			if !quorumSatisfied(accepted, view.Sets) {
				if !promised.IsZero() && !promised.Less(ballot) {
					s.store.bumpRound(recordRoundKey(s.namespace, s.shard, key), promised)
					continue
				}
				return Record{}, false, ErrQuorum
			}
			s.repairBestEffort(ctx, view, best)
		}
		if !found {
			return Record{}, false, nil
		}
		return cloneRecord(best), true, nil
	}
	return Record{}, false, ErrConflict
}

func (s *Shard) CAS(ctx context.Context, key RecordKey, expectRev uint64, value []byte) (Record, bool, error) {
	return s.cas(ctx, key, expectRev, value, false)
}

func (s *Shard) Delete(ctx context.Context, key RecordKey, expectRev uint64) (Record, bool, error) {
	return s.cas(ctx, key, expectRev, nil, true)
}

func (s *Shard) DeleteValue(ctx context.Context, key RecordKey, expectRev uint64, value []byte) (Record, bool, error) {
	return s.cas(ctx, key, expectRev, value, true)
}

func (s *Shard) cas(ctx context.Context, key RecordKey, expectRev uint64, value []byte, deleted bool) (Record, bool, error) {
	for attempt := 0; attempt < s.store.maxAttempts; attempt++ {
		view, err := s.View(ctx)
		if err != nil {
			return Record{}, false, err
		}
		roundKey := recordRoundKey(s.namespace, s.shard, key)
		ballot := s.store.nextBallot(roundKey, Ballot{})
		reads, prepared, promised := s.prepare(ctx, view, key, ballot)
		if !promised.IsZero() && !promised.Less(ballot) {
			s.store.bumpRound(roundKey, promised)
			continue
		}
		if !quorumSatisfied(prepared, view.Sets) {
			return Record{}, false, ErrQuorum
		}
		cur, physicalFound := highest(reads)
		logicalFound := physicalFound && !cur.Deleted
		if !matchRev(logicalFound, cur.Meta.Rev, expectRev) {
			return cloneRecord(cur), false, nil
		}
		if deleted && !logicalFound {
			return Record{}, false, nil
		}
		next := Record{
			Namespace: s.namespace, Shard: s.shard, Key: key,
			Value: append([]byte(nil), value...), Deleted: deleted,
			Meta: RecordMeta{Ballot: ballot, Rev: cur.Meta.Rev + 1, UpdatedAt: s.store.now()},
		}
		accepted, promised := s.accept(ctx, view, key, next, ballot, prepared)
		if !quorumSatisfied(accepted, view.Sets) {
			if !promised.IsZero() && !promised.Less(ballot) {
				s.store.bumpRound(roundKey, promised)
				continue
			}
			return Record{}, false, ErrQuorum
		}
		s.repairBestEffort(ctx, view, next)
		if deleted {
			return Record{}, true, nil
		}
		return cloneRecord(next), true, nil
	}
	return Record{}, false, ErrConflict
}

func (s *Shard) EnsureReady(ctx context.Context) error {
	view, err := s.View(ctx)
	if err != nil {
		return err
	}
	local, err := s.store.getLocalShard(s.namespace, s.shard)
	if err != nil {
		return err
	}
	if local.ready(view.Label) {
		return nil
	}
	records, ok, err := s.fetchShardRecords(ctx, view)
	if err != nil {
		return err
	}
	if !ok {
		return ErrQuorum
	}
	for _, rec := range records {
		local.repair(rec, s.store.now())
	}
	s.repairRecordsBestEffort(ctx, view, records)
	local.markReady(view.Label)
	return nil
}

func (s *Shard) Snapshot(ctx context.Context) (Snapshot, error) {
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
	return local.snapshot(view.Label), nil
}

func (s *Shard) Watch(ctx context.Context, token string) (Watch, error) {
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
	return local.watch(ctx, view.Label, token), nil
}

func (s *Shard) WatchSince(ctx context.Context, fromRev uint64) (Watch, error) {
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
	return local.watchSince(ctx, view.Label, fromRev)
}

func (s *Shard) prepare(ctx context.Context, view ShardView, key RecordKey, ballot Ballot) ([]recordRead, map[MemberID]bool, Ballot) {
	req := Request{Op: OpPrepare, Namespace: s.namespace, Shard: s.shard, Key: key, Ballot: ballot}
	type result struct {
		member   MemberID
		response Response
		err      error
	}
	members := shardMembers(view)
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
		reads = append(reads, recordRead{record: res.response.Record, found: res.response.Found})
	}
	return reads, prepared, maxPromised
}

func (s *Shard) accept(ctx context.Context, view ShardView, key RecordKey, rec Record, ballot Ballot, prepared map[MemberID]bool) (map[MemberID]bool, Ballot) {
	req := Request{Op: OpAccept, Namespace: s.namespace, Shard: s.shard, Key: key, Ballot: ballot, Record: rec}
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
	var maxPromised Ballot
	for range prepared {
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
		accepted[res.member] = true
	}
	return accepted, maxPromised
}

func (s *Shard) repairBestEffort(ctx context.Context, view ShardView, rec Record) {
	s.repairRecordsBestEffort(ctx, view, []Record{rec})
}

func (s *Shard) repairRecordsBestEffort(ctx context.Context, view ShardView, records []Record) {
	repairCtx := context.WithoutCancel(ctx)
	if _, ok := repairCtx.Deadline(); !ok {
		var cancel context.CancelFunc
		repairCtx, cancel = context.WithTimeout(repairCtx, s.store.repairTimeout)
		defer cancel()
	}
	members := shardMembers(view)
	var wg sync.WaitGroup
	for _, member := range members {
		member := member
		for _, rec := range records {
			rec := rec
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = s.call(repairCtx, view.Label, member, Request{Op: OpRepair, Namespace: s.namespace, Shard: s.shard, Record: rec})
			}()
		}
	}
	wg.Wait()
}

func (s *Shard) fetchShardRecords(ctx context.Context, view ShardView) ([]Record, bool, error) {
	req := Request{Op: OpSnapshot, Namespace: s.namespace, Shard: s.shard}
	type result struct {
		member  MemberID
		records []Record
		err     error
	}
	members := shardMembers(view)
	ch := make(chan result, len(members))
	for _, member := range members {
		member := member
		go func() {
			resp, err := s.call(ctx, view.Label, member, req)
			if err != nil {
				ch <- result{member: member, err: err}
				return
			}
			ch <- result{member: member, records: resp.Records}
		}()
	}
	success := map[MemberID]bool{}
	merged := map[RecordKey]Record{}
	for range members {
		res := <-ch
		if res.err != nil {
			continue
		}
		success[res.member] = true
		for _, rec := range res.records {
			cur, found := merged[rec.Key]
			if !found || recordNewer(rec, cur) {
				merged[rec.Key] = cloneRecord(rec)
			}
		}
	}
	if !quorumSatisfied(success, view.Sets) {
		return nil, false, nil
	}
	out := make([]Record, 0, len(merged))
	for _, rec := range merged {
		out = append(out, rec)
	}
	return out, true, nil
}

func (s *Shard) fetchAllShardRecords(ctx context.Context, view ShardView) ([]Record, bool, error) {
	req := Request{Op: OpSnapshot, Namespace: s.namespace, Shard: s.shard}
	type result struct {
		records []Record
		err     error
	}
	members := shardMembers(view)
	ch := make(chan result, len(members))
	for _, member := range members {
		member := member
		go func() {
			resp, err := s.call(ctx, view.Label, member, req)
			if err != nil {
				ch <- result{err: err}
				return
			}
			ch <- result{records: resp.Records}
		}()
	}
	merged := map[RecordKey]Record{}
	for range members {
		res := <-ch
		if res.err != nil {
			return nil, false, nil
		}
		for _, rec := range res.records {
			cur, found := merged[rec.Key]
			if !found || recordNewer(rec, cur) {
				merged[rec.Key] = cloneRecord(rec)
			}
		}
	}
	out := make([]Record, 0, len(merged))
	for _, rec := range merged {
		out = append(out, rec)
	}
	return out, true, nil
}

func (s *Shard) call(ctx context.Context, label string, member MemberID, req Request) (Response, error) {
	if err := ctx.Err(); err != nil {
		return Response{}, err
	}
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

func matchRev(found bool, rev, expect uint64) bool {
	if !found {
		return expect == 0
	}
	return rev == expect
}
