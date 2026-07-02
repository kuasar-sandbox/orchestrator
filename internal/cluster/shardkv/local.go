package shardkv

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"
)

type StoreOptions struct {
	Local                 MemberID
	Resolver              ShardResolver
	Transport             Transport
	Ready                 MemberReadyProvider
	Clock                 func() time.Time
	Epoch                 string
	DefaultWatchRetention int
	MaxAttempts           int
	RepairTimeout         time.Duration
}

type Store struct {
	local                 MemberID
	resolver              ShardResolver
	transport             Transport
	ready                 MemberReadyProvider
	now                   func() time.Time
	epoch                 string
	defaultWatchRetention int
	maxAttempts           int
	repairTimeout         time.Duration

	mu      sync.Mutex
	closed  bool
	shards  map[string]*localShard
	stripes []recordSetStripe
}

const defaultRecordSetStripes = 4096

type recordSetStripe struct {
	casMu   sync.Mutex
	roundMu sync.Mutex
	round   uint64
}

func NewStore(opts StoreOptions) (*Store, error) {
	if opts.Local == "" {
		return nil, fmt.Errorf("shardkv: local member is required")
	}
	if opts.Resolver == nil {
		return nil, fmt.Errorf("shardkv: resolver is required")
	}
	now := opts.Clock
	if now == nil {
		now = time.Now
	}
	epoch := opts.Epoch
	if epoch == "" {
		epoch = newEpoch()
	}
	if opts.DefaultWatchRetention <= 0 {
		opts.DefaultWatchRetention = 10000
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 64
	}
	if opts.RepairTimeout <= 0 {
		opts.RepairTimeout = 250 * time.Millisecond
	}
	return &Store{
		local: opts.Local, resolver: opts.Resolver, transport: opts.Transport, ready: opts.Ready,
		now: now, epoch: epoch, defaultWatchRetention: opts.DefaultWatchRetention,
		maxAttempts: opts.MaxAttempts, repairTimeout: opts.RepairTimeout,
		shards: map[string]*localShard{}, stripes: make([]recordSetStripe, defaultRecordSetStripes),
	}, nil
}

func (s *Store) Shard(ns Namespace, shard ShardKey) (*Shard, error) {
	if s == nil {
		return nil, ErrClosed
	}
	view, err := s.resolver.ResolveShard(ns, shard)
	if err != nil {
		return nil, err
	}
	if err := validateView(view); err != nil {
		return nil, err
	}
	return &Shard{store: s, namespace: ns, shard: shard}, nil
}

func (s *Store) Configure(resolver ShardResolver, transport Transport, ready MemberReadyProvider, defaultWatchRetention int) error {
	if s == nil {
		return ErrClosed
	}
	if resolver == nil {
		return fmt.Errorf("shardkv: resolver is required")
	}
	if defaultWatchRetention <= 0 {
		defaultWatchRetention = 10000
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	s.resolver = resolver
	s.transport = transport
	s.ready = ready
	s.defaultWatchRetention = defaultWatchRetention
	for _, shard := range s.shards {
		shard.mu.Lock()
		shard.retention = s.watchRetentionLocked(shard.namespace)
		shard.mu.Unlock()
	}
	return nil
}

func (s *Store) Handle(ctx context.Context, req Request) (Response, error) {
	if err := ctx.Err(); err != nil {
		return Response{}, err
	}
	if err := s.validateRequest(req); err != nil {
		return Response{}, err
	}
	switch req.Op {
	case OpSnapshot:
		ls, err := s.getLocalShardNoTouch(req.Namespace, req.Shard)
		if err != nil {
			return Response{}, err
		}
		rs := ls.recordSetNoTouch(req.RecordSet)
		if rs == nil {
			return Response{OK: true}, nil
		}
		records, rev, certificate := rs.snapshotRecords(true)
		return Response{Records: records, Certificate: certificate, OK: true, Rev: rev}, nil
	case OpRepair:
		ls, err := s.getLocalShardNoTouch(req.Namespace, req.Shard)
		if err != nil {
			return Response{}, err
		}
		rs := ls.recordSetNoTouch(req.RecordSet)
		if rs == nil {
			if req.Record.Key == "" {
				return Response{OK: false}, nil
			}
			rs = ls.recordSet(req.RecordSet, s.now())
		}
		ok := rs.repair(req.Record, s.now())
		return Response{OK: ok}, nil
	case OpInstall:
		ls, err := s.getLocalShardNoTouch(req.Namespace, req.Shard)
		if err != nil {
			return Response{}, err
		}
		rs := ls.recordSet(req.RecordSet, s.now())
		ok := rs.install(req.Records, req.Rev, req.Certificate, s.now())
		return Response{OK: ok, Rev: req.Rev}, nil
	}
	ls, err := s.getLocalShard(req.Namespace, req.Shard)
	if err != nil {
		return Response{}, err
	}
	rs := ls.recordSet(req.RecordSet, s.now())
	switch req.Op {
	case OpRead:
		rec, found, rev := rs.read(req.Key)
		return Response{Record: rec, Found: found, OK: true, Rev: rev}, nil
	case OpPrepare:
		rec, found, rev, ok, promised, accepted, acceptedFound := rs.prepare(req.Key, req.Ballot)
		return Response{Record: rec, Found: found, Accepted: accepted, AcceptedFound: acceptedFound, OK: ok, Promised: promised, Rev: rev}, nil
	case OpAccept:
		ok, promised := rs.accept(req.Key, req.Record, req.Ballot, s.now())
		return Response{OK: ok, Promised: promised, Rev: req.Record.Meta.Rev}, nil
	default:
		return Response{}, fmt.Errorf("shardkv: unknown op %q", req.Op)
	}
}

func (s *Store) validateRequest(req Request) error {
	if req.Namespace == "" || req.Shard == "" || req.RecordSet == "" {
		return ErrInvalidView
	}
	view, err := s.resolver.ResolveShard(req.Namespace, req.Shard)
	if err != nil {
		return err
	}
	if req.Label != "" && req.Label != view.Label {
		return ErrInvalidView
	}
	var members []MemberID
	switch req.Op {
	case OpRead, OpSnapshot:
		members = readMembers(view)
	case OpPrepare, OpAccept, OpInstall, OpRepair:
		members = writeMembers(view)
	default:
		return fmt.Errorf("shardkv: unknown op %q", req.Op)
	}
	for _, member := range members {
		if member == s.local {
			return nil
		}
	}
	return ErrInvalidView
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	for _, shard := range s.shards {
		shard.close()
	}
	return nil
}

func (s *Store) getLocalShard(ns Namespace, shard ShardKey) (*localShard, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	key := localShardKey(ns, shard)
	ls := s.shards[key]
	if ls == nil {
		ls = newLocalShard(ns, shard, s.epoch, s.watchRetentionLocked(ns), s.now())
		s.shards[key] = ls
	}
	ls.touch(s.now())
	return ls, nil
}

func (s *Store) getLocalShardNoTouch(ns Namespace, shard ShardKey) (*localShard, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	key := localShardKey(ns, shard)
	ls := s.shards[key]
	if ls == nil {
		ls = newLocalShard(ns, shard, s.epoch, s.watchRetentionLocked(ns), s.now())
		s.shards[key] = ls
	}
	return ls, nil
}

func (s *Store) watchRetentionLocked(ns Namespace) int {
	retention := s.defaultWatchRetention
	if spec := namespaceSpec(ns, s.resolver); spec.WatchRetention > 0 {
		retention = spec.WatchRetention
	}
	if retention <= 0 {
		retention = 10000
	}
	return retention
}

func (s *Store) nextBallot(key string, atLeast Ballot) Ballot {
	stripe := s.recordSetStripe(key)
	stripe.roundMu.Lock()
	defer stripe.roundMu.Unlock()
	cur := stripe.round
	if cur < atLeast.Round {
		cur = atLeast.Round
	}
	cur++
	stripe.round = cur
	return Ballot{Round: cur, Writer: s.local}
}

func (s *Store) bumpRound(key string, promised Ballot) {
	stripe := s.recordSetStripe(key)
	stripe.roundMu.Lock()
	defer stripe.roundMu.Unlock()
	if stripe.round < promised.Round {
		stripe.round = promised.Round
	}
}

func (s *Store) lockRecordSet(key string) func() {
	stripe := s.recordSetStripe(key)
	stripe.casMu.Lock()
	return stripe.casMu.Unlock
}

func (s *Store) recordSetStripe(key string) *recordSetStripe {
	return &s.stripes[hashRecordSetKey(key)%uint64(len(s.stripes))]
}

func hashRecordSetKey(key string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= prime64
	}
	return h
}

func localShardKey(ns Namespace, shard ShardKey) string {
	return string(ns) + "\x00" + string(shard)
}

func recordSetRoundKey(ns Namespace, shard ShardKey, recordSet RecordSetName) string {
	return string(ns) + "\x00" + string(shard) + "\x00" + string(recordSet)
}

type localShard struct {
	namespace Namespace
	shard     ShardKey
	epoch     string
	retention int

	mu         sync.Mutex
	recordSets map[RecordSetName]*localRecordSet
	lastAccess time.Time
}

type localRecordSet struct {
	namespace Namespace
	shard     ShardKey
	name      RecordSetName
	epoch     string
	retention int

	mu          sync.Mutex
	records     map[RecordKey]Record
	accepted    map[uint64]Record
	promised    map[RecordKey]Ballot
	setPromised Ballot
	certificate CommitCertificate
	rev         uint64
	log         []WatchEvent
	subs        map[int]*watchSub
	nextSub     int
	readyLabel  map[string]uint64
	lastAccess  time.Time
}

func newLocalShard(ns Namespace, shard ShardKey, epoch string, retention int, now time.Time) *localShard {
	return &localShard{
		namespace: ns, shard: shard, epoch: epoch, retention: retention,
		recordSets: map[RecordSetName]*localRecordSet{},
		lastAccess: now,
	}
}

func newLocalRecordSet(ns Namespace, shard ShardKey, name RecordSetName, epoch string, retention int, now time.Time) *localRecordSet {
	return &localRecordSet{
		namespace: ns, shard: shard, name: name, epoch: epoch, retention: retention,
		records: map[RecordKey]Record{}, accepted: map[uint64]Record{}, promised: map[RecordKey]Ballot{},
		subs: map[int]*watchSub{}, readyLabel: map[string]uint64{},
		lastAccess: now,
	}
}

type watchSub struct {
	label     string
	queue     chan WatchEvent
	done      chan struct{}
	closeOnce sync.Once
}

func (s *localShard) touch(now time.Time) {
	s.mu.Lock()
	s.lastAccess = now
	s.mu.Unlock()
}

func (s *localShard) recordSet(name RecordSetName, now time.Time) *localRecordSet {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastAccess = now
	rs := s.recordSets[name]
	if rs == nil {
		rs = newLocalRecordSet(s.namespace, s.shard, name, s.epoch, s.retention, now)
		s.recordSets[name] = rs
	}
	rs.touch(now)
	return rs
}

func (s *localShard) recordSetNoTouch(name RecordSetName) *localRecordSet {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recordSets[name]
}

func (s *localShard) recordSetNames() []RecordSetName {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]RecordSetName, 0, len(s.recordSets))
	for name := range s.recordSets {
		out = append(out, name)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (s *localRecordSet) touch(now time.Time) {
	s.mu.Lock()
	s.lastAccess = now
	s.mu.Unlock()
}

func (s *localRecordSet) read(key RecordKey) (Record, bool, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, found := s.records[key]
	return cloneRecord(rec), found, s.rev
}

func (s *localRecordSet) readReady(label string, key RecordKey, minRev uint64) (Record, bool, uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	readyRev, ok := s.readyLabel[label]
	if !ok || readyRev < minRev || s.rev < readyRev {
		return Record{}, false, readyRev, false
	}
	rec, found := s.records[key]
	if found && rec.Meta.Rev > readyRev {
		return Record{}, false, readyRev, false
	}
	return cloneRecord(rec), found, readyRev, true
}

func (s *localRecordSet) prepare(key RecordKey, ballot Ballot) (Record, bool, uint64, bool, Ballot, Record, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	promised := s.promised[key]
	if promised.Less(s.setPromised) {
		promised = s.setPromised
	}
	if ballot.Less(promised) {
		return Record{}, false, s.rev, false, promised, Record{}, false
	}
	s.setPromised = ballot
	s.promised[key] = ballot
	rec, found := s.records[key]
	accepted, acceptedFound := s.highestAcceptedLocked()
	return cloneRecord(rec), found, s.rev, true, ballot, accepted, acceptedFound
}

func (s *localRecordSet) accept(key RecordKey, rec Record, ballot Ballot, now time.Time) (bool, Ballot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	promised := s.promised[key]
	if promised.Less(s.setPromised) {
		promised = s.setPromised
	}
	if ballot.Less(promised) {
		return false, promised
	}
	if rec.Meta.Rev == 0 || rec.Meta.Rev > s.rev+1 {
		return false, promised
	}
	cur, found := s.records[key]
	if rec.Meta.Rev <= s.rev {
		if found && sameCommittedRecord(rec, cur) {
			s.setPromised = ballot
			s.promised[key] = ballot
			return true, ballot
		}
		return false, promised
	}
	if accepted, ok := s.accepted[rec.Meta.Rev]; ok && rec.Meta.Ballot.Less(accepted.Meta.Ballot) {
		if promised.Less(accepted.Meta.Ballot) {
			s.promised[key] = accepted.Meta.Ballot
			s.setPromised = accepted.Meta.Ballot
		}
		return false, s.promised[key]
	}
	rec.Namespace = s.namespace
	rec.Shard = s.shard
	rec.RecordSet = s.name
	rec.Key = key
	rec.Value = append([]byte(nil), rec.Value...)
	s.setPromised = ballot
	s.promised[key] = ballot
	s.accepted[rec.Meta.Rev] = rec
	s.lastAccess = now
	return true, ballot
}

func (s *localRecordSet) highestAcceptedLocked() (Record, bool) {
	var out Record
	found := false
	for _, rec := range s.accepted {
		if rec.Meta.Rev <= s.rev {
			continue
		}
		if !found || recordNewer(rec, out) {
			out = cloneRecord(rec)
			found = true
		}
	}
	return out, found
}

func (s *localRecordSet) repair(rec Record, now time.Time) bool {
	if rec.Key == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, found := s.records[rec.Key]
	if found && !recordNewer(rec, cur) {
		if s.promised[rec.Key].Less(cur.Meta.Ballot) {
			s.promised[rec.Key] = cur.Meta.Ballot
		}
		if s.setPromised.Less(cur.Meta.Ballot) {
			s.setPromised = cur.Meta.Ballot
		}
		return false
	}
	rec.Namespace = s.namespace
	rec.Shard = s.shard
	rec.RecordSet = s.name
	rec.Value = append([]byte(nil), rec.Value...)
	s.putLocked(rec, now, true)
	if s.promised[rec.Key].Less(rec.Meta.Ballot) {
		s.promised[rec.Key] = rec.Meta.Ballot
	}
	if s.setPromised.Less(rec.Meta.Ballot) {
		s.setPromised = rec.Meta.Ballot
	}
	return true
}

func (s *localRecordSet) install(records []Record, rev uint64, certificate CommitCertificate, now time.Time) bool {
	return s.installLocked(records, rev, certificate, now, true)
}

func (s *localRecordSet) installNoTouch(records []Record, rev uint64, certificate CommitCertificate) bool {
	return s.installLocked(records, rev, certificate, time.Time{}, false)
}

func (s *localRecordSet) installLocked(records []Record, rev uint64, certificate CommitCertificate, now time.Time, touch bool) bool {
	next := make(map[RecordKey]Record, len(records))
	var maxRev uint64
	for _, rec := range records {
		if rec.Key == "" {
			return false
		}
		rec.Namespace = s.namespace
		rec.Shard = s.shard
		rec.RecordSet = s.name
		rec.Value = append([]byte(nil), rec.Value...)
		if maxRev < rec.Meta.Rev {
			maxRev = rec.Meta.Rev
		}
		next[rec.Key] = rec
	}
	if rev < maxRev {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if rev < s.rev && !s.certificate.IsZero() {
		return false
	}
	changed := s.rev != rev || len(s.records) != len(next)
	if !changed {
		for key, rec := range next {
			cur, ok := s.records[key]
			if !ok || !sameCommittedRecord(rec, cur) || !rec.Meta.Ballot.IsZero() && cur.Meta.Ballot != rec.Meta.Ballot {
				changed = true
				break
			}
		}
	}
	if rev == s.rev {
		if changed {
			if !s.certificate.IsZero() && !s.sameLogicalSnapshotLocked(next, rev) {
				return false
			}
		} else {
			if !certificate.IsZero() {
				s.certificate = cloneCommitCertificate(certificate)
			}
			for acceptedRev := range s.accepted {
				if acceptedRev <= rev {
					delete(s.accepted, acceptedRev)
				}
			}
			if touch {
				s.lastAccess = now
			}
			s.refreshPromisesLocked()
			return true
		}
	}
	if changed && !certificate.IsZero() {
		if rec, ok := s.incrementalInstallRecordLocked(next, rev); ok {
			s.certificate = cloneCommitCertificate(certificate)
			for acceptedRev := range s.accepted {
				if acceptedRev <= rev {
					delete(s.accepted, acceptedRev)
				}
			}
			s.putLocked(rec, now, touch)
			if s.promised[rec.Key].Less(rec.Meta.Ballot) {
				s.promised[rec.Key] = rec.Meta.Ballot
			}
			if s.setPromised.Less(rec.Meta.Ballot) {
				s.setPromised = rec.Meta.Ballot
			}
			return true
		}
	}
	s.records = next
	s.rev = rev
	s.certificate = cloneCommitCertificate(certificate)
	for acceptedRev := range s.accepted {
		if acceptedRev <= rev {
			delete(s.accepted, acceptedRev)
		}
	}
	if touch {
		s.lastAccess = now
	}
	s.refreshPromisesLocked()
	if changed {
		s.log = nil
		s.broadcastResetLocked()
	}
	return true
}

func (s *localRecordSet) sameLogicalSnapshotLocked(next map[RecordKey]Record, rev uint64) bool {
	curRecords := make([]Record, 0, len(s.records))
	for _, rec := range s.records {
		curRecords = append(curRecords, cloneRecord(rec))
	}
	nextRecords := make([]Record, 0, len(next))
	for _, rec := range next {
		nextRecords = append(nextRecords, cloneRecord(rec))
	}
	curDigest, err := snapshotDigest(rev, normalizeSnapshotRecords(curRecords))
	if err != nil {
		return false
	}
	nextDigest, err := snapshotDigest(rev, normalizeSnapshotRecords(nextRecords))
	if err != nil {
		return false
	}
	return curDigest == nextDigest
}

func (s *localRecordSet) refreshPromisesLocked() {
	for key, rec := range s.records {
		if s.promised[key].Less(rec.Meta.Ballot) {
			s.promised[key] = rec.Meta.Ballot
		}
		if s.setPromised.Less(rec.Meta.Ballot) {
			s.setPromised = rec.Meta.Ballot
		}
	}
}

func (s *localRecordSet) incrementalInstallRecordLocked(next map[RecordKey]Record, rev uint64) (Record, bool) {
	if rev != s.rev+1 {
		return Record{}, false
	}
	var changed Record
	changedCount := 0
	for key, cur := range s.records {
		rec, ok := next[key]
		if !ok {
			return Record{}, false
		}
		if !sameCommittedRecord(rec, cur) || !rec.Meta.Ballot.IsZero() && cur.Meta.Ballot != rec.Meta.Ballot {
			changed = rec
			changedCount++
		}
	}
	for key, rec := range next {
		if _, ok := s.records[key]; ok {
			continue
		}
		changed = rec
		changedCount++
	}
	if changedCount != 1 || changed.Meta.Rev != rev {
		return Record{}, false
	}
	return cloneRecord(changed), true
}

func (s *localRecordSet) putLocked(rec Record, now time.Time, touch bool) {
	cur, found := s.records[rec.Key]
	changed := !found || cur.Deleted != rec.Deleted || cur.Meta.Rev != rec.Meta.Rev || !bytes.Equal(cur.Value, rec.Value)
	s.records[rec.Key] = rec
	if touch {
		s.lastAccess = now
	}
	if !changed {
		return
	}
	previousRev := s.rev
	if rec.Meta.Rev > s.rev {
		s.rev = rec.Meta.Rev
	}
	if rec.Meta.Rev <= previousRev {
		return
	}
	if rec.Meta.Rev > previousRev+1 {
		s.resetWatchersLocked()
		s.log = nil
	}
	ev := WatchEvent{
		Namespace: s.namespace, Shard: s.shard, RecordSet: s.name, Key: rec.Key, Record: cloneRecord(rec),
		Rev: rec.Meta.Rev, Token: makeWatchToken(s.epoch, "", s.name, rec.Meta.Rev),
	}
	if rec.Deleted {
		ev.Type = EventDelete
	} else {
		ev.Type = EventPut
	}
	s.log = append(s.log, ev)
	if s.retention <= 0 {
		s.retention = 10000
	}
	if len(s.log) > s.retention {
		copy(s.log, s.log[len(s.log)-s.retention:])
		s.log = s.log[:s.retention]
	}
	for id, sub := range s.subs {
		out := ev
		out.Token = makeWatchToken(s.epoch, sub.label, s.name, rec.Meta.Rev)
		s.enqueueWatchEventLocked(id, sub, out)
	}
}

func sameCommittedRecord(a, b Record) bool {
	return a.Key == b.Key &&
		a.Deleted == b.Deleted &&
		a.Meta.Rev == b.Meta.Rev &&
		bytes.Equal(a.Value, b.Value)
}

func (s *localRecordSet) resetWatchersLocked() {
	for id, sub := range s.subs {
		delete(s.subs, id)
		sub.close()
	}
}

func (s *localRecordSet) broadcastResetLocked() {
	for id, sub := range s.subs {
		for _, ev := range s.resetEventsLocked(sub.label) {
			if !s.enqueueWatchEventLocked(id, sub, ev) {
				break
			}
		}
	}
}

func (s *localRecordSet) enqueueWatchEventLocked(id int, sub *watchSub, ev WatchEvent) bool {
	select {
	case <-sub.done:
		delete(s.subs, id)
		sub.close()
		return false
	case sub.queue <- ev:
		return true
	default:
		delete(s.subs, id)
		sub.close()
		return false
	}
}

func (s *localRecordSet) snapshotRecords(includeDeleted bool) ([]Record, uint64, CommitCertificate) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Record, 0, len(s.records))
	for _, rec := range s.records {
		if rec.Deleted && !includeDeleted {
			continue
		}
		out = append(out, cloneRecord(rec))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, s.rev, cloneCommitCertificate(s.certificate)
}

func (s *localRecordSet) markReady(label string, rev uint64) {
	s.mu.Lock()
	if rev > s.rev {
		if rev > s.rev+1 {
			s.resetWatchersLocked()
			s.log = nil
		}
		s.rev = rev
	}
	if _, ok := s.readyLabel[label]; !ok || s.readyLabel[label] < rev {
		s.readyLabel[label] = rev
	}
	s.mu.Unlock()
}

func (s *localRecordSet) ready(label string, rev uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	readyRev, ok := s.readyLabel[label]
	return ok && readyRev >= rev && s.rev >= rev
}

func (s *localShard) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rs := range s.recordSets {
		rs.close()
	}
}

func (s *localRecordSet) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, sub := range s.subs {
		delete(s.subs, id)
		sub.close()
	}
}

func (s *localRecordSet) snapshot(label string) Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	records := make([]Record, 0, len(s.records))
	for _, rec := range s.records {
		if rec.Deleted {
			continue
		}
		records = append(records, cloneRecord(rec))
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Key < records[j].Key })
	token := makeWatchToken(s.epoch, label, s.name, s.rev)
	return Snapshot{Namespace: s.namespace, Shard: s.shard, RecordSet: s.name, Label: label, Epoch: s.epoch, Rev: s.rev, Token: token, Records: records}
}

func (s *localRecordSet) watch(ctx context.Context, label, token string) Watch {
	s.mu.Lock()
	defer s.mu.Unlock()
	parsed, ok := parseWatchToken(token)
	reset := token == "" || !ok || parsed.Epoch != s.epoch || parsed.Label != label || parsed.RecordSet != s.name
	fromRev := parsed.Rev
	if !reset && (fromRev > s.rev || watchLogUnavailableLocked(s.log, s.rev, fromRev)) {
		reset = true
	}

	currentToken := makeWatchToken(s.epoch, label, s.name, s.rev)
	var initial []WatchEvent
	if reset {
		initial = s.resetEventsLocked(label)
	} else {
		for _, ev := range s.log {
			if ev.Rev > fromRev {
				ev.Token = makeWatchToken(s.epoch, label, s.name, ev.Rev)
				initial = append(initial, ev)
			}
		}
	}
	ch := s.addWatcherLocked(ctx, label, initial)
	return Watch{Reset: reset, Token: currentToken, Events: ch}
}

func (s *localRecordSet) watchSince(ctx context.Context, label string, fromRev uint64) (Watch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	reset := false
	if fromRev > s.rev {
		fromRev = s.rev
	}
	if watchLogUnavailableLocked(s.log, s.rev, fromRev) {
		if fromRev > 0 {
			return Watch{}, ErrCompacted
		}
		reset = true
	}

	currentToken := makeWatchToken(s.epoch, label, s.name, s.rev)
	var initial []WatchEvent
	if reset {
		initial = s.resetEventsLocked(label)
	} else {
		for _, ev := range s.log {
			if ev.Rev > fromRev {
				ev.Token = makeWatchToken(s.epoch, label, s.name, ev.Rev)
				initial = append(initial, ev)
			}
		}
	}
	ch := s.addWatcherLocked(ctx, label, initial)
	return Watch{Reset: reset, Token: currentToken, Events: ch}, nil
}

func watchLogUnavailableLocked(log []WatchEvent, head, fromRev uint64) bool {
	if fromRev >= head {
		return false
	}
	if len(log) == 0 {
		return true
	}
	return fromRev < log[0].Rev-1
}

func (s *localRecordSet) resetEventsLocked(label string) []WatchEvent {
	currentToken := makeWatchToken(s.epoch, label, s.name, s.rev)
	events := []WatchEvent{{Type: EventReset, Namespace: s.namespace, Shard: s.shard, RecordSet: s.name, Rev: s.rev, Token: currentToken}}
	keys := make([]RecordKey, 0, len(s.records))
	for key, rec := range s.records {
		if !rec.Deleted {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for _, key := range keys {
		rec := cloneRecord(s.records[key])
		events = append(events, WatchEvent{Type: EventPut, Namespace: s.namespace, Shard: s.shard, RecordSet: s.name, Key: key, Record: rec, Rev: s.rev, Token: currentToken})
	}
	events = append(events, WatchEvent{Type: EventBookmark, Namespace: s.namespace, Shard: s.shard, RecordSet: s.name, Rev: s.rev, Token: currentToken})
	return events
}

func (s *localRecordSet) addWatcherLocked(ctx context.Context, label string, initial []WatchEvent) <-chan WatchEvent {
	ch := make(chan WatchEvent, 1024)
	sub := &watchSub{
		label: label,
		queue: make(chan WatchEvent, 1024),
		done:  make(chan struct{}),
	}
	id := s.nextSub
	s.nextSub++
	s.subs[id] = sub
	go sub.run(ctx, ch, initial)
	go func() {
		<-sub.done
		s.mu.Lock()
		if cur, ok := s.subs[id]; ok && cur == sub {
			delete(s.subs, id)
			sub.close()
		}
		s.mu.Unlock()
	}()
	return ch
}

func (s *watchSub) run(ctx context.Context, out chan<- WatchEvent, initial []WatchEvent) {
	defer close(s.done)
	defer close(out)
	for _, ev := range initial {
		select {
		case out <- ev:
		case <-ctx.Done():
			return
		}
	}
	for {
		select {
		case ev, ok := <-s.queue:
			if !ok {
				return
			}
			select {
			case out <- ev:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *watchSub) close() {
	s.closeOnce.Do(func() {
		close(s.queue)
	})
}

type watchToken struct {
	Epoch     string        `json:"epoch"`
	Label     string        `json:"label"`
	RecordSet RecordSetName `json:"record_set"`
	Rev       uint64        `json:"rev"`
}

func makeWatchToken(epoch, label string, recordSet RecordSetName, rev uint64) string {
	raw, _ := json.Marshal(watchToken{Epoch: epoch, Label: label, RecordSet: recordSet, Rev: rev})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func parseWatchToken(token string) (watchToken, bool) {
	if token == "" {
		return watchToken{}, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return watchToken{}, false
	}
	var out watchToken
	if err := json.Unmarshal(raw, &out); err != nil {
		return watchToken{}, false
	}
	return out, true
}

func newEpoch() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return base64.RawURLEncoding.EncodeToString(b[:])
}
