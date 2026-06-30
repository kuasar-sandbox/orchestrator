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

	mu     sync.Mutex
	closed bool
	shards map[string]*localShard
	rounds map[string]uint64
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
		opts.MaxAttempts = 5
	}
	if opts.RepairTimeout <= 0 {
		opts.RepairTimeout = 250 * time.Millisecond
	}
	return &Store{
		local: opts.Local, resolver: opts.Resolver, transport: opts.Transport, ready: opts.Ready,
		now: now, epoch: epoch, defaultWatchRetention: opts.DefaultWatchRetention,
		maxAttempts: opts.MaxAttempts, repairTimeout: opts.RepairTimeout,
		shards: map[string]*localShard{}, rounds: map[string]uint64{},
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
	switch req.Op {
	case OpSnapshot:
		ls, err := s.getLocalShardNoTouch(req.Namespace, req.Shard)
		if err != nil {
			return Response{}, err
		}
		records, rev := ls.snapshotRecords(true)
		return Response{Records: records, OK: true, Rev: rev}, nil
	case OpRepair:
		ls, err := s.getLocalShardNoTouch(req.Namespace, req.Shard)
		if err != nil {
			return Response{}, err
		}
		ok := ls.repair(req.Record, s.now())
		return Response{OK: ok}, nil
	}
	ls, err := s.getLocalShard(req.Namespace, req.Shard)
	if err != nil {
		return Response{}, err
	}
	switch req.Op {
	case OpPrepare:
		rec, found, ok, promised := ls.prepare(req.Key, req.Ballot)
		return Response{Record: rec, Found: found, OK: ok, Promised: promised}, nil
	case OpAccept:
		ok, promised := ls.accept(req.Key, req.Record, req.Ballot, s.now())
		return Response{OK: ok, Promised: promised}, nil
	default:
		return Response{}, fmt.Errorf("shardkv: unknown op %q", req.Op)
	}
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
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.rounds[key]
	if cur < atLeast.Round {
		cur = atLeast.Round
	}
	cur++
	s.rounds[key] = cur
	return Ballot{Round: cur, Writer: s.local}
}

func (s *Store) bumpRound(key string, promised Ballot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rounds[key] < promised.Round {
		s.rounds[key] = promised.Round
	}
}

func localShardKey(ns Namespace, shard ShardKey) string {
	return string(ns) + "\x00" + string(shard)
}

func recordRoundKey(ns Namespace, shard ShardKey, key RecordKey) string {
	return string(ns) + "\x00" + string(shard) + "\x00" + string(key)
}

type localShard struct {
	namespace Namespace
	shard     ShardKey
	epoch     string
	retention int

	mu         sync.Mutex
	records    map[RecordKey]Record
	promised   map[RecordKey]Ballot
	rev        uint64
	log        []WatchEvent
	subs       map[int]chan WatchEvent
	nextSub    int
	readyLabel map[string]bool
	lastAccess time.Time
}

func newLocalShard(ns Namespace, shard ShardKey, epoch string, retention int, now time.Time) *localShard {
	return &localShard{
		namespace: ns, shard: shard, epoch: epoch, retention: retention,
		records: map[RecordKey]Record{}, promised: map[RecordKey]Ballot{},
		subs: map[int]chan WatchEvent{}, readyLabel: map[string]bool{},
		lastAccess: now,
	}
}

func (s *localShard) touch(now time.Time) {
	s.mu.Lock()
	s.lastAccess = now
	s.mu.Unlock()
}

func (s *localShard) prepare(key RecordKey, ballot Ballot) (Record, bool, bool, Ballot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	promised := s.promised[key]
	if ballot.Less(promised) {
		return Record{}, false, false, promised
	}
	s.promised[key] = ballot
	rec, found := s.records[key]
	return cloneRecord(rec), found, true, ballot
}

func (s *localShard) accept(key RecordKey, rec Record, ballot Ballot, now time.Time) (bool, Ballot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	promised := s.promised[key]
	if ballot.Less(promised) {
		return false, promised
	}
	cur, found := s.records[key]
	if found && rec.Meta.Ballot.Less(cur.Meta.Ballot) {
		if promised.Less(cur.Meta.Ballot) {
			s.promised[key] = cur.Meta.Ballot
		}
		return false, s.promised[key]
	}
	rec.Namespace = s.namespace
	rec.Shard = s.shard
	rec.Key = key
	rec.Value = append([]byte(nil), rec.Value...)
	s.promised[key] = ballot
	s.putLocked(rec, now)
	return true, ballot
}

func (s *localShard) repair(rec Record, now time.Time) bool {
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
		return false
	}
	rec.Namespace = s.namespace
	rec.Shard = s.shard
	rec.Value = append([]byte(nil), rec.Value...)
	s.putLocked(rec, now)
	if s.promised[rec.Key].Less(rec.Meta.Ballot) {
		s.promised[rec.Key] = rec.Meta.Ballot
	}
	return true
}

func (s *localShard) putLocked(rec Record, now time.Time) {
	cur, found := s.records[rec.Key]
	changed := !found || cur.Deleted != rec.Deleted || cur.Meta.Rev != rec.Meta.Rev || !bytes.Equal(cur.Value, rec.Value)
	s.records[rec.Key] = rec
	s.lastAccess = now
	if !changed {
		return
	}
	s.rev++
	ev := WatchEvent{
		Namespace: s.namespace, Shard: s.shard, Key: rec.Key, Record: cloneRecord(rec),
		Rev: s.rev, Token: makeWatchToken(s.epoch, "", s.rev),
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
	for id, ch := range s.subs {
		select {
		case ch <- ev:
		default:
			delete(s.subs, id)
			close(ch)
		}
	}
}

func (s *localShard) snapshotRecords(includeDeleted bool) ([]Record, uint64) {
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
	return out, s.rev
}

func (s *localShard) markReady(label string) {
	s.mu.Lock()
	s.readyLabel[label] = true
	s.mu.Unlock()
}

func (s *localShard) ready(label string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readyLabel[label]
}

func (s *localShard) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, ch := range s.subs {
		delete(s.subs, id)
		close(ch)
	}
}

func (s *localShard) snapshot(label string) Snapshot {
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
	token := makeWatchToken(s.epoch, label, s.rev)
	return Snapshot{Namespace: s.namespace, Shard: s.shard, Label: label, Epoch: s.epoch, Rev: s.rev, Token: token, Records: records}
}

func (s *localShard) watch(ctx context.Context, label, token string) Watch {
	s.mu.Lock()
	defer s.mu.Unlock()
	parsed, ok := parseWatchToken(token)
	reset := token == "" || !ok || parsed.Epoch != s.epoch || parsed.Label != label
	fromRev := parsed.Rev
	if !reset && len(s.log) > 0 && fromRev < s.log[0].Rev-1 {
		reset = true
	}

	ch := make(chan WatchEvent, 1024)
	currentToken := makeWatchToken(s.epoch, label, s.rev)
	if reset {
		ch <- WatchEvent{Type: EventReset, Namespace: s.namespace, Shard: s.shard, Rev: s.rev, Token: currentToken}
		keys := make([]RecordKey, 0, len(s.records))
		for key, rec := range s.records {
			if !rec.Deleted {
				keys = append(keys, key)
			}
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		for _, key := range keys {
			rec := cloneRecord(s.records[key])
			ch <- WatchEvent{Type: EventPut, Namespace: s.namespace, Shard: s.shard, Key: key, Record: rec, Rev: s.rev, Token: currentToken}
		}
		ch <- WatchEvent{Type: EventBookmark, Namespace: s.namespace, Shard: s.shard, Rev: s.rev, Token: currentToken}
	} else {
		for _, ev := range s.log {
			if ev.Rev > fromRev {
				ev.Token = makeWatchToken(s.epoch, label, ev.Rev)
				ch <- ev
			}
		}
	}
	id := s.nextSub
	s.nextSub++
	s.subs[id] = ch
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		if cur, ok := s.subs[id]; ok {
			delete(s.subs, id)
			close(cur)
		}
		s.mu.Unlock()
	}()
	return Watch{Reset: reset, Token: currentToken, Events: ch}
}

func (s *localShard) watchSince(ctx context.Context, label string, fromRev uint64) (Watch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if fromRev > 0 && len(s.log) > 0 && fromRev < s.log[0].Rev-1 {
		return Watch{}, ErrCompacted
	}

	ch := make(chan WatchEvent, 1024)
	currentToken := makeWatchToken(s.epoch, label, s.rev)
	for _, ev := range s.log {
		if ev.Rev > fromRev {
			ev.Token = makeWatchToken(s.epoch, label, ev.Rev)
			ch <- ev
		}
	}
	id := s.nextSub
	s.nextSub++
	s.subs[id] = ch
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		if cur, ok := s.subs[id]; ok {
			delete(s.subs, id)
			close(cur)
		}
		s.mu.Unlock()
	}()
	return Watch{Token: currentToken, Events: ch}, nil
}

type watchToken struct {
	Epoch string `json:"epoch"`
	Label string `json:"label"`
	Rev   uint64 `json:"rev"`
}

func makeWatchToken(epoch, label string, rev uint64) string {
	raw, _ := json.Marshal(watchToken{Epoch: epoch, Label: label, Rev: rev})
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
