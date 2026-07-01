package shardkv

import (
	"context"
	"errors"
	"time"
)

type Namespace string
type ShardKey string
type RecordSetName string
type RecordKey string
type MemberID string

type Ballot struct {
	Round  uint64   `json:"round"`
	Writer MemberID `json:"writer"`
}

func (b Ballot) Less(o Ballot) bool {
	if b.Round != o.Round {
		return b.Round < o.Round
	}
	return b.Writer < o.Writer
}

func (b Ballot) IsZero() bool { return b.Round == 0 && b.Writer == "" }

type RecordMeta struct {
	Ballot    Ballot    `json:"ballot"`
	Rev       uint64    `json:"rev"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Record struct {
	Namespace Namespace     `json:"namespace"`
	Shard     ShardKey      `json:"shard"`
	RecordSet RecordSetName `json:"record_set"`
	Key       RecordKey     `json:"key"`
	Value     []byte        `json:"value,omitempty"`
	Deleted   bool          `json:"deleted,omitempty"`
	Meta      RecordMeta    `json:"meta"`
}

type NamespaceSpec struct {
	ShardMemberCount   int           `json:"shard_member_count"`
	TombstoneRetention time.Duration `json:"tombstone_retention"`
	WatchRetention     int           `json:"watch_retention"`
	IdleShardTTL       time.Duration `json:"idle_shard_ttl"`
	Pinned             bool          `json:"pinned"`
}

type Layout struct {
	Namespaces map[Namespace]NamespaceSpec `json:"namespaces"`
}

type ShardMemberSet struct {
	Version int64      `json:"version"`
	Label   string     `json:"label"`
	Members []MemberID `json:"members"`
	Quorum  int        `json:"quorum"`
}

type ShardView struct {
	Namespace Namespace        `json:"namespace"`
	Shard     ShardKey         `json:"shard"`
	Local     MemberID         `json:"local"`
	Label     string           `json:"label"`
	Sets      []ShardMemberSet `json:"sets"`
}

type ShardResolver interface {
	ResolveShard(ns Namespace, shard ShardKey) (ShardView, error)
}

type MemberReadyProvider interface {
	Ready(label string, member MemberID) bool
}

type Transport interface {
	Call(ctx context.Context, member MemberID, req Request) (Response, error)
}

type TransportFunc func(ctx context.Context, member MemberID, req Request) (Response, error)

func (f TransportFunc) Call(ctx context.Context, member MemberID, req Request) (Response, error) {
	return f(ctx, member, req)
}

type Op string

const (
	OpRead     Op = "read"
	OpPrepare  Op = "prepare"
	OpAccept   Op = "accept"
	OpRepair   Op = "repair"
	OpSnapshot Op = "snapshot"
	OpInstall  Op = "install"
)

type Request struct {
	Op        Op            `json:"op"`
	Label     string        `json:"label,omitempty"`
	Namespace Namespace     `json:"namespace"`
	Shard     ShardKey      `json:"shard"`
	RecordSet RecordSetName `json:"record_set"`
	Key       RecordKey     `json:"key,omitempty"`
	Ballot    Ballot        `json:"ballot,omitempty"`
	Record    Record        `json:"record,omitempty"`
	Records   []Record      `json:"records,omitempty"`
	Rev       uint64        `json:"rev,omitempty"`
}

type Response struct {
	Record   Record   `json:"record,omitempty"`
	Records  []Record `json:"records,omitempty"`
	Found    bool     `json:"found,omitempty"`
	OK       bool     `json:"ok,omitempty"`
	Promised Ballot   `json:"promised,omitempty"`
	Rev      uint64   `json:"rev,omitempty"`
	Error    string   `json:"error,omitempty"`
}

type Snapshot struct {
	Namespace Namespace     `json:"namespace"`
	Shard     ShardKey      `json:"shard"`
	RecordSet RecordSetName `json:"record_set"`
	Label     string        `json:"label"`
	Epoch     string        `json:"epoch"`
	Rev       uint64        `json:"rev"`
	Token     string        `json:"token"`
	Records   []Record      `json:"records"`
}

type EventType string

const (
	EventReset    EventType = "reset"
	EventPut      EventType = "put"
	EventDelete   EventType = "delete"
	EventBookmark EventType = "bookmark"
)

type WatchEvent struct {
	Type      EventType     `json:"type"`
	Namespace Namespace     `json:"namespace"`
	Shard     ShardKey      `json:"shard"`
	RecordSet RecordSetName `json:"record_set"`
	Key       RecordKey     `json:"key,omitempty"`
	Record    Record        `json:"record,omitempty"`
	Rev       uint64        `json:"rev,omitempty"`
	Token     string        `json:"token,omitempty"`
}

type Watch struct {
	Reset  bool
	Token  string
	Events <-chan WatchEvent
}

var (
	ErrUnknownNamespace   = errors.New("shardkv: unknown namespace")
	ErrInvalidView        = errors.New("shardkv: invalid shard view")
	ErrQuorum             = errors.New("shardkv: quorum unavailable")
	ErrConflict           = errors.New("shardkv: record conflict")
	ErrStaleBallot        = errors.New("shardkv: stale ballot")
	ErrReplicaUnavailable = errors.New("shardkv: replica unavailable")
	ErrCompacted          = errors.New("shardkv: watch compacted")
	ErrClosed             = errors.New("shardkv: store closed")
)

func cloneRecord(in Record) Record {
	in.Value = append([]byte(nil), in.Value...)
	return in
}

func recordNewer(a, b Record) bool {
	if a.Meta.Rev != b.Meta.Rev {
		return a.Meta.Rev > b.Meta.Rev
	}
	return b.Meta.Ballot.Less(a.Meta.Ballot)
}

func highest(records []recordRead) (Record, bool) {
	var best Record
	found := false
	for _, r := range records {
		if !r.found {
			continue
		}
		if !found || recordNewer(r.record, best) {
			best = r.record
			found = true
		}
	}
	return best, found
}

type recordRead struct {
	member MemberID
	record Record
	found  bool
	head   uint64
}
