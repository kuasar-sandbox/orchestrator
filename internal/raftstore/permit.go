package raftstore

import (
	"errors"
	"sync"
	"time"
)

const MaximumServePermitMillis = uint64(60_000)

var (
	ErrPermitMissing  = errors.New("raftstore: Serve Permit is missing")
	ErrPermitExpired  = errors.New("raftstore: Serve Permit expired")
	ErrPermitMismatch = errors.New("raftstore: Serve Permit identity mismatch")
	ErrPermitDenied   = errors.New("raftstore: operation is closed by the Serve Permit")
	ErrPermitCapacity = errors.New("raftstore: Serve Permit identity capacity exceeded")
)

const maximumCachedServePermits = 2

type PermitOperation uint8

const (
	PermitRegistryRead PermitOperation = iota + 1
	PermitRegistryWrite
	PermitHolderProbe
	PermitHolderDispatch
	PermitHolderEvent
	PermitHolderEventAck
	PermitRouterCacheForward
)

type PermitIdentity struct {
	ClusterID         string `json:"cluster_id"`
	StorageGeneration string `json:"storage_generation"`
	SystemEpoch       uint64 `json:"system_epoch"`
	ManifestDigest    string `json:"manifest_digest"`
}

func (i PermitIdentity) Validate() error {
	if i.ClusterID == "" || i.StorageGeneration == "" || i.SystemEpoch == 0 || !isSHA256(i.ManifestDigest) {
		return errors.New("raftstore: incomplete permit identity")
	}
	return nil
}

type PermitGrant struct {
	PermitIdentity
	CommitIndex       uint64 `json:"commit_index"`
	MaxLifetimeMillis uint64 `json:"max_lifetime_millis"`
	ServeGate         bool   `json:"serve_gate"`
	WriteGate         bool   `json:"write_gate"`
	CutoverGate       bool   `json:"cutover_gate"`
	RecoveryClosed    bool   `json:"recovery_closed"`
}

func (g PermitGrant) Validate() error {
	if err := g.PermitIdentity.Validate(); err != nil {
		return err
	}
	if g.CommitIndex == 0 || g.MaxLifetimeMillis == 0 || g.MaxLifetimeMillis > MaximumServePermitMillis {
		return errors.New("raftstore: permit grant is not quorum-confirmed")
	}
	return nil
}

type ServePermit struct {
	Grant     PermitGrant
	expiresAt time.Time
}

// NewServePermit uses the local monotonic proposal start, so response delay can
// only shorten a permit and can never extend it beyond the committed refresh.
func NewServePermit(grant PermitGrant, proposalStarted time.Time) (ServePermit, error) {
	if err := grant.Validate(); err != nil {
		return ServePermit{}, err
	}
	return ServePermit{
		Grant:     grant,
		expiresAt: proposalStarted.Add(time.Duration(grant.MaxLifetimeMillis) * time.Millisecond),
	}, nil
}

func (p ServePermit) Authorize(now time.Time, identity PermitIdentity, operation PermitOperation) error {
	if err := identity.Validate(); err != nil {
		return err
	}
	if p.expiresAt.IsZero() {
		return ErrPermitMissing
	}
	if !now.Before(p.expiresAt) {
		return ErrPermitExpired
	}
	if p.Grant.PermitIdentity != identity {
		return ErrPermitMismatch
	}
	if !p.Grant.ServeGate || !p.Grant.CutoverGate || !p.Grant.RecoveryClosed {
		return ErrPermitDenied
	}
	switch operation {
	case PermitRegistryRead, PermitHolderProbe, PermitRouterCacheForward:
		return nil
	case PermitRegistryWrite, PermitHolderDispatch, PermitHolderEvent, PermitHolderEventAck:
		if !p.Grant.WriteGate {
			return ErrPermitDenied
		}
		return nil
	default:
		return ErrPermitDenied
	}
}

type PermitCache struct {
	mu             sync.RWMutex
	now            func() time.Time
	permits        map[PermitIdentity]ServePermit
	clusterID      string
	generation     string
	retiredThrough uint64
}

func NewPermitCache(now func() time.Time) *PermitCache {
	if now == nil {
		now = time.Now
	}
	return &PermitCache{now: now, permits: make(map[PermitIdentity]ServePermit, maximumCachedServePermits)}
}

func (c *PermitCache) Install(grant PermitGrant, proposalStarted time.Time) error {
	permit, err := NewServePermit(grant, proposalStarted)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.clusterID == "" {
		c.clusterID = grant.ClusterID
		c.generation = grant.StorageGeneration
	} else if c.clusterID != grant.ClusterID || c.generation != grant.StorageGeneration {
		return errors.New("raftstore: Serve Permit cache belongs to another storage generation")
	}
	current, found := c.permits[grant.PermitIdentity]
	if found && current.Grant.CommitIndex > grant.CommitIndex {
		return errors.New("raftstore: older Serve Permit cannot replace a newer permit")
	}
	if found && current.Grant.CommitIndex == grant.CommitIndex {
		if current.Grant != grant {
			return errors.New("raftstore: Serve Permit commit index equivocation")
		}
		// Replaying one grant must not extend its original monotonic expiry.
		return nil
	}
	for _, installed := range c.permits {
		if installed.Grant.CommitIndex == grant.CommitIndex && installed.Grant != grant {
			return errors.New("raftstore: Serve Permit commit index equivocation")
		}
	}
	if grant.CommitIndex <= c.retiredThrough {
		return errors.New("raftstore: retired Serve Permit cannot be reinstalled")
	}
	if !found && len(c.permits) == maximumCachedServePermits {
		now := c.now()
		for identity, installed := range c.permits {
			if !now.Before(installed.expiresAt) {
				if installed.Grant.CommitIndex > c.retiredThrough {
					c.retiredThrough = installed.Grant.CommitIndex
				}
				delete(c.permits, identity)
			}
		}
		if grant.CommitIndex <= c.retiredThrough {
			return errors.New("raftstore: retired Serve Permit cannot be reinstalled")
		}
	}
	if !found && len(c.permits) == maximumCachedServePermits {
		return ErrPermitCapacity
	}
	c.permits[grant.PermitIdentity] = permit
	return nil
}

func (c *PermitCache) Authorize(identity PermitIdentity, operation PermitOperation) error {
	c.mu.RLock()
	permit, found := c.permits[identity]
	count := len(c.permits)
	c.mu.RUnlock()
	if !found {
		if count == 0 {
			return ErrPermitMissing
		}
		return ErrPermitMismatch
	}
	return permit.Authorize(c.now(), identity, operation)
}

func (c *PermitCache) Clear() {
	c.mu.Lock()
	for _, permit := range c.permits {
		if permit.Grant.CommitIndex > c.retiredThrough {
			c.retiredThrough = permit.Grant.CommitIndex
		}
	}
	clear(c.permits)
	c.mu.Unlock()
}
