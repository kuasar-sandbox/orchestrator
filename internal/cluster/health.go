package cluster

import (
	"sync"
	"time"
)

type ReplicaAvailability interface {
	Available() bool
	MarkSuccess()
	MarkFailure()
}

type ReplicaHealth struct {
	mu             sync.Mutex
	cooldown       time.Duration
	unhealthyUntil time.Time
}

func NewReplicaHealth(cooldown time.Duration) *ReplicaHealth {
	if cooldown <= 0 {
		cooldown = 2 * time.Second
	}
	return &ReplicaHealth{cooldown: cooldown}
}

func (h *ReplicaHealth) Available() bool {
	if h == nil {
		return true
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return time.Now().After(h.unhealthyUntil)
}

func (h *ReplicaHealth) MarkSuccess() {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.unhealthyUntil = time.Time{}
	h.mu.Unlock()
}

func (h *ReplicaHealth) MarkFailure() {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.unhealthyUntil = time.Now().Add(h.cooldown)
	h.mu.Unlock()
}

type FuncReplicaAvailability struct {
	available func() bool
	cooldown  *ReplicaHealth
}

func NewFuncReplicaAvailability(available func() bool, cooldown time.Duration) *FuncReplicaAvailability {
	return &FuncReplicaAvailability{available: available, cooldown: NewReplicaHealth(cooldown)}
}

func (h *FuncReplicaAvailability) Available() bool {
	if h == nil {
		return true
	}
	if h.available != nil && !h.available() {
		return false
	}
	return h.cooldown == nil || h.cooldown.Available()
}

func (h *FuncReplicaAvailability) MarkSuccess() {
	if h != nil && h.cooldown != nil {
		h.cooldown.MarkSuccess()
	}
}

func (h *FuncReplicaAvailability) MarkFailure() {
	if h != nil && h.cooldown != nil {
		h.cooldown.MarkFailure()
	}
}
