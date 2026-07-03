// Package membergroup wraps hashicorp/memberlist for the cluster control plane.
//
// Membership lists still come from registry configuration or placer_link seeds.
// memberlist is used only as a failure detector and metadata propagation channel.
package membergroup

import (
	"encoding/json"
	"sync"
)

const (
	DefaultPlacerLabel = "placer.default"

	RoleRegistry = "registry"
	RolePlacer   = "placer"
	RoleObserver = "observer"
)

// Meta is the small memberlist metadata payload used by registry/placer roles.
type Meta struct {
	Role       string `json:"role"`
	ID         string `json:"id"`
	Advertise  string `json:"advertise,omitempty"`
	Ready      bool   `json:"ready,omitempty"`
	ReadyLabel string `json:"ready_label,omitempty"`
}

func encodeMeta(meta Meta, limit int) []byte {
	if meta.ID == "" {
		meta.ID = meta.Role
	}
	raw, err := json.Marshal(meta)
	if err != nil || len(raw) > limit {
		return nil
	}
	return raw
}

func DecodeMeta(raw []byte) (Meta, bool) {
	var meta Meta
	if len(raw) == 0 {
		return meta, false
	}
	if err := json.Unmarshal(raw, &meta); err != nil {
		return Meta{}, false
	}
	return meta, true
}

// AddressBook is the name -> HTTP base URL map used by the HTTP transport.
// Configured registry members and placer_link placer seeds populate it; memberlist
// metadata updates keep it fresh after join.
type AddressBook struct {
	mu    sync.RWMutex
	addrs map[string]string
}

func NewAddressBook(seed map[string]string) *AddressBook {
	b := &AddressBook{addrs: map[string]string{}}
	for id, addr := range seed {
		b.Set(id, addr)
	}
	return b
}

func (b *AddressBook) Set(id, advertise string) {
	if b == nil || id == "" || advertise == "" {
		return
	}
	b.mu.Lock()
	b.addrs[id] = advertise
	b.mu.Unlock()
}

func (b *AddressBook) Snapshot() map[string]string {
	out := map[string]string{}
	if b == nil {
		return out
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	for id, addr := range b.addrs {
		out[id] = addr
	}
	return out
}

func (b *AddressBook) Resolve(id, _ string) (string, bool) {
	if b == nil || id == "" {
		return "", false
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	addr, ok := b.addrs[id]
	return addr, ok
}
