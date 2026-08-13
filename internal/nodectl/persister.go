package nodectl

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Persister snapshots State to a single JSON file. /run is tmpfs by
// convention so the file disappears on host reboot — exactly the
// desired semantics (host reboot kills all sandboxes).
//
// Writes are atomic: write tmp + fsync + rename. Reads tolerate missing
// or corrupt files (caller falls back to cgroup-as-truth recovery,
// §11.2).
type Persister struct {
	Path string
	mu   sync.Mutex
}

type persistedState struct {
	Version           int                     `json:"version"`
	NodeBudget        Resources               `json:"node_budget"`
	HostReserved      Resources               `json:"host_reserved"`
	OperationalMargin Resources               `json:"operational_margin"`
	AllocatablePool   Resources               `json:"allocatable_pool"`
	Wm                Watermarks              `json:"watermarks"`
	Reservations      map[string]*Reservation `json:"reservations"`
}

// Flush writes state to Path atomically. Caller must hold state lock
// for a consistent snapshot.
func (p *Persister) Flush(s *State) error {
	if p.Path == "" {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(p.Path), 0o755); err != nil {
		return fmt.Errorf("persister: mkdir: %w", err)
	}
	snapshot := persistedState{
		Version: 1, NodeBudget: s.NodeBudget, HostReserved: s.HostReserved,
		OperationalMargin: s.OperationalMargin, AllocatablePool: s.AllocatablePool,
		Wm: s.Wm, Reservations: s.PersistenceReservations(),
	}
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("persister: marshal: %w", err)
	}
	tmp := p.Path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("persister: open tmp: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("persister: write: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("persister: fsync: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("persister: close: %w", err)
	}
	if err := os.Rename(tmp, p.Path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("persister: rename: %w", err)
	}
	return nil
}

// Load reads the last persisted state. Returns (nil, nil) if the file
// does not exist (cold start), an error for any other failure.
func (p *Persister) Load() (*State, error) {
	if p.Path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(p.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("persister: read: %w", err)
	}
	var persisted persistedState
	if err := json.Unmarshal(data, &persisted); err != nil {
		return nil, fmt.Errorf("persister: unmarshal: %w", err)
	}
	s := &State{
		NodeBudget: persisted.NodeBudget, HostReserved: persisted.HostReserved,
		OperationalMargin: persisted.OperationalMargin, AllocatablePool: persisted.AllocatablePool,
		Wm: persisted.Wm, bySID: make(map[string]*Reservation),
		tokenToSID: make(map[string]string), cgroupToSID: make(map[string]string),
	}
	if err := s.RestoreReservations(persisted.Reservations); err != nil {
		return nil, fmt.Errorf("persister: rebuild sandbox index: %w", err)
	}
	return s, nil
}
