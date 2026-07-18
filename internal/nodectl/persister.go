package nodectl

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Persister snapshots State to a single JSON file. /run is tmpfs by
// convention so the file disappears on host reboot — exactly the
// desired semantics (host reboot kills all sandboxes).
//
// Writes are atomic: write tmp + fsync + rename. Reads tolerate missing
// or corrupt files (caller falls back to cgroup-as-truth recovery,
// §11.2).
type Persister struct {
	Path        string
	syncParent  func(*os.File) error
	closeParent func(*os.File) error
}

type PublishedFlushError struct{ Err error }

func (e *PublishedFlushError) Error() string {
	return fmt.Sprintf("persister: published state: %v", e.Err)
}
func (e *PublishedFlushError) Unwrap() error { return e.Err }

func FlushPublished(err error) bool {
	var published *PublishedFlushError
	return errors.As(err, &published)
}

// Flush writes state to Path atomically. Caller must hold state lock
// for a consistent snapshot.
func (p *Persister) Flush(s *State) error {
	if p.Path == "" {
		return nil
	}
	parent := filepath.Dir(p.Path)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return fmt.Errorf("persister: mkdir: %w", err)
	}
	dir, err := os.Open(parent)
	if err != nil {
		return fmt.Errorf("persister: open parent: %w", err)
	}
	closeParent := p.closeParent
	if closeParent == nil {
		closeParent = func(file *os.File) error { return file.Close() }
	}
	closeBeforePublish := func() { _ = closeParent(dir) }
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		closeBeforePublish()
		return fmt.Errorf("persister: marshal: %w", err)
	}
	tmp := p.Path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		closeBeforePublish()
		return fmt.Errorf("persister: open tmp: %w", err)
	}
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		os.Remove(tmp)
		closeBeforePublish()
		return fmt.Errorf("persister: chmod tmp: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		closeBeforePublish()
		return fmt.Errorf("persister: write: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		closeBeforePublish()
		return fmt.Errorf("persister: fsync: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		closeBeforePublish()
		return fmt.Errorf("persister: close: %w", err)
	}
	if err := os.Rename(tmp, p.Path); err != nil {
		os.Remove(tmp)
		closeBeforePublish()
		return fmt.Errorf("persister: rename: %w", err)
	}
	syncParent := p.syncParent
	if syncParent == nil {
		syncParent = func(file *os.File) error { return file.Sync() }
	}
	if err := syncParent(dir); err != nil {
		_ = closeParent(dir)
		return &PublishedFlushError{Err: fmt.Errorf("fsync parent: %w", err)}
	}
	if err := closeParent(dir); err != nil {
		return &PublishedFlushError{Err: fmt.Errorf("close parent: %w", err)}
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
	var s State
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("persister: unmarshal: %w", err)
	}
	if s.Reservations == nil {
		s.Reservations = make(map[string]*Reservation)
	}
	if s.PreparedSandboxAdmissions == nil {
		s.PreparedSandboxAdmissions = make(map[string]*PreparedSandboxAdmission)
	}
	return &s, nil
}
