// Package launcher abstracts how sandbox/build runner units are supervised. The
// primary implementation drives systemd template units keyed by run-id over
// D-Bus; a fork-exec fallback can implement the same interface for non-systemd hosts.
package launcher

import (
	"context"
	"fmt"

	"github.com/coreos/go-systemd/v22/dbus"
	godbus "github.com/godbus/dbus/v5"
)

// Unit is a minimal view of a systemd unit's liveness.
type Unit struct {
	Name        string
	ActiveState string // active | activating | failed | inactive | ...
	SubState    string
}

type ResourceProperties struct {
	CPUQuotaPerSecUSec uint64
	MemoryMax          uint64
}

// Launcher supervises sandbox-ctl instances.
type Launcher interface {
	// Start launches the template instance ("replace" job mode) and waits
	// for the start job to settle.
	Start(ctx context.Context, unit string) error
	// Stop stops the unit (SIGTERM then SIGKILL after TimeoutStopSec via the unit).
	Stop(ctx context.Context, unit string) error
	// ResetFailed clears a failed unit so its name is reusable.
	ResetFailed(ctx context.Context, unit string) error
	// List returns units matching the glob pattern (the liveness authority).
	List(ctx context.Context, pattern string) ([]Unit, error)
	// Reload re-reads unit files after node-ctl installs/updates them.
	Reload(ctx context.Context) error
	// SetResources applies runtime-only cgroup limits to an already-started
	// preassigned service or slice.
	SetResources(ctx context.Context, unit string, properties ResourceProperties) error
	// Resources reads the effective service/slice values used to gate assignment.
	Resources(ctx context.Context, unit, unitType string) (ResourceProperties, error)
	Close() error
}

// Systemd is the systemd-D-Bus Launcher.
type Systemd struct{ conn *dbus.Conn }

// NewSystemd connects to the system bus' systemd manager.
func NewSystemd(ctx context.Context) (*Systemd, error) {
	conn, err := dbus.NewSystemdConnectionContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("launcher: connect systemd: %w", err)
	}
	return &Systemd{conn: conn}, nil
}

func (s *Systemd) Close() error {
	s.conn.Close()
	return nil
}

func (s *Systemd) Start(ctx context.Context, unit string) error {
	ch := make(chan string, 1)
	if _, err := s.conn.StartUnitContext(ctx, unit, "replace", ch); err != nil {
		return fmt.Errorf("launcher: start %s: %w", unit, err)
	}
	return waitJob(ctx, unit, "start", ch)
}

func (s *Systemd) Stop(ctx context.Context, unit string) error {
	ch := make(chan string, 1)
	if _, err := s.conn.StopUnitContext(ctx, unit, "replace", ch); err != nil {
		return fmt.Errorf("launcher: stop %s: %w", unit, err)
	}
	return waitJob(ctx, unit, "stop", ch)
}

func (s *Systemd) ResetFailed(ctx context.Context, unit string) error {
	// Best-effort: a non-failed unit returns an error we ignore.
	_ = s.conn.ResetFailedUnitContext(ctx, unit)
	return nil
}

func (s *Systemd) Reload(ctx context.Context) error {
	if err := s.conn.ReloadContext(ctx); err != nil {
		return fmt.Errorf("launcher: daemon-reload: %w", err)
	}
	return nil
}

func (s *Systemd) SetResources(ctx context.Context, unit string, properties ResourceProperties) error {
	var values []dbus.Property
	if properties.CPUQuotaPerSecUSec != 0 {
		values = append(values, dbus.Property{Name: "CPUQuotaPerSecUSec", Value: godbus.MakeVariant(properties.CPUQuotaPerSecUSec)})
	}
	if properties.MemoryMax != 0 {
		values = append(values, dbus.Property{Name: "MemoryMax", Value: godbus.MakeVariant(properties.MemoryMax)})
	}
	if len(values) == 0 {
		return nil
	}
	if err := s.conn.SetUnitPropertiesContext(ctx, unit, true, values...); err != nil {
		return fmt.Errorf("launcher: set resources for %s: %w", unit, err)
	}
	return nil
}

func (s *Systemd) Resources(ctx context.Context, unit, unitType string) (ResourceProperties, error) {
	properties, err := s.conn.GetUnitTypePropertiesContext(ctx, unit, unitType)
	if err != nil {
		return ResourceProperties{}, fmt.Errorf("launcher: read resources for %s: %w", unit, err)
	}
	readUint64 := func(name string) (uint64, error) {
		value, ok := properties[name]
		if !ok {
			return 0, fmt.Errorf("launcher: %s has no %s property", unit, name)
		}
		parsed, ok := value.(uint64)
		if !ok {
			return 0, fmt.Errorf("launcher: %s property %s has type %T", unit, name, value)
		}
		return parsed, nil
	}
	cpu, err := readUint64("CPUQuotaPerSecUSec")
	if err != nil {
		return ResourceProperties{}, err
	}
	memory, err := readUint64("MemoryMax")
	if err != nil {
		return ResourceProperties{}, err
	}
	return ResourceProperties{CPUQuotaPerSecUSec: cpu, MemoryMax: memory}, nil
}

func (s *Systemd) List(ctx context.Context, pattern string) ([]Unit, error) {
	states := []string{"active", "activating", "reloading", "failed", "deactivating"}
	us, err := s.conn.ListUnitsByPatternsContext(ctx, states, []string{pattern})
	if err != nil {
		return nil, fmt.Errorf("launcher: list %s: %w", pattern, err)
	}
	out := make([]Unit, 0, len(us))
	for _, u := range us {
		out = append(out, Unit{Name: u.Name, ActiveState: u.ActiveState, SubState: u.SubState})
	}
	return out, nil
}

func waitJob(ctx context.Context, unit, op string, ch <-chan string) error {
	select {
	case res := <-ch:
		if res != "done" {
			return fmt.Errorf("launcher: %s %s: job %s", op, unit, res)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
