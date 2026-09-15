package types

import (
	"errors"
	"fmt"
	"math"
)

// BuildResources is the immutable resource demand of one Build. CPU is stored
// in milli-CPU and memory/storage in bytes. It supplies admission/accounting
// and A/B execution sandbox resources. Target Sandbox resources are independent.
type BuildResources struct {
	CPU     int64 `json:"cpu" yaml:"cpu"`
	Memory  int64 `json:"memory" yaml:"memory"`
	Storage int64 `json:"storage,omitempty" yaml:"storage,omitempty"`
}

// BuildAdmissionLimit is a resolved aggregate limit. Zero means that dimension
// is not limited; input validation ensures configured values can never become
// zero accidentally.
type BuildAdmissionLimit struct {
	MaxBuilds int64          `json:"max_builds,omitempty"`
	Resources BuildResources `json:"resources,omitempty"`
}

func (l BuildAdmissionLimit) AllowsOne(resources BuildResources) bool {
	return withinLimit(0, 1, l.MaxBuilds) &&
		withinLimit(0, resources.CPU, l.Resources.CPU) &&
		withinLimit(0, resources.Memory, l.Resources.Memory) &&
		withinLimit(0, resources.Storage, l.Resources.Storage)
}

// AllowsAdd checks all configured dimensions without overflowing the sum.
func (l BuildAdmissionLimit) AllowsAdd(usedBuilds int64, used, add BuildResources) bool {
	return withinLimit(usedBuilds, 1, l.MaxBuilds) &&
		withinLimit(used.CPU, add.CPU, l.Resources.CPU) &&
		withinLimit(used.Memory, add.Memory, l.Resources.Memory) &&
		withinLimit(used.Storage, add.Storage, l.Resources.Storage)
}

func withinLimit(used, add, limit int64) bool {
	if used < 0 || add < 0 || used > math.MaxInt64-add {
		return false
	}
	return limit == 0 || used+add <= limit
}

func (r BuildResources) ValidateRequired() error {
	if r.CPU <= 0 {
		return errors.New("build.resources.cpu must be > 0")
	}
	if r.Memory <= 0 {
		return errors.New("build.resources.memory must be > 0")
	}
	if r.Storage < 0 {
		return errors.New("build.resources.storage must be >= 0")
	}
	return nil
}

// Add returns the component-wise sum and fails closed on signed overflow.
func (r BuildResources) Add(other BuildResources) (BuildResources, error) {
	cpu, err := checkedAddInt64(r.CPU, other.CPU)
	if err != nil {
		return BuildResources{}, fmt.Errorf("build resources CPU: %w", err)
	}
	memory, err := checkedAddInt64(r.Memory, other.Memory)
	if err != nil {
		return BuildResources{}, fmt.Errorf("build resources memory: %w", err)
	}
	storage, err := checkedAddInt64(r.Storage, other.Storage)
	if err != nil {
		return BuildResources{}, fmt.Errorf("build resources storage: %w", err)
	}
	return BuildResources{CPU: cpu, Memory: memory, Storage: storage}, nil
}

// Sub returns the component-wise difference and fails rather than underflowing.
func (r BuildResources) Sub(other BuildResources) (BuildResources, error) {
	if other.CPU < 0 || other.Memory < 0 || other.Storage < 0 ||
		r.CPU < other.CPU || r.Memory < other.Memory || r.Storage < other.Storage {
		return BuildResources{}, errors.New("build resources subtraction underflow")
	}
	return BuildResources{
		CPU: r.CPU - other.CPU, Memory: r.Memory - other.Memory, Storage: r.Storage - other.Storage,
	}, nil
}

func checkedAddInt64(a, b int64) (int64, error) {
	if a < 0 || b < 0 {
		return 0, errors.New("negative value")
	}
	if a > math.MaxInt64-b {
		return 0, errors.New("overflow")
	}
	return a + b, nil
}
