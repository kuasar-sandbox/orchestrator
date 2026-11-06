package nodectl

import (
	"fmt"
	"math"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
)

func numericConfig() *config.ResourceListenConfig {
	return &config.ResourceListenConfig{Resources: config.ResourceHostConfig{PhysicalMemory: "32GiB", PhysicalCPU: "8", HostReserved: config.ResourceHostReserved{Memory: "1GiB", CPU: 1.5}}}
}

func TestResolveAdmissionRequiresPositiveEffectiveValues(t *testing.T) {
	for _, field := range []string{"rate", "burst"} {
		c := numericConfig()
		if field == "rate" {
			c.Admission.Rate = -1
		} else {
			c.Admission.Burst = -1
		}
		if _, err := Resolve(c); err == nil || !strings.Contains(err.Error(), "resource_listen.admission."+field) {
			t.Fatalf("%s error=%v", field, err)
		}
	}
	for _, explicit := range []bool{false, true} {
		c := numericConfig()
		if explicit {
			c.Admission.Rate, c.Admission.Burst = 0, 0
		}
		r, err := Resolve(c)
		if err != nil || r.Admission.Rate != 4 || r.Admission.Burst != 16 {
			t.Fatalf("defaults=%+v %v", r, err)
		}
		a := NewAdmissionController(r.Admission)
		for i := 0; i < r.Admission.Burst; i++ {
			if !a.ConsumeToken() {
				t.Fatal("valid burst exhausted early")
			}
		}
		if a.ConsumeToken() {
			t.Fatal("burst overdraw")
		}
		a.lastRefill = time.Now().Add(-time.Second)
		if !a.ConsumeToken() {
			t.Fatal("valid rate did not refill")
		}
	}
}

func TestResolveCPURejectsMalformedAndOverflowingValues(t *testing.T) {
	for _, value := range []string{"1.5", "8junk", "-1", "8 ", "1e2", strconv.FormatUint(math.MaxUint64/1000+1, 10), "18446744073709551616"} {
		t.Run(value, func(t *testing.T) {
			c := numericConfig()
			c.Resources.PhysicalCPU = value
			if _, err := Resolve(c); err == nil || !strings.Contains(err.Error(), "resource_listen.resources.physical_cpu") {
				t.Fatalf("physical_cpu=%s: %v", value, err)
			}
		})
	}
	for _, value := range []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1), math.Ldexp(1, 64) / 1000, math.MaxFloat64} {
		t.Run(fmt.Sprint(value), func(t *testing.T) {
			c := numericConfig()
			c.Resources.HostReserved.CPU = value
			if _, err := Resolve(c); err == nil || !strings.Contains(err.Error(), "resource_listen.resources.host_reserved.cpu") {
				t.Fatalf("host CPU=%g: %v", value, err)
			}
		})
	}
}

func TestResolveCPUValidValuesKeepLegacyRoundingAndPoolFractions(t *testing.T) {
	for _, value := range []string{"auto", "8", "0", strconv.FormatUint(math.MaxUint64/1000, 10)} {
		for _, reserve := range []float64{0, 1.5, 0.0009, 8, math.Nextafter(math.Ldexp(1, 64)/1000, 0)} {
			c := numericConfig()
			c.Resources.PhysicalCPU, c.Resources.HostReserved.CPU = value, reserve
			r, err := Resolve(c)
			if err != nil {
				t.Fatal(err)
			}
			cores, _ := strconv.ParseUint(value, 10, 64)
			if value == "auto" {
				cores = uint64(runtime.NumCPU())
			}
			if reserve == 0 {
				reserve = 1.5
			}
			if r.PhysicalCPU != cores*1000 || r.HostReserved.CPUMilli != uint64(reserve*1000) {
				t.Fatalf("CPU conversion=%+v", r)
			}
			state := NewPoolState(r)
			if state.NodeBudget.CPUMilli != r.PhysicalCPU || state.HostReserved.CPUMilli != r.HostReserved.CPUMilli {
				t.Fatal("resolved CPU disagrees with resource state")
			}
		}
	}
	memory, cpu := "1GiB", 0.1255
	c := &config.ResourceListenConfig{Pools: []config.ResourcePoolConfig{{Name: "fractional", Allocatable: config.ResourceAllocatable{Memory: &memory, CPU: &cpu}, Capacity: config.ResourceAllocatable{CPU: &cpu}}}}
	r, err := Resolve(c)
	if err != nil || r.Pools[0].Capacity.CPUMilli != 126 || r.Pools[0].Allocatable.CPUMilli != 126 {
		t.Fatalf("fractional pool CPU=%+v %v", r, err)
	}
}

func TestResolveRejectsRoundedZeroGrowRatePerPool(t *testing.T) {
	c := numericConfig()
	c.RateLimits.MemoryGrantPerSecFactor = 1e-12
	if _, err := Resolve(c); err == nil || !strings.Contains(err.Error(), "resource_listen.rate_limits.memory_grant_per_sec_factor") {
		t.Fatalf("legacy zero rate=%v", err)
	}
	large, small := "2GiB", "1GiB"
	c = &config.ResourceListenConfig{Pools: []config.ResourcePoolConfig{{Name: "large", Allocatable: config.ResourceAllocatable{Memory: &large}}, {Name: "small", Allocatable: config.ResourceAllocatable{Memory: &small}}}}
	c.RateLimits.MemoryGrantPerSecFactor = 1.0 / (2 << 30) // first pool 1 byte/s, second rounds to zero
	if _, err := Resolve(c); err == nil || !strings.Contains(err.Error(), "pools[1] (small)") {
		t.Fatalf("second pool zero rate=%v", err)
	}
}
