package orch

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type buildAdmissionObservability struct {
	mu                  sync.Mutex
	registrationRejects map[string]int64
	executionRejects    map[string]int64
	executionWouldWait  atomic.Int64
	registrationExpired atomic.Int64
	queueExpired        atomic.Int64
}

// SetMetrics connects Builder admission gauges/counters to the conductor's
// existing Prometheus registry. Durable SQLite rows remain the authority.
func (o *Orchestrator) SetMetrics(mx *metrics.M) {
	o.mx = mx
	if usage, err := o.st.BuildUsage(context.Background()); err == nil {
		o.setBuildAdmissionGauges(usage)
	}
}

func (o *Orchestrator) BuilderAdmissionStatus(ctx context.Context) (configsock.BuilderAdmissionStatus, error) {
	registration, err := configresolve.BuilderRegistrationLimit(o.cfg.Builder)
	if err != nil {
		return configsock.BuilderAdmissionStatus{}, err
	}
	execution, err := configresolve.BuilderExecutionLimit(o.cfg.Builder)
	if err != nil {
		return configsock.BuilderAdmissionStatus{}, err
	}
	usage, builds, err := o.st.BuildUsageDetails(ctx)
	if err != nil {
		return configsock.BuilderAdmissionStatus{}, err
	}
	o.setBuildAdmissionGauges(usage)
	o.buildAdmission.mu.Lock()
	rejections := make(map[string]int64, len(o.buildAdmission.registrationRejects))
	for reason, count := range o.buildAdmission.registrationRejects {
		rejections[reason] = count
	}
	executionRejections := make(map[string]int64, len(o.buildAdmission.executionRejects))
	for reason, count := range o.buildAdmission.executionRejects {
		executionRejections[reason] = count
	}
	o.buildAdmission.mu.Unlock()
	oldestAge := int64(0)
	if usage.OldestWaitingUnix > 0 {
		oldestAge = time.Now().Unix() - usage.OldestWaitingUnix
		if oldestAge < 0 {
			oldestAge = 0
		}
	}
	entries := make([]configsock.BuilderBuildStatus, 0, len(builds))
	for _, b := range builds {
		entries = append(entries, configsock.BuilderBuildStatus{BuildID: b.BuildID, TemplateID: b.TemplateID, Status: b.Status, Resources: b.Resources, RunID: b.RunID, WaitingUnix: b.WaitingUnix, ExecutionClaimed: b.ExecutionClaimed, ExecutionClaimedUnix: b.ExecutionClaimedUnix, CancelRequested: b.CancelRequestedUnix != 0, DeleteRequested: b.DeleteRequestedUnix != 0})
	}
	return configsock.BuilderAdmissionStatus{
		Builds:        entries,
		Registration:  admissionLevelStatus(registration, usage.RegistrationBuilds, usage.Registration),
		Execution:     admissionLevelStatus(execution, usage.ExecutionBuilds, usage.Execution),
		WaitingBuilds: usage.WaitingBuilds, OldestWaitAgeSec: oldestAge,
		RegistrationReject:  rejections,
		ExecutionReject:     executionRejections,
		ExecutionWouldWait:  o.buildAdmission.executionWouldWait.Load(),
		RegistrationExpired: o.buildAdmission.registrationExpired.Load(),
		QueueExpired:        o.buildAdmission.queueExpired.Load(),
	}, nil
}

func admissionLevelStatus(limit types.BuildAdmissionLimit, usedBuilds int64, used types.BuildResources) configsock.BuildAdmissionLevelStatus {
	return configsock.BuildAdmissionLevelStatus{
		Configured: limit, UsedBuilds: usedBuilds, Used: used,
		Available: configsock.BuildAdmissionHeadroom{
			MaxBuilds: remaining(limit.MaxBuilds, usedBuilds),
			CPU:       remaining(limit.Resources.CPU, used.CPU),
			Memory:    remaining(limit.Resources.Memory, used.Memory),
			Storage:   remaining(limit.Resources.Storage, used.Storage),
		},
	}
}

func remaining(limit, used int64) *int64 {
	if limit == 0 {
		return nil
	}
	value := limit - used
	if value < 0 {
		value = 0
	}
	return &value
}

func (o *Orchestrator) recordRegistrationRejection(reason string) {
	o.buildAdmission.mu.Lock()
	if o.buildAdmission.registrationRejects == nil {
		o.buildAdmission.registrationRejects = make(map[string]int64)
	}
	if o.buildAdmission.registrationRejects[reason] < math.MaxInt64 {
		o.buildAdmission.registrationRejects[reason]++
	}
	o.buildAdmission.mu.Unlock()
	o.mx.Inc(`builder_registration_rejections_total{reason="` + reason + `"}`)
}

func (o *Orchestrator) recordExecutionWouldWait(ctx context.Context) {
	o.buildAdmission.executionWouldWait.Add(1)
	o.mx.Inc("builder_execution_would_wait_total")
	o.refreshBuildAdmissionGauges(ctx)
}

func (o *Orchestrator) recordExecutionRejection(reason string) {
	o.buildAdmission.mu.Lock()
	if o.buildAdmission.executionRejects == nil {
		o.buildAdmission.executionRejects = make(map[string]int64)
	}
	if o.buildAdmission.executionRejects[reason] < math.MaxInt64 {
		o.buildAdmission.executionRejects[reason]++
	}
	o.buildAdmission.mu.Unlock()
	o.mx.Inc(`builder_execution_rejections_total{reason="` + reason + `"}`)
}

func (o *Orchestrator) recordBuildExpired(state types.BuildState) {
	switch state {
	case types.BuildRegistered:
		o.buildAdmission.registrationExpired.Add(1)
		o.mx.Inc("builder_registration_expired_total")
	case types.BuildWaiting:
		o.buildAdmission.queueExpired.Add(1)
		o.mx.Inc("builder_queue_expired_total")
	}
}

func (o *Orchestrator) refreshBuildAdmissionGauges(ctx context.Context) {
	usage, err := o.st.BuildUsage(ctx)
	if err != nil {
		o.log.Warn("builder admission metrics", "err", err)
		return
	}
	o.setBuildAdmissionGauges(usage)
}

func (o *Orchestrator) setBuildAdmissionGauges(usage store.BuildAdmissionUsage) {
	registration, registrationErr := configresolve.BuilderRegistrationLimit(o.cfg.Builder)
	execution, executionErr := configresolve.BuilderExecutionLimit(o.cfg.Builder)
	if registrationErr != nil || executionErr != nil {
		return
	}
	setLevelGauges(o.mx, "registration", registration, usage.RegistrationBuilds, usage.Registration)
	setLevelGauges(o.mx, "execution", execution, usage.ExecutionBuilds, usage.Execution)
	o.mx.Set("builder_execution_waiting_builds", usage.WaitingBuilds)
	oldestAge := int64(0)
	if usage.OldestWaitingUnix > 0 {
		oldestAge = time.Now().Unix() - usage.OldestWaitingUnix
		if oldestAge < 0 {
			oldestAge = 0
		}
	}
	o.mx.Set("builder_execution_oldest_wait_age_seconds", oldestAge)
}

func setLevelGauges(mx *metrics.M, level string, limit types.BuildAdmissionLimit, usedBuilds int64, used types.BuildResources) {
	prefix := "builder_" + level + "_"
	mx.Set(prefix+"used_builds", usedBuilds)
	mx.Set(prefix+"used_cpu_milli", used.CPU)
	mx.Set(prefix+"used_memory_bytes", used.Memory)
	mx.Set(prefix+"used_storage_bytes", used.Storage)
	for _, value := range []struct {
		name  string
		limit int64
		used  int64
	}{
		{"limit_builds", limit.MaxBuilds, usedBuilds},
		{"limit_cpu_milli", limit.Resources.CPU, used.CPU},
		{"limit_memory_bytes", limit.Resources.Memory, used.Memory},
		{"limit_storage_bytes", limit.Resources.Storage, used.Storage},
	} {
		if value.limit == 0 {
			continue
		}
		mx.Set(prefix+value.name, value.limit)
		available := value.limit - value.used
		if available < 0 {
			available = 0
		}
		mx.Set(prefix+"available_"+value.name[len("limit_"):], available)
	}
}
