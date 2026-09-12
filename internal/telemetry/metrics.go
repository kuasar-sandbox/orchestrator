package telemetry

import extension "github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"

const (
	SandboxIDAttribute = "sandbox.id"
	StableIDAttribute  = "sandbox.stable_id"
)

var resourceMetrics = [...]struct {
	name string
	unit string
}{
	{"sandbox.cpu.count", "{cpu}"},
	{"sandbox.cpu.used", "%"},
	{"sandbox.memory.total", "By"},
	{"sandbox.memory.used", "By"},
	{"sandbox.memory.cache", "By"},
	{"sandbox.disk.total", "By"},
	{"sandbox.disk.used", "By"},
}

func metricField(name string) (extension.Field, bool) {
	for i, metric := range resourceMetrics {
		if name == metric.name {
			return extension.Field(i), true
		}
	}
	return 0, false
}
