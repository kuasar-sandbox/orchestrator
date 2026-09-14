package main

import (
	"fmt"

	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"gopkg.in/yaml.v3"
)

func renderTelemetryConfig(template bool, path string) ([]byte, error) {
	if template {
		return []byte(telemetryConfigSkeleton), nil
	}
	if path == "" {
		return nil, fmt.Errorf("config telemetry: --config <file> or --template required")
	}
	cfg, err := config.LoadTelemetry(path)
	if err != nil {
		return nil, err
	}
	if cfg.Paths.TelemetryExecutable != "" {
		executables, err := configresolve.CurrentExecutables()
		if err != nil {
			return nil, err
		}
		if err := configresolve.ValidateComponentExecutableMetadata(cfg.Paths.TelemetryExecutable, executables.OrchestratorCtl()); err != nil {
			return nil, fmt.Errorf("paths.telemetry_executable: %w", err)
		}
	} else if err := config.ValidateTelemetryFinal(cfg); err != nil {
		return nil, err
	}
	output, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.Paths.TelemetryExecutable != "" {
		output = append([]byte("# node-ctl declarative/bootstrap configuration is valid; runtime owner and final validation are deferred to custom telemetry startup.\n"), output...)
	}
	return output, nil
}

const telemetryConfigSkeleton = `# node-ctl telemetry serve --config <this>
# Independent component: never participates in create/resume readiness barriers.
config_socket: /run/sandbox/node-ctl.socket
api_socket: /run/sandbox/telemetry.sock
# Bind OTLP directly in the sandbox-facing network namespace, as for Proxy/MMDS.
# proxy_netns: sandbox-proxy
# Applies only to the sandbox OTLP listeners, not remote exporter/query clients.
route_capacity: 65536
paths:
  # telemetry_executable: /opt/kuasar/bin/custom-telemetry
telemetry:
  storage:
    type: local
    path: /var/lib/sandbox/telemetry
    retention: 168h
    max_size: 10GiB
    max_series: 1000000
collector:
  receivers:
    envd:
      collection_interval: 5s
      timeout: 1s
      concurrency: 64
    sandboxstats:
      resource_interval: 5s
      traffic_interval: 10s
      usage_interval: 1m
    sandboxotlp:
      grpc_listen: ":4317"
      http_listen: ":4318"
      max_connections: 256
      max_requests: 32
      max_request_bytes: 4194304
  processors:
    batch:
      timeout: 1s
      send_batch_size: 8192
      send_batch_max_size: 8192
  exporters:
    sandboxstorage: {}
    # otlp_http/observability:
    #   endpoint: https://collector.example.com
  service:
    extensions: []
    telemetry:
      metrics: {level: none}
    pipelines:
      metrics:
        receivers: [envd, sandboxstats, sandboxotlp]
        processors: [batch]
        exporters: [sandboxstorage]
`
