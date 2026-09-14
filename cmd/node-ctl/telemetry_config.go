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

const telemetryConfigSkeleton = `# node-ctl telemetry serve --config /etc/node-ctl/telemetry.yaml
# Separate from conductor.yaml/proxy.yaml; never a create/resume barrier.
config_socket: /run/sandbox/node-ctl.socket
api_socket: /run/sandbox/telemetry.sock
# Use the namespace carrying the existing connector management path.
# proxy_netns: sandbox-proxy
# For loopback listeners in the existing connector management namespace, add
# these to the actual vswitch start command, retaining its MMDS mapping:
#   --mgmt-extract=sandbox-proxy:sw0m0:169.254.169.254/32
#   --mgmt-service=169.254.169.254:4317:127.0.0.1:4317
#   --mgmt-service=169.254.169.254:4318:127.0.0.1:4318
# Enable route_localnet on that management device and bind the OTLP addresses
# below to 127.0.0.1. Empty proxy_netns uses the current process namespace.
# This never changes the network used by remote exporters/query clients or UDS.
route_capacity: 65536
paths:
  # telemetry_executable: /opt/kuasar/bin/custom-telemetry
query: {backend: local, handler: e2b}
local:
  enabled: true
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
    sandboxlocal: {}
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
        exporters: [sandboxlocal]
# See docs/telemetry.md for Collector pipelines and query backends.
`
