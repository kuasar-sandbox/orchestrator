package main

import (
	"bytes"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
	"gopkg.in/yaml.v3"
)

// configCmd implements `node-ctl config <role>` — a per-role config diagnose +
// generate tool (role ∈ {conductor, proxy}; cluster-ctl has its own registry/router/
// placer). The role disambiguates the schema, so the skeleton + validation are
// role-specific:
//
//	node-ctl config conductor --template                 # commented skeleton
//	node-ctl config proxy --config proxy.yaml            # normalize + validate, re-emit
//	node-ctl config conductor --config conductor.yaml --resolve
//	  [-o <file>]                                        # write to file (default stdout)
func configCmd(args []string, _ *slog.Logger) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: node-ctl config <conductor|proxy> [--template | --config <f> [--resolve]] [-o <f>]")
	}
	role := args[0]
	fs := flag.NewFlagSet("config "+role, flag.ExitOnError)
	cfgPath := fs.String("config", "", "input config YAML to normalize + validate")
	template := fs.Bool("template", false, "emit a commented skeleton for the role")
	resolve := fs.Bool("resolve", false, "expand auto/derived values to the effective form")
	out := fs.String("o", "", "write output to this file instead of stdout")
	_ = fs.Parse(args[1:])

	var (
		output []byte
		err    error
	)
	switch role {
	case "conductor":
		output, err = renderConductorConfig(*template, *resolve, *cfgPath)
	case "proxy":
		output, err = renderProxyConfig(*template, *cfgPath)
	default:
		return fmt.Errorf("config: unknown role %q (conductor|proxy)", role)
	}
	if err != nil {
		return err
	}
	if bytes.HasPrefix(output, []byte("# node-ctl bootstrap configuration is valid;")) {
		_, _ = fmt.Fprintln(os.Stderr, "node-ctl config: bootstrap configuration is valid; final validation is performed by the custom component")
	}
	if *out != "" {
		return os.WriteFile(*out, output, 0o644)
	}
	_, err = os.Stdout.Write(output)
	return err
}

// renderConductorConfig handles `config conductor`. --resolve additionally expands the
// resource controller's "auto" memory/cpu (and validates its watermarks) so the
// operator sees the effective numbers.
func renderConductorConfig(template, resolve bool, path string) ([]byte, error) {
	if template {
		return []byte(conductorConfigSkeleton), nil
	}
	if path == "" {
		return nil, fmt.Errorf("config conductor: --config <file> or --template required")
	}
	cfg, err := config.LoadConductor(path) // applies defaults + bootstrap validation
	if err != nil {
		return nil, err
	}
	executables, err := configresolve.CurrentExecutables()
	if err != nil {
		return nil, err
	}
	if err := configresolve.ValidateComponentExecutable(cfg.Paths.ConductorExecutable, executables.OrchestratorCtl()); err != nil {
		return nil, fmt.Errorf("paths.conductor_executable: %w", err)
	}
	cfg.Sandbox.Resources = configresolve.MaterializedSandboxResources(cfg.Sandbox.Resources)
	if resolve && cfg.ResourceListen != nil && cfg.ResourceListen.Enabled {
		r, rerr := nodectl.Resolve(cfg.ResourceListen)
		if rerr != nil {
			return nil, fmt.Errorf("resource_listen: %w", rerr)
		}
		cfg.ResourceListen.Socket = r.Listen
		cfg.ResourceListen.Resources.PhysicalMemory = strconv.FormatUint(r.PhysicalMemory, 10)
		cfg.ResourceListen.Resources.PhysicalCPU = strconv.Itoa(int(r.PhysicalCPU / 1000))
	}
	output, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.Paths.ConductorExecutable != "" {
		output = append([]byte("# node-ctl bootstrap configuration is valid; the custom conductor App performs final validation.\n"), output...)
	}
	return output, nil
}

// renderProxyConfig handles `config proxy`. The worker config has no auto/derived
// values, so --resolve is a no-op over the defaulted form.
func renderProxyConfig(template bool, path string) ([]byte, error) {
	if template {
		return []byte(proxyConfigSkeleton), nil
	}
	if path == "" {
		return nil, fmt.Errorf("config proxy: --config <file> or --template required")
	}
	cfg, err := config.LoadProxy(path)
	if err != nil {
		return nil, err
	}
	executables, err := configresolve.CurrentExecutables()
	if err != nil {
		return nil, err
	}
	if err := configresolve.ValidateComponentExecutable(cfg.Paths.ProxyExecutable, executables.OrchestratorCtl()); err != nil {
		return nil, fmt.Errorf("paths.proxy_executable: %w", err)
	}
	output, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.Paths.ProxyExecutable != "" {
		output = append([]byte("# node-ctl bootstrap configuration is valid; the custom proxy App performs final validation.\n"), output...)
	}
	return output, nil
}

// conductorConfigSkeleton is the commented authoring template for conductor.yaml
// (deploy/conductor.example.yaml is the curated copy). Config is grouped by concern;
// external binaries (sandbox-ctl, connector-ctl, flatten-ctl, ...) are auto-discovered
// next to node-ctl then on PATH.
const conductorConfigSkeleton = `# node-ctl conductor serve config — node-ctl conductor serve --config <this>.
# The unmodified e2b SDK reaches this node via E2B_DOMAIN/E2B_API_KEY (dev:
# E2B_API_URL/E2B_SANDBOX_URL http). Required: api.domain + encryption_key.
api:
  domain: sandboxes.example.com
  listen: ":443"                                 # dev: ":3000" (plain http/h2c)
  tls: { cert: /etc/node-ctl/tls/fullchain.pem, key: /etc/node-ctl/tls/privkey.pem }
proxy:                                           # data-plane policy (<port>-<sid>.<domain>)
  mode: internal                                 # internal | external | off
  auth: enforce                                  # off | log | enforce: validate X-Access-Token
  # data_listen: ":8443"                          # internal-mode dedicated listener; "" = share api.listen.
  #                                               # external mode: data config lives in proxy.yaml; master freezes it for workers.
  # proxy_netns: sw0_mgmt                         # internal-mode forwarding netns for floatingip dials + MMDS listen.
  # park_timeout: 30s
  # metrics_listen: ":9900"                       # serve's own Prometheus text endpoint
# AES-256 keys for tenant credentials at rest (":"-separated, first active). Prefer the
# NODE_CONFIG_ENCRYPTION_KEY env (overrides). Generate: e2b-key-ctl gen-key.
encryption_key: "0000000000000000000000000000000000000000000000000000000000000000"
# Shared remote manifest store (manifest.key empty; the tenant key arrives via env).
manifest_config: /opt/sandbox/manifest.yaml
paths:
  # conductor_executable: /opt/kuasar/bin/xconductor # service-UID-owned app/conductor binary; validated-FD exec + sealed bootstrap
  run_root: /run/sandbox
  base_root: /var/lib/sandbox
  config_socket: /run/sandbox/node-ctl.socket  # local control socket: run + task + manifest-key admin + plugin + api plane (h2c)
  # db_path: /var/lib/sandbox/node-ctl.db    # default = <base_root>/node-ctl.db
  # admin_pidfile: /run/sandbox/node-ctl-admin.pids  # PID allowlist for the admin plane; unset = socket 0600 perms
  # plugin_pidfile: /run/sandbox/node-ctl-plugin.pids # PID allowlist for the plugin plane (proxy/agent registration)
# units:                                          # systemd template units (defaults shown)
#   dir: /etc/systemd/system
#   runner: sandbox-runner@.service
#   builder: sandbox-builder@.service
#   runner_pool_size: 0
#   builder_pool_size: 0 # required when execution CPU or memory is capped
#   pool_wait_timeout: 5s
#   install: true
sandbox:                                          # sandbox-instance defaults
  timeout_sec: 300
  resources:
    capacity: { cpu: 2, memory: 2GiB }            # guest-visible VM capacity / E2B SKU
    allocatable:
      # cpu: 2                                    # omitted => follows final capacity.cpu
      memory: 256MiB                              # settled guest headroom, not a total Budget
    # startup: { memory: 512MiB }                 # cold headroom in static/dynamic mode; omitted => capacity.memory
    overhead: { memory: 32MiB }                   # node-owned host VMM overhead
    watermark_high: { ratio: 0.875 }              # node-owned memory.high pressure ratio
  network:
    switch: sw0
    # tapfd_socket: /run/kuasar/connector/sw0/tapfd.sock # persistent TAPFD/1 PREPARE/OPEN/RELEASE; connector: vswitch serve --tapfd-listen <same path>
    hostname: sandbox                             # guest hostname (sethostname + /etc/hosts)
    dns: [169.254.169.253]                        # /etc/resolv.conf nameserver(s) injected into the guest
    e2b:  { inner_ip: 169.254.0.21/30, nexthop: 169.254.0.22 }   # e2b: /30 + gateway for envd port-forward
    bare: { inner_ip: 169.254.1.1/31,  nexthop: 169.254.1.0 }
  boot:
    kernel: /opt/sandbox/kernel/6.1/vmlinux
    runtime: /opt/sandbox/runtime/v1/sandbox-runtime.bundle
    # Pre-formatted empty ext4 seeding the cold-boot overlay upper (required for img templates).
    overlay_diff_template: /opt/sandbox/overlay-templates/basic-1G.ext4
builder:                                           # Build resources are separate from phase sandbox.resources
  admission:
    registration:                                 # all non-terminal Build definitions; omitted => resolved execution
      max_builds: 16
      resources: { cpu: 64, memory: 256GiB, storage: 1TiB }
    execution:                                    # concurrently executing Build units; omitted => max_builds: 2
      max_builds: 4
      resources: { cpu: 16, memory: 64GiB, storage: 256GiB }
  registration_ttl: 1h                           # registered but never triggered
  queue_ttl: 30m                                  # waiting for execution admission
  diff_template: /opt/sandbox/overlay-templates/builder-8G.ext4  # build VM writable disk (pull cache + export scratch)
  # Build CPU/memory also enforce each builder service; execution aggregate CPU/memory enforce sandbox-builder.slice.
  # storage is admission-only until a filesystem quota backend is configured in a future change.
  # insecure_registry: false                      # pull base over plain HTTP (dev/local registry)
  # platform: linux/amd64
  # pull_timeout_sec: 600                         # in-guest pull+flatten | one RUN step |
  # step_timeout_sec: 600                         # readyCmd poll | whole build
  # ready_timeout_sec: 120
  # total_timeout_sec: 1800
  # image_uri_mask must match the CLI's E2B_IMAGE_URI_MASK ({templateID}/{buildID} tokens)
  # AND be reachable from inside a build sandbox (the pull runs in the guest):
  # image_uri_mask: "docker.sandboxes.example.com/e2b/custom-envs/{templateID}:{buildID}"
  # referer:                                        # optional OCI Referrers import cache
  #   enabled: false                                # lookup before pull+flatten
  #   fallback: true                                # unsupported/unavailable registry continues without writeback
  #   writeback: true                               # after miss+upload, put referrer; failure fails the build
  #   desc: ""                                      # public owner descriptor; required when enabled
  #   key: ""                                       # HMAC message paired with MANIFEST_KEY; empty = desc
  #   validity: ""                                  # optional Go duration, e.g. 720h
  # files_storage: COPY build contexts; client direct-uploads (presigned PUT) to
  # this bucket, build fetches (presigned GET). Unset → COPY rejected (501).
  # Local/single-node: point at versitygw (guest-runtime/native-deps: make versitygw).
  # files_storage:
  #   endpoint: https://obs.cn-north-4.example.com   # versitygw: http://127.0.0.1:7070
  #   region: cn-north-4
  #   bucket: kuasar-build-files                            # required
  #   force_path_style: false                               # versitygw/minio need true
  #   access_key: ""                                        # empty → AWS default chain
  #   secret_key: ""
# resource_listen: optional in-process node resource controller (admission / budget /
# density; socket is the sole endpoint and its canonical identity is injected into
# sandbox YAML + lease/inventory). Absent/disabled = static cgroup. The tuning
# is inlined here (no separate file). Inspect / operate with:
# node-ctl resource {status|list|drain}.
# resource_listen:
#   enabled: true
#   socket: /run/sandbox-resource.sock           # "" = pkg/resource default (sandbox-ctl's default)
#   # Advanced tuning — all defaulted (node-resource.md §3.2); usually left untouched:
#   # state_path: /run/node-ctl/state.json         # deprecated and ignored
#   # audit_path: /run/node-ctl/audit.log
#   # cgroup_scan_paths: [/sys/fs/cgroup/sandbox.slice/sandbox-runner.slice, /sys/fs/cgroup/sandbox.slice/sandbox-builder.slice]
#   # resources: { physical_memory: auto, physical_cpu: auto, host_reserved: { memory: 16GiB, cpu: 1.5 } }
#   # watermarks: { operational_margin_factor: 0.10, high_factor: 0.85, low_factor: 0.70, emergency_factor: 0.05, startup_factor: 0.50 }
#   # rate_limits: { memory_grant_per_sec_factor: 0.05 }
#   # admission: { rate: 4, burst: 16, startup_ttl: 30s, queue_ttl: 30s, queue_max_depth: 256 }
checkpoint:                                        # paused-state capture
  mode: local                                     # local | bundle
  local_dir: /var/lib/sandbox-saved
  # merge_ref: false                              # omit/null => sandbox-ctl default
  # drop_caches: false                            # omit/null => sandbox-ctl default
  # remote:
  #   # Used only by export-sandbox/promote publication; Pause always captures locally.
  #   ref_location_parent: file:///mnt/shared/kuasar/snapshots
# mmds:                                            # optional envd FC-mode token re-keying
#   enabled: false                                # false => envd non-secure; proxy.auth must be enforce
#   listen: 127.0.0.1:19254                        # MMDS listener (vswitch --mgmt-service target)
#   routes:
#     enabled: true
#     max_routes_per_sandbox: 32
#     max_namespace_bytes: 65536
#     max_static_body_bytes: 16384
#     max_secret_value_bytes: 16384
#     reserved_path_prefixes: [/latest/api/, /internal/]
#   services:
#     external-mmds: { endpoint: unix:///run/kuasar/mmds/external-mmds.sock }
# cluster:                                         # connect this node to registry node_link (node.md §10)
#   node_link:                                    # how to reach registry node_link
#     endpoint: registry.cluster.example.com:7700 # "" = standalone single-node
#     # node supports registry owner redirect; registry returns member node_advertise targets when available
#     tls: { cert: "", key: "", ca: "" }          # node_link client mTLS; empty = plain h2c
#   node_id: ""                                   # "" = hostname
#   labels: { zone: z1, pool: default }
#   data_endpoint: ""                             # host:port the router forwards data to; "" = api.listen
#   heartbeat_interval: 10s
`

// proxyConfigSkeleton is the commented authoring template for proxy.yaml
// (deploy/proxy.example.yaml is the curated copy).
const proxyConfigSkeleton = `# node-ctl proxy master config — node-ctl proxy serve --config <this>.
# External data-plane mode (serve's proxy.mode: external). A single master registers
# on conductor's plugin plane, owns listener sockets, and supervises workers that
# read a shared-memory route table.
config_socket: /run/sandbox/node-ctl.socket      # serve's control socket (= serve paths.config_socket)
paths:
  # proxy_executable: /opt/kuasar/bin/xproxy         # optional static custom App; only node-ctl -> master (workers reexec master)
  run_root: /run/sandbox                        # sandbox runtime root containing <sid>/ctl.sock (required)
data_listen: ":443"                              # master-bound ingress passed to workers; "" = UDS-only proxyForwarder
# proxy_netns: sw0_mgmt                          # forwarding netns for floatingip dials + conductor MMDS listen; "" = current netns
proxy_socket: /run/sandbox/proxy.sock            # UDS registered for conductor proxyForwarder
stats_socket: /run/sandbox/proxy-stats.sock      # master-only traffic stats UDS registered for conductor queries
shm_path: /run/sandbox/proxy-routes.shm           # shared route table path
route_capacity: 65536                            # fixed route slots
workers: 2                                       # worker processes supervised by the master
tls: { cert: /etc/node-ctl/tls/fullchain.pem, key: /etc/node-ctl/tls/privkey.pem }   # = serve's wildcard cert; omit = h2c
auth: enforce                                    # bootstrap fallback until serve pushes policy: off | log | enforce
park_timeout: 30s                                # bootstrap fallback
# metrics_listen: 127.0.0.1:9095                  # master metrics endpoint (aggregates worker counters)
`
