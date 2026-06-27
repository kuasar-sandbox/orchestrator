package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/config"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/nodectl"
	"gopkg.in/yaml.v3"
)

// configCmd implements `node-ctl config <role>` — a per-role config diagnose +
// generate tool (role ∈ {serve, proxy}; cluster-ctl has its own registry/router/
// scaler). The role disambiguates the schema, so the skeleton + validation are
// role-specific:
//
//	node-ctl config serve --template            # commented skeleton for serve.yaml
//	node-ctl config proxy --config proxy.yaml   # normalize + validate, re-emit
//	node-ctl config serve --config serve.yaml --resolve   # + expand auto/derived
//	  [-o <file>]                                          # write to file (default stdout)
func configCmd(args []string, _ *slog.Logger) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: node-ctl config <serve|proxy> [--template | --config <f> [--resolve]] [-o <f>]")
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
	case "serve":
		output, err = renderServeConfig(*template, *resolve, *cfgPath)
	case "proxy":
		output, err = renderProxyConfig(*template, *cfgPath)
	default:
		return fmt.Errorf("config: unknown role %q (serve|proxy)", role)
	}
	if err != nil {
		return err
	}
	if *out != "" {
		return os.WriteFile(*out, output, 0o644)
	}
	_, err = os.Stdout.Write(output)
	return err
}

// renderServeConfig handles `config serve`. --resolve additionally expands the
// resource controller's "auto" memory/cpu (and validates its watermarks) so the
// operator sees the effective numbers.
func renderServeConfig(template, resolve bool, path string) ([]byte, error) {
	if template {
		return []byte(serveConfigSkeleton), nil
	}
	if path == "" {
		return nil, fmt.Errorf("config serve: --config <file> or --template required")
	}
	cfg, err := config.Load(path) // applies defaults + validates
	if err != nil {
		return nil, err
	}
	if resolve && cfg.ResourceListen != nil && cfg.ResourceListen.Enabled {
		r, rerr := nodectl.Resolve(cfg.ResourceListen)
		if rerr != nil {
			return nil, fmt.Errorf("resource_listen: %w", rerr)
		}
		cfg.ResourceListen.Socket = r.Listen
		cfg.ResourceListen.Resources.PhysicalMemory = strconv.FormatUint(r.PhysicalMemory, 10)
		cfg.ResourceListen.Resources.PhysicalCPU = strconv.Itoa(int(r.PhysicalCPU / 1000))
	}
	return yaml.Marshal(cfg)
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
	return yaml.Marshal(cfg)
}

// serveConfigSkeleton is the commented authoring template for serve.yaml
// (deploy/serve.example.yaml is the curated copy). Config is grouped by concern;
// external binaries (sandbox-ctl, vswitch-ctl, flatten-ctl, …) are auto-discovered
// next to node-ctl then on PATH.
const serveConfigSkeleton = `# node-ctl serve config — node-ctl serve --config <this>.
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
  #                                               # external mode: the data port lives in proxy.yaml (workers own it).
  # park_timeout: 30s
  # metrics_listen: ":9900"                       # serve's own Prometheus text endpoint
# AES-256 keys for manifest keys at rest (":"-separated, first active). Prefer the
# NODE_CONFIG_ENCRYPTION_KEY env (overrides). Generate: e2b-key-ctl gen-key.
encryption_key: "0000000000000000000000000000000000000000000000000000000000000000"
# Shared remote manifest store (manifest.key empty; the tenant key arrives via env).
manifest_config: /opt/sandbox/manifest.yaml
paths:
  run_root: /run/sandbox
  base_root: /var/lib/sandbox
  config_socket: /run/sandbox/node-ctl.socket  # local control socket: task + manifest-key admin + plugin + api plane (h2c)
  # db_path: /var/lib/sandbox/node-ctl.db    # default = <base_root>/node-ctl.db
  # admin_pidfile: /run/sandbox/node-ctl-admin.pids  # PID allowlist for the admin plane; unset = socket 0600 perms
  # plugin_pidfile: /run/sandbox/node-ctl-plugin.pids # PID allowlist for the plugin plane (proxy/agent registration)
# units:                                          # systemd template units (defaults shown)
#   dir: /etc/systemd/system
#   runner: sandbox-runner@.service
#   builder: sandbox-builder@.service
#   install: true
sandbox:                                          # sandbox-instance defaults
  timeout_sec: 300
  resources:
    vcpu: 2
    memory: 2GiB
    # control_socket: /run/sandbox-resource.sock  # resource-controller UDS (opt-in); omit = static cgroup.
    #                                              # Set this to resource_listen.socket when hosting the controller below.
  network:
    switch: sw0
    hostname: sandbox                             # guest hostname (sethostname + /etc/hosts)
    dns: [169.254.169.253]                        # /etc/resolv.conf nameserver(s) injected into the guest
    e2b:  { inner_ip: 169.254.0.21/30, nexthop: 169.254.0.22 }   # e2b: /30 + gateway for envd port-forward
    bare: { inner_ip: 169.254.1.1/31,  nexthop: 169.254.1.0 }
  boot:
    kernel: /opt/sandbox/kernel/6.1/vmlinux
    runtime_e2b: /opt/sandbox/runtime/v1/sandbox-runtime-e2b.erofs
    runtime_base: /opt/sandbox/runtime/v1/sandbox-runtime.erofs
    # Pre-formatted empty ext4 seeding the cold-boot overlay upper (required for img templates).
    overlay_diff_template: /opt/sandbox/overlay-templates/basic-1G.ext4
builder:                                           # builds run INSIDE build sandboxes
  max_concurrent: 2
  runtime_builder: /opt/sandbox/runtime/v1/sandbox-runtime-builder.erofs  # build-sandbox guest runtime
  diff_template: /opt/sandbox/overlay-templates/builder-8G.ext4  # build VM writable disk (pull cache + export scratch)
  # vcpu: 2                                       # per build-sandbox capacity
  # memory: 4GiB
  # cpu_quota: "200%"                             # -> sandbox-builder.slice CPUQuota
  # memory_max: "8G"                              # -> sandbox-builder.slice MemoryMax
  # insecure_registry: false                      # pull base over plain HTTP (dev/local registry)
  # platform: linux/amd64
  # pull_timeout_sec: 600                         # in-guest pull+flatten | one RUN step |
  # step_timeout_sec: 600                         # readyCmd poll | whole build
  # ready_timeout_sec: 120
  # total_timeout_sec: 1800
  # image_uri_mask must match the CLI's E2B_IMAGE_URI_MASK ({templateID}/{buildID} tokens)
  # AND be reachable from inside a build sandbox (the pull runs in the guest):
  # image_uri_mask: "docker.sandboxes.example.com/e2b/custom-envs/{templateID}:{buildID}"
  # files_storage: COPY build contexts; client direct-uploads (presigned PUT) to
  # this bucket, build fetches (presigned GET). Unset → COPY rejected (501).
  # Local/single-node: point at versitygw (sandbox-deps: make versitygw).
  # files_storage:
  #   endpoint: https://obs.cn-north-4.example.com   # versitygw: http://127.0.0.1:7070
  #   region: cn-north-4
  #   bucket: kuasar-build-files                            # required
  #   force_path_style: false                               # versitygw/minio need true
  #   access_key: ""                                        # empty → AWS default chain
  #   secret_key: ""
# resource_listen: optional in-process node resource controller (admission / budget /
# density; sandbox-ctl dials its socket). Absent/disabled = static cgroup. The tuning
# is inlined here (no separate file). Inspect / operate with:
# node-ctl resource {status|list|drain|grant|reclaim}.
# resource_listen:
#   enabled: true
#   socket: /run/sandbox-resource.sock           # "" = pkg/resource default (sandbox-ctl's default)
#   # Advanced tuning — all defaulted (node-resource.md §3.2); usually left untouched:
#   # state_path: /run/node-ctl/state.json
#   # audit_path: /run/node-ctl/audit.log
#   # resources: { physical_memory: auto, physical_cpu: auto, host_reserved: { memory: 16GiB, cpu: 1.5 } }
#   # watermarks: { operational_margin_factor: 0.10, high_factor: 0.85, low_factor: 0.70, emergency_factor: 0.05, startup_factor: 0.50 }
#   # rate_limits: { memory_grant_per_sec_factor: 0.05 }
#   # admission: { rate: 4, burst: 16, startup_ttl: 30s, queue_ttl: 30s, queue_max_depth: 256 }
#   # dampening: { recover_duration: 60s, cooldown_periods: 10 }
checkpoint:                                        # paused-state tiering
  mode: local                                     # local (node-bound files) | remote (portable manifest)
  local_dir: /var/lib/sandbox-saved
# mmds:                                            # optional envd FC-mode token re-keying
#   enabled: false                                # false => envd non-secure; proxy.auth must be enforce
#   listen: 127.0.0.1:19254                        # MMDS listener (vswitch --mgmt-service target)
# cluster:                                         # connect this node to registry node_link (node.md §10)
#   node_link:                                    # how to reach registry node_link
#     endpoint: registry.cluster.example.com:7700 # "" = standalone single-node
#     tls: { cert: "", key: "", ca: "" }          # node_link client mTLS; empty = plain h2c
#   node_id: ""                                   # "" = hostname
#   labels: { zone: z1, pool: default }
#   data_endpoint: ""                             # host:port the router forwards data to; "" = api.listen
#   heartbeat_interval: 10s
`

// proxyConfigSkeleton is the commented authoring template for proxy.yaml
// (deploy/proxy.example.yaml is the curated copy).
const proxyConfigSkeleton = `# node-ctl proxy worker config — node-ctl proxy --config <this> --id <name>.
# External data-plane mode (serve's proxy.mode: external). One file shared by all
# worker instances; per-instance identity is on the command line:
#   --id <name>            unique per worker (required)
#   --socket <uds>         gateway-forward UDS; default <dir(config_socket)>/<id>.sock
#   --metrics-listen <a>   optional Prometheus endpoint, per-instance (ports must differ)
#   --mmds                 host the FC MMDS service on this instance (addr = mmds_listen)
config_socket: /run/sandbox/node-ctl.socket      # serve's control socket (= serve paths.config_socket)
data_listen: ":443"                              # SO_REUSEPORT ingress (all workers share it); "" = UDS-only gateway-forward
tls: { cert: /etc/node-ctl/tls/fullchain.pem, key: /etc/node-ctl/tls/privkey.pem }   # = serve's wildcard cert; omit = h2c
auth: enforce                                    # bootstrap fallback until serve pushes policy: off | log | enforce
park_timeout: 30s                                # bootstrap fallback
# mmds_listen: 127.0.0.1:19254                    # FC MMDS addr the --mmds worker binds (only when serve has mmds.enabled)
`
