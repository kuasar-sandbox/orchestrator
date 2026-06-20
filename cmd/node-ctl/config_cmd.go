package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/config"
	"gopkg.in/yaml.v3"
)

// configCmd implements `node-ctl config` — emit a normalized config from
// --config (loading applies defaults + validates), or a commented skeleton via
// --template. Mirrors `sandbox-ctl config` / `flatten-ctl config`.
//
//	node-ctl config --config <file>   # normalize + validate, re-emit
//	node-ctl config --template        # emit a commented skeleton
//	  [-o <file>]                             # write to file (default stdout)
func configCmd(args []string, _ *slog.Logger) error {
	fs := flag.NewFlagSet("config", flag.ExitOnError)
	cfgPath := fs.String("config", "", "input config YAML to normalize + validate")
	template := fs.Bool("template", false, "emit a commented skeleton config instead of reading --config")
	out := fs.String("o", "", "write output to this file instead of stdout")
	_ = fs.Parse(args)

	var output []byte
	if *template {
		output = []byte(orchConfigSkeleton)
	} else {
		if *cfgPath == "" {
			return fmt.Errorf("config: --config <file> or --template required")
		}
		cfg, err := config.Load(*cfgPath) // applies defaults + validates
		if err != nil {
			return err
		}
		b, err := yaml.Marshal(cfg)
		if err != nil {
			return err
		}
		output = b
	}
	if *out != "" {
		return os.WriteFile(*out, output, 0o644)
	}
	_, err := os.Stdout.Write(output)
	return err
}

// orchConfigSkeleton is the commented authoring template (see deploy/config.example.yaml).
const orchConfigSkeleton = `# node-ctl config — node-ctl serve --config <this>.
# The unmodified e2b SDK reaches this node via E2B_DOMAIN/E2B_API_KEY (dev:
# E2B_API_URL/E2B_SANDBOX_URL http). Required: api.domain + encryption_key.
# Config is grouped by concern; external binaries (sandbox-ctl, vswitch-ctl,
# flatten-ctl, …) are auto-discovered next to node-ctl then on PATH.
api:
  domain: sandboxes.example.com
  listen: ":443"                                 # dev: ":3000" (plain http/h2c)
  tls: { cert: /etc/node-ctl/tls/fullchain.pem, key: /etc/node-ctl/tls/privkey.pem }
proxy:
  mode: internal                                 # internal | external | off
  auth: enforce                                  # off | log | enforce: validate X-Access-Token
  # sockets: [/run/sandbox/proxy0.sock]          # external: routesync UDS the orchestrator dials
  # data_listen: ":8443"                          # dedicated data-plane listener; "" = share api.listen
  # park_timeout: 30s
  # metrics_listen: ":9900"                       # optional Prometheus text endpoint
# AES-256 keys for manifest keys at rest (":"-separated, first active). Prefer the
# NODE_CTL_ENCRYPTION_KEY env (overrides). Generate: e2b-key-ctl gen-key.
encryption_key: "0000000000000000000000000000000000000000000000000000000000000000"
# Shared remote manifest store (manifest.key empty; the tenant key arrives via env).
manifest_config: /opt/sandbox/manifest.yaml
# Allowlist (who may create/build/import) is the manifest_keys table: node-ctl
# manifest-key add|remove|check|list. e2b API keys: e2b-key-ctl gen-apikey.
paths:
  run_root: /run/sandbox
  base_root: /var/lib/sandbox
  config_socket: /run/sandbox/node-ctl.socket  # local control socket: task + manifest-key admin + api plane (h2c)
  # db_path: /var/lib/sandbox/node-ctl.db    # default = <base_root>/node-ctl.db
  # admin_pidfile: /run/sandbox/node-ctl-admin.pids  # PID allowlist for the admin plane; unset = socket 0600 perms (same uid/root)
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
    # control_socket: /run/sandbox-resource.sock  # node-ctl resource socket (opt-in); omit = static cgroup
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
# resource_listen: optional in-process node resource controller (admission /
# budget / density; sandbox-ctl dials its socket). Disabled = static cgroup.
# Inspect / operate with: node-ctl resource {status|list|drain|grant|reclaim}.
# resource_listen:
#   enabled: true
#   socket: /run/sandbox-resource.sock           # "" = built-in default
#   config: /etc/node-ctl/resource.yaml          # "" = built-in defaults
checkpoint:                                        # paused-state tiering
  mode: local                                     # local (node-bound files) | remote (portable manifest)
  local_dir: /var/lib/sandbox-saved
`
