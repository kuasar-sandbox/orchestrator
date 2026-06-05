package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/config"
	"gopkg.in/yaml.v3"
)

// configCmd implements `orchestrator-ctl config` — emit a normalized config from
// --config (loading applies defaults + validates), or a commented skeleton via
// --template. Mirrors `sandbox-ctl config` / `flatten-ctl config`.
//
//	orchestrator-ctl config --config <file>   # normalize + validate, re-emit
//	orchestrator-ctl config --template        # emit a commented skeleton
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
const orchConfigSkeleton = `# orchestrator-ctl config — orchestrator-ctl serve --config <this>.
# The unmodified e2b SDK reaches this node via E2B_DOMAIN/E2B_API_KEY (dev:
# E2B_API_URL/E2B_SANDBOX_URL http). Required: domain + encryption_key.
domain: sandboxes.example.com
listen: ":443"                                   # dev: ":3000" (plain http/h2c)
tls_cert: /etc/orchestrator-ctl/tls/fullchain.pem
tls_key: /etc/orchestrator-ctl/tls/privkey.pem
# AES-256 keys for manifest keys at rest (":"-separated, first active). Prefer the
# ORCHESTRATOR_ENCRYPTION_KEY env (overrides). Generate: e2b-key-ctl gen-key.
encryption_key: "0000000000000000000000000000000000000000000000000000000000000000"
# Allowlist (who may create/build) is the manifest_keys table: orchestrator-ctl
# manifest-key add|remove|check|list. e2b API keys: e2b-key-ctl gen-apikey.
# Paths
run_root: /run/sandbox
base_root: /var/lib/sandbox
manifest_config: /opt/sandbox/manifest.yaml      # shared; manifest.key empty (key via env)
runtime_e2b_erofs: /opt/sandbox/runtime/v1/sandbox-runtime-e2b.erofs
runtime_erofs: /opt/sandbox/runtime/v1/sandbox-runtime.erofs
kernel: /opt/sandbox/kernel/6.1/vmlinux
# Pre-formatted empty ext4 seeding the cold-boot overlay upper (required for img
# templates). Deployment-provided (mkfs.ext4 on a sparse file).
overlay_diff_template: /opt/sandbox/overlay-templates/basic-1G.ext4
config_socket: /run/sandbox/orchestrator.socket  # run-task fetches LaunchSpecs here
# resource_socket: /run/sandbox-resource.sock    # sandbox-sentinel UDS (opt-in); omit = static cgroup
# Networking (vswitch)
switch: sw0
inner_cidr: 10.42.0.0/16
# Sandbox spec defaults (e2b templates carry no size)
default_vcpu: 2
default_memory: 2GiB
default_timeout_sec: 300
# Builder resource pool
builder_max_concurrent: 2
# builder_cpu_quota: "200%"                      # -> sandbox-builder.slice CPUQuota
# builder_memory_max: "8G"                       # -> sandbox-builder.slice MemoryMax
# Builder registry: where the e2b CLI pushes its client-built image (must match the
# CLI's E2B_IMAGE_URI_MASK; {templateID}/{buildID} tokens). When a build trigger
# omits fromImage, it is derived from this. builder_insecure_registry pulls over
# plain HTTP (dev/local registry).
# builder_image_uri_mask: "docker.sandboxes.example.com/e2b/custom-envs/{templateID}:{buildID}"
# builder_insecure_registry: false
# builder_platform: linux/amd64
# Systemd units (optional; defaults shown). Generated + installed at startup.
# unit_dir: /etc/systemd/system
# runner_unit: sandbox-runner@.service
# builder_unit: sandbox-builder@.service
# exec_dir: /opt/sandbox/bin                     # default = orchestrator binary dir
# install_units: true
`
