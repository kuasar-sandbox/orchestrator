package main

import (
	"flag"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"

	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
)

// configCmd implements `cluster-ctl config <role>` — a per-role config diagnose +
// generate tool (role ∈ {registry, router, placer}), mirroring `node-ctl config`.
// Each role has its own file + schema, so the skeleton and validation are
// role-specific (registry/router/placer.yaml are independent — no shared file).
//
//	cluster-ctl config registry --template            # commented skeleton for registry.yaml
//	cluster-ctl config router   --config router.yaml  # normalize + validate, re-emit
//	cluster-ctl config placer   --config placer.yaml --resolve   # + defaulted effective form
//	  [-o <file>]                                                # write to file (default stdout)
func configCmd(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: cluster-ctl config <registry|router|placer> [--template | --config <f> [--resolve]] [-o <f>]")
	}
	role := args[0]
	fs := flag.NewFlagSet("config "+role, flag.ExitOnError)
	cfgPath := fs.String("config", "", "input config YAML to normalize + validate")
	template := fs.Bool("template", false, "emit a commented skeleton for the role")
	// Cluster role configs have no auto-detected values, so --resolve == --config
	// (defaults already are the effective form). Accepted for CLI symmetry.
	_ = fs.Bool("resolve", false, "expand defaults to the effective form")
	out := fs.String("o", "", "write output to this file instead of stdout")
	_ = fs.Parse(args[1:])

	var (
		skeleton string
		load     func(string) (any, error)
	)
	switch role {
	case "registry":
		skeleton = registryConfigSkeleton
		load = func(p string) (any, error) { return clustercfg.LoadConsensusRegistry(p) }
	case "router":
		skeleton = routerConfigSkeleton
		load = func(p string) (any, error) { return clustercfg.LoadConsensusRouter(p) }
	case "placer":
		skeleton = placerConfigSkeleton
		load = func(p string) (any, error) { return clustercfg.LoadFinalPlacer(p) }
	default:
		return fmt.Errorf("config: unknown role %q (registry|router|placer)", role)
	}

	var output []byte
	if *template {
		output = []byte(skeleton)
	} else {
		if *cfgPath == "" {
			return fmt.Errorf("config %s: --config <file> or --template required", role)
		}
		cfg, err := load(*cfgPath)
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

const registryConfigSkeleton = `# cluster-ctl registry --config <this>
member:
  id: registry-1
  listen: ":7700"
  tls: { cert: /etc/kuasar/tls/registry.crt, key: /etc/kuasar/tls/registry.key, ca: /etc/kuasar/tls/ca.crt }
registry_layout:
  chain: /etc/kuasar/registry-layout/chain.json
  keys: /etc/kuasar/registry-layout/keyring.json
  guard: /var/lib/kuasar/registry-layout.guard
storage:
  nodehost_dir: /var/lib/kuasar/raft/nodehost
  wal_dir: /var/lib/kuasar/raft/wal
  state_engine_dir: /var/lib/kuasar/raft/state
  enrollment_path: /var/lib/kuasar/raft/enrollment.json
  raft_listen: 127.0.0.1:63001
  open_mode: bootstrap              # bootstrap | join | restart
  bootstrap_secret_file: /etc/kuasar/bootstrap.secret
  storage_protection: dm-crypt      # dm-crypt | ephemeral-tmpfs
  initialize_workers: 16
  transition_workers: 16
  snapshot_workers: 4
  operation_timeout: 5s
  fence_retention: 1h
  tls: { cert: /etc/kuasar/tls/registry.crt, key: /etc/kuasar/tls/registry.key, ca: /etc/kuasar/tls/ca.crt }
placers:
  endpoints:
    - { name: placer-1, endpoint: "https://placer-1.example:7800" }
  tls: { cert: /etc/kuasar/tls/registry.crt, key: /etc/kuasar/tls/registry.key, ca: /etc/kuasar/tls/ca.crt }
session:
  max_nodes: 5000
  anti_entropy: 2s
  event_workers: 32
  reconnect_per_second: 200
workflow:
  park_timeout: 30s
  poll_interval: 20ms
  permit_refresh: 1s
  recovery_scan_interval: 250ms
  recovery_shards_per_scan: 64
  recovery_workers: 8
  recovery_per_node_workers: 1
  compaction_workers: 4
  pending_workflows_per_page: 256
  recovery_page_objects: 64
  recovery_page_bytes: 524288
  recovery_max_report_bytes: 67108864
  recovery_lookup_page: 256
`

const routerConfigSkeleton = `# cluster-ctl router --config <this>
domain: sandboxes.example.com
registry_layout:
  chain: /etc/kuasar/registry-layout/chain.json
  keys: /etc/kuasar/registry-layout/keyring.json
  guard: /var/lib/kuasar/router-registry-layout.guard
registry_tls: { cert: /etc/kuasar/tls/router.crt, key: /etc/kuasar/tls/router.key, ca: /etc/kuasar/tls/ca.crt }
registry_response_timeout: 35s # must exceed registry workflow.park_timeout
node_tls: { cert: /etc/kuasar/tls/router.crt, key: /etc/kuasar/tls/router.key, ca: /etc/kuasar/tls/ca.crt }
providers:
  endpoints:
    - { name: provider-1, endpoint: "https://provider-1.example:7900" }
  tls: { cert: /etc/kuasar/tls/router.crt, key: /etc/kuasar/tls/router.key, ca: /etc/kuasar/tls/ca.crt }
ingress:
  listen: ":443"
  tls: { cert: /etc/kuasar/tls/ingress.crt, key: /etc/kuasar/tls/ingress.key }
auth:
  api_key: enforce
  data_plane: enforce
  cache_ttl: 60s
cache:
  route_ttl: 5m
  idle_timeout: 2m
# metrics_listen: ":9910"
`

const placerConfigSkeleton = `# cluster-ctl placer --config <this>
placer:
  id: placer-1
  listen: ":7800"
  tls: { cert: /etc/kuasar/tls/placer.crt, key: /etc/kuasar/tls/placer.key, ca: /etc/kuasar/tls/ca.crt }
group_sources:
  - source_id: example-file-source
    source_type: file
    path: /var/lib/kuasar/groups
placement:
  candidates: 4
`
