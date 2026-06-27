package main

import (
	"flag"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
)

// configCmd implements `cluster-ctl config <role>` — a per-role config diagnose +
// generate tool (role ∈ {registry, router, scaler}), mirroring `node-ctl config`.
// Each role has its own file + schema, so the skeleton and validation are
// role-specific (registry/router/scaler.yaml are independent — no shared file).
//
//	cluster-ctl config registry --template            # commented skeleton for registry.yaml
//	cluster-ctl config router   --config router.yaml  # normalize + validate, re-emit
//	cluster-ctl config scaler   --config scaler.yaml --resolve   # + defaulted effective form
//	  [-o <file>]                                                # write to file (default stdout)
func configCmd(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: cluster-ctl config <registry|router|scaler> [--template | --config <f> [--resolve]] [-o <f>]")
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
		load = func(p string) (any, error) { return clustercfg.LoadRegistry(p) }
	case "router":
		skeleton = routerConfigSkeleton
		load = func(p string) (any, error) { return clustercfg.LoadRouter(p) }
	case "scaler":
		skeleton = scalerConfigSkeleton
		load = func(p string) (any, error) { return clustercfg.LoadScaler(p) }
	default:
		return fmt.Errorf("config: unknown role %q (registry|router|scaler)", role)
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

const registryConfigSkeleton = `# cluster-ctl registry config — cluster-ctl registry --config <this> (cluster.md §3/§4.1).
# Registry-owned state authority + node_link / route_link / scale_link hub.
sandbox_group:                       # how per-group config is sourced (§6.2)
  providers: { key: store, sandbox_config: store, placement: store, image_pull: store }
  # encryption_key: ""               # AES-256 sealing self-stored group secrets (or SANDBOX_GROUP_ENCRYPTION_KEY env)
  # tls: { cert: ..., key: ..., ca: ... }   # client mTLS for external:<addr> providers
node_link:                           # nodes dial this
  listen: ":7700"
  # tls: { cert: ..., key: ..., ca: ... }   # server mTLS
  heartbeat_interval: 10s
  node_dead_after: 30s
  revision_retention: 10000
route_link:                          # routers/admin tools dial this
  listen: /run/cluster/route-link.sock
  # tls: { cert: ..., key: ..., ca: ... }   # server mTLS when split across hosts (§5.4)
scale_link:                          # scalers dial this
  listen: /run/cluster/scale-link.sock
  # tls: { cert: ..., key: ..., ca: ... }   # server mTLS when split across hosts (§5.4)
reserve:
  park_timeout: 30s
`

const routerConfigSkeleton = `# cluster-ctl router config — cluster-ctl router --config <this> (cluster-router.md §3).
# e2b-compatible unified ingress. Required: domain.
domain: sandboxes.example.com
route_link:                          # upstream: registry route_link
  endpoint: /run/cluster/route-link.sock  # UDS local; host:port remote
  # tls: { cert: ..., key: ..., ca: ... }   # client mTLS to dial a remote route_link (§5.4)
ingress:                             # downstream: e2b client ingress
  listen: ":443"
  # tls: { cert: ..., key: ... }     # wildcard *.<domain> + api.<domain>
auth:
  api_key: enforce                   # caller api_key auth: off | log | enforce
  data_plane: enforce                # data-plane access-token check: off | log | enforce
  cache_ttl: 60s                     # api_key↔group verification cache
# metrics_listen: ":9910"            # optional Prometheus text endpoint
`

const scalerConfigSkeleton = `# cluster-ctl scaler config — cluster-ctl scaler --config <this> (cluster-scaler.md §3).
# Standalone placement scheduler; dials registry scale_link (no listener of its own).
scale_link:                          # upstream: registry scale_link
  endpoint: /run/cluster/scale-link.sock  # UDS local; host:port remote
  # tls: { cert: ..., key: ..., ca: ... }   # client mTLS to dial a remote scale_link (§5.4)
placement:
  candidates: 2                      # P2C sample size
  zone_admit_max: yellow             # exclude nodes hotter than this (green|yellow|red)
  node_dead_after: 30s               # exclude nodes silent longer than this
  # shuffle_sharding:                # empty = static nodeSelectors only (cluster-scaler.md §4.4)
  #   - selector: { pool: gpu }
  #     shard_by: zone
  #     n: 2
`
