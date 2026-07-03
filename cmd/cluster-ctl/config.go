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
		load = func(p string) (any, error) { return clustercfg.LoadRegistry(p) }
	case "router":
		skeleton = routerConfigSkeleton
		load = func(p string) (any, error) { return clustercfg.LoadRouter(p) }
	case "placer":
		skeleton = placerConfigSkeleton
		load = func(p string) (any, error) { return clustercfg.LoadPlacer(p) }
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

const registryConfigSkeleton = `# cluster-ctl registry config — cluster-ctl registry --config <this> (docs/cluster.md).
member:                              # unified HTTP control plane
  id: registry-1
  listen: ":7700"
  # tls: { cert: ..., key: ..., ca: ... }   # server mTLS
membership:
  active: 1
  # next: 2                         # joint owner set target during membership change
  # old_grace: 1                    # previous version kept as peer/node_link ingress after cutover
  reload_ready_timeout: 10s          # wait for active/next members before applying reload
  versions:
    - version: 1
      members:
        - { id: registry-1, advertise: "https://registry-1.example:7700", node_advertise: "registry-1.example:7700" }
  owners:
    route_link: 1
    node_link: 1
    placer_link: 1
    node_list: 1
node_link:
  # listen: ""                       # optional split listener for node streams; empty = member.listen
  heartbeat_interval: 10s
  node_dead_after: 30s
route_link:
  park_timeout: 30s
node_list:
  watch_retention: 10000
placer_link:
  placer_label: placer.default          # placer memberlist label; not a configured placer list
  placer_replica_count: 3
  min_ready_placers: 1
  place_timeout: 2s
`

const routerConfigSkeleton = `# cluster-ctl router config — cluster-ctl router --config <this> (docs/cluster-router.md).
# e2b-compatible unified ingress. Required: domain.
domain: sandboxes.example.com
registry:                            # bootstrap endpoint for registry membership
  bootstrap: registry-1.example:7700
  # tls: { cert: ..., key: ..., ca: ... }   # client mTLS to registry control plane
ingress:                             # downstream: e2b client ingress
  listen: ":443"
  # tls: { cert: ..., key: ... }     # wildcard *.<domain> + api.<domain>
auth:
  api_key: enforce                   # caller api_key auth: off | log | enforce
  data_plane: enforce                # data-plane access-token check: off | log | enforce
  cache_ttl: 60s                     # api_key↔group verification cache
cache:
  route_ttl: 5m
  idle_timeout: 2m
# metrics_listen: ":9910"            # optional Prometheus text endpoint
`

const placerConfigSkeleton = `# cluster-ctl placer config — cluster-ctl placer --config <this> (docs/cluster-placer.md).
# Standalone placement scheduler; starts from registry membership, joins the
# placer memberlist label, consumes node_list, and provides placement.
placer:
  id: placer-1
  listen: ":7800"
  advertise: "https://placer-1.example:7800"
  memberlist_label: placer.default
  # tls: { cert: ..., key: ..., ca: ... }   # server mTLS for placer Place API
registry:
  bootstrap: registry-1.example:7700
  # tls: { cert: ..., key: ..., ca: ... }   # client mTLS to registry control plane
import_groups:                         # standalone placer requires at least one source
  - source_id: example-file-source
    source_type: file
    path: /var/lib/kuasar/groups
placement:
  candidates: 2                      # P2C sample size
  zone_admit_max: yellow             # exclude nodes hotter than this (green|yellow|red)
  node_dead_after: 30s               # exclude nodes silent longer than this
  import_source_owner_count: 3       # candidates that may race for each source lease
  import_source_lease_ttl: 15s       # registry-side source lease TTL
  selector_patch_refresh_interval: 1m # refresh unchanged selector patches before node key TTL
  # shuffle_sharding:                # empty = static nodeSelectors only
  #   - selector: { pool: gpu }
  #     shard_by: zone
  #     n: 2
`
