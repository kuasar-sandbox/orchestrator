package main

import (
	"flag"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
)

// configCmd implements `cluster-ctl config` — emit a commented skeleton
// (--template) or normalize + validate a config (--config), mirroring
// `node-ctl config`.
func configCmd(args []string) error {
	fs := flag.NewFlagSet("config", flag.ExitOnError)
	cfgPath := fs.String("config", "", "input config YAML to normalize + validate")
	template := fs.Bool("template", false, "emit a commented skeleton instead of reading --config")
	out := fs.String("o", "", "write output to this file instead of stdout")
	_ = fs.Parse(args)

	var output []byte
	if *template {
		output = []byte(clusterConfigSkeleton)
	} else {
		if *cfgPath == "" {
			return fmt.Errorf("config: --config <file> or --template required")
		}
		cfg, err := clustercfg.Load(*cfgPath)
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

const clusterConfigSkeleton = `# cluster-ctl config (cluster.md §3). Required: domain + store.
domain: sandboxes.example.com
store:
  kind: sqlite                       # sqlite | etcd | raft (etcd/raft = Phase 7)
  dsn: /var/lib/cluster/registry.db
group_config:
  providers: { key: store, sandbox_config: store, placement: store }
  # encryption_key: ""               # AES-256 sealing self-stored manifest_key (or CLUSTER_GROUP_ENCRYPTION_KEY env)
channel:
  listen: ":7700"                    # nodes dial this (node-link)
  # tls: { cert: ..., key: ..., ca: ... }   # mTLS (Phase 7)
  heartbeat_interval: 10s
  node_dead_after: 30s
  revision_retention: 10000
op:
  listen: /run/cluster/registry.sock # router / scaler dial (Phase 3/4)
reserve:
  park_timeout: 30s
router:                              # cluster-router.md §3 (Phase 3)
  listen: ":443"
  auth_cache_ttl: 60s
  data_plane_auth: enforce
scaler:                              # cluster-scaler.md §3 (Phase 4)
  place_candidates: 2
  zone_admit_max: yellow
`
