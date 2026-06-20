// Command cluster-ctl is the kuasar-sandbox cluster control plane (cluster.md).
//
//	cluster-ctl registry  --config <yaml>   # durable state authority + node-link hub
//	cluster-ctl router    --config <yaml>   # e2b-compatible unified ingress (Phase 3)
//	cluster-ctl scaler    --config <yaml>   # placement scheduler (Phase 4)
//	cluster-ctl group     <upsert|get|list|remove> ...   # sandbox-group config admin
//	cluster-ctl config    [--template]      # render skeleton / normalize config
//	cluster-ctl version
//
// One binary, three roles as independent processes (cluster.md §1/§4.2).
package main

import (
	"fmt"
	"log/slog"
	"os"
)

var version = "0.1.0-dev"

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	var err error
	switch os.Args[1] {
	case "registry":
		err = runRegistry(os.Args[2:], log)
	case "router":
		err = runRouter(os.Args[2:], log)
	case "scaler":
		err = fmt.Errorf("scaler is not implemented yet (Phase 4)")
	case "group":
		err = groupCmd(os.Args[2:])
	case "config":
		err = configCmd(os.Args[2:])
	case "version", "-v", "--version":
		fmt.Println("cluster-ctl", version)
	default:
		usage()
	}
	if err != nil {
		log.Error("cluster-ctl", "cmd", os.Args[1], "err", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: cluster-ctl {registry|router|scaler|group|config|version} [flags]")
	os.Exit(2)
}
