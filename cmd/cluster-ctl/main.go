// Command cluster-ctl is the kuasar-sandbox cluster control plane (cluster.md).
//
//	cluster-ctl registry  --config <registry.yaml>   # state authority + node_link hub
//	cluster-ctl router    --config <router.yaml>     # e2b-compatible unified ingress
//	cluster-ctl scaler    --config <scaler.yaml>     # placement scheduler
//	cluster-ctl scaler import --config <scaler.yaml> -i groups.jsonl
//	cluster-ctl config <registry|router|scaler> [--template|--config <f>|--resolve]  # config diagnose / generate
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
		err = runScaler(os.Args[2:], log)
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
	fmt.Fprintln(os.Stderr, "usage: cluster-ctl {registry|router|scaler|config|version} [flags]")
	os.Exit(2)
}
