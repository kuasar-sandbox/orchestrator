// node-ctl is the reference implementation of the sandbox resource
// control protocol's controller role. See docs/node.md.
//
// Subcommands:
//
//	daemon   start the controller daemon (systemd unit entrypoint)
//	status   print node-level headroom + reservation summary
//	list     dump reservation table as JSON
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

func main() {
	if len(os.Args) < 2 {
		printUsage(os.Stderr)
		os.Exit(2)
	}
	switch os.Args[1] {
	case "daemon":
		os.Exit(daemonCmd(os.Args[2:]))
	case "status":
		os.Exit(statusCmd(os.Args[2:]))
	case "list":
		os.Exit(listCmd(os.Args[2:]))
	case "drain":
		os.Exit(drainCmd(os.Args[2:]))
	case "grant":
		os.Exit(grantCmd(os.Args[2:]))
	case "reclaim":
		os.Exit(reclaimCmd(os.Args[2:]))
	case "-h", "--help", "help":
		printUsage(os.Stdout)
		os.Exit(0)
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", os.Args[1])
		printUsage(os.Stderr)
		os.Exit(2)
	}
}

func printUsage(w *os.File) {
	fmt.Fprintf(w, `node-ctl — sandbox resource controller (reference implementation)

Usage:
  node-ctl daemon   [--config /etc/node-ctl/node-ctl.yaml]
  node-ctl status   [--state /run/node-ctl/state.json]
  node-ctl list     [--state /run/node-ctl/state.json]
  node-ctl drain    [--socket /run/sandbox-resource.sock] [--disable]
  node-ctl grant    <sid> --memory <size> [--socket ...]
  node-ctl reclaim  <sid> --memory <target> [--socket ...]

See docs/node.md for the protocol contract and subcommand details.
`)
}

func signalContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 4)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigCh
		cancel()
	}()
	return ctx, cancel
}
