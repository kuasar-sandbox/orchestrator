package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
)

func adminClient(socketPath string) (*nodectl.Client, error) {
	c := &nodectl.Client{SocketPath: socketPath}
	if err := c.Connect(); err != nil {
		return nil, err
	}
	return c, nil
}

// resourceCmd dispatches `node-ctl resource <verb>`. The controller itself runs
// inside serve (resource_listen); these are admin / inspection clients to it.
func resourceCmd(args []string) int {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "usage: node-ctl resource {status|list|drain} ...")
		return 2
	}
	switch args[0] {
	case "status":
		return statusCmd(args[1:])
	case "list":
		return listCmd(args[1:])
	case "drain":
		return drainCmd(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "unknown resource verb %q\n", args[0])
		return 2
	}
}

func drainCmd(args []string) int {
	fs := flag.NewFlagSet("drain", flag.ContinueOnError)
	socket := fs.String("socket", nodectl.DefaultSocket, "controller UDS path")
	disable := fs.Bool("disable", false, "disable drain mode (re-enable admissions)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	c, err := adminClient(*socket)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer c.Close()
	if err := c.AdminDrain(!*disable); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	state := "enabled"
	if *disable {
		state = "disabled"
	}
	fmt.Printf("drain mode %s\n", state)
	return 0
}
