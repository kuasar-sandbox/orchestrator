package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/fullof-work/mass-sandbox/pkg/config"
	"github.com/fullof-work/mass-sandbox/pkg/nodectl"
)

func adminClient(socketPath string) (*nodectl.Client, error) {
	c := &nodectl.Client{SocketPath: socketPath}
	if err := c.Connect(); err != nil {
		return nil, err
	}
	return c, nil
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

func grantCmd(args []string) int {
	fs := flag.NewFlagSet("grant", flag.ContinueOnError)
	socket := fs.String("socket", nodectl.DefaultSocket, "controller UDS path")
	memSize := fs.String("memory", "", "delta memory to grant (e.g. 256MiB)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: node-ctl grant <sandbox-id> --memory <size>")
		return 2
	}
	if *memSize == "" {
		fmt.Fprintln(os.Stderr, "--memory required")
		return 2
	}
	delta, err := config.ParseSize(*memSize)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	sid := fs.Arg(0)
	c, err := adminClient(*socket)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer c.Close()
	newAlloc, err := c.AdminGrant(sid, delta)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Printf("granted +%d to %s, new allocatable=%d\n", delta, sid, newAlloc)
	return 0
}

func reclaimCmd(args []string) int {
	fs := flag.NewFlagSet("reclaim", flag.ContinueOnError)
	socket := fs.String("socket", nodectl.DefaultSocket, "controller UDS path")
	memSize := fs.String("memory", "", "target allocatable memory after reclaim (e.g. 256MiB)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: node-ctl reclaim <sandbox-id> --memory <target>")
		return 2
	}
	if *memSize == "" {
		fmt.Fprintln(os.Stderr, "--memory required")
		return 2
	}
	target, err := config.ParseSize(*memSize)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	sid := fs.Arg(0)
	c, err := adminClient(*socket)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer c.Close()
	newAlloc, err := c.AdminReclaim(sid, target)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Printf("reclaimed %s to allocatable=%d\n", sid, newAlloc)
	return 0
}
