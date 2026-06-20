package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/nodectl"
)

func statusCmd(args []string) int {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	statePath := fs.String("state", "/run/node-ctl/state.json", "path to state.json")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	p := &nodectl.Persister{Path: *statePath}
	s, err := p.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if s == nil {
		fmt.Println("(no state — controller not running or never wrote state)")
		return 0
	}
	allocated := s.NodeAllocated()
	pool := s.AllocatablePool.MemoryBytes
	zone := s.MemoryZone()

	fmt.Printf("zone:                %s\n", zone)
	fmt.Printf("node_budget:         %d MiB / %d cpu_milli\n",
		s.NodeBudget.MemoryBytes>>20, s.NodeBudget.CPUMilli)
	fmt.Printf("host_reserved:       %d MiB / %d cpu_milli\n",
		s.HostReserved.MemoryBytes>>20, s.HostReserved.CPUMilli)
	fmt.Printf("operational_margin:  %d MiB / %d cpu_milli\n",
		s.OperationalMargin.MemoryBytes>>20, s.OperationalMargin.CPUMilli)
	fmt.Printf("allocatable_pool:    %d MiB / %d cpu_milli\n",
		pool>>20, s.AllocatablePool.CPUMilli)
	fmt.Printf("node_allocated:      %d MiB / %d cpu_milli\n",
		allocated.MemoryBytes>>20, allocated.CPUMilli)
	if pool > 0 {
		fmt.Printf("utilization:         %.1f%%\n",
			float64(allocated.MemoryBytes)/float64(pool)*100)
	}
	fmt.Printf("reservations:        %d\n", len(s.Reservations))
	return 0
}

func listCmd(args []string) int {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	statePath := fs.String("state", "/run/node-ctl/state.json", "path to state.json")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	p := &nodectl.Persister{Path: *statePath}
	s, err := p.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if s == nil {
		fmt.Println("[]")
		return 0
	}
	out, err := json.MarshalIndent(s.Reservations, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Println(string(out))
	return 0
}
