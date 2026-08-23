package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
)

func statusCmd(args []string) int {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	socket := fs.String("socket", nodectl.DefaultSocket, "controller UDS path")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	c, err := adminClient(*socket)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer c.Close()
	status, err := c.AdminStatus()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Printf("zone:                %s\n", status.Zone)
	fmt.Printf("node_budget:         %d MiB / %d cpu_milli\n", status.NodeBudget.MemoryBytes>>20, status.NodeBudget.CPUMilli)
	fmt.Printf("host_reserved:       %d MiB / %d cpu_milli\n", status.HostReserved.MemoryBytes>>20, status.HostReserved.CPUMilli)
	fmt.Printf("operational_margin:  %d MiB / %d cpu_milli\n", status.OperationalMargin.MemoryBytes>>20, status.OperationalMargin.CPUMilli)
	fmt.Printf("allocatable_pool:    %d MiB / %d cpu_milli\n", status.Pool.MemoryBytes>>20, status.Pool.CPUMilli)
	fmt.Printf("reserved_memory:     %d MiB / %d cpu_milli\n", status.Allocated.MemoryBytes>>20, status.Allocated.CPUMilli)
	if status.Pool.MemoryBytes > 0 {
		fmt.Printf("utilization:         %.1f%%\n", float64(status.Allocated.MemoryBytes)/float64(status.Pool.MemoryBytes)*100)
	}
	fmt.Printf("startup_in_flight:   %d MiB\n", status.StartupInFlight>>20)
	fmt.Printf("reservations:        %d (provisional=%d unknown=%d)\n", status.ReservationCount, status.ProvisionalCount, status.UnknownCount)
	fmt.Printf("drained:             %v\n", status.Drained)
	return 0
}

func listCmd(args []string) int {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	socket := fs.String("socket", nodectl.DefaultSocket, "controller UDS path")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	c, err := adminClient(*socket)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer c.Close()
	reservations, err := c.AdminList()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	out, err := json.MarshalIndent(reservations, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Println(string(out))
	return 0
}
