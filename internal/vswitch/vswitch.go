// Package vswitch wraps the vswitch-ctl CLI for per-sandbox port allocation.
//
//	attach <switch> --inner-ip=<ip> [--port=0]   -> JSON AttachOutput (tap mode: no --to-netns)
//	open-port <switch> --port=<N>  (TAPFD_SOCKET) -> tap-fd handoff (Network.TapFD.Exec)
//	detach <switch> --port=<N>                    -> plain text
//
// The orchestrator runs in tap mode (the port's tap fd goes to cloud-hypervisor).
// MTU is not returned by attach; it arrives in the tap-fd handoff metadata.
package vswitch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
)

// attachOutput mirrors sandbox-vswitch pkg/vswitch AttachOutput (subset we use).
type attachOutput struct {
	Port       uint32 `json:"port"`
	PortMAC    string `json:"port_mac"`
	InnerIP    string `json:"inner_ip"`
	FloatingIP string `json:"floating_ip"`
	Mode       string `json:"mode"`
}

// Port is the orchestrator-facing result of an attach.
type Port struct {
	Port       string // 1-based port handle (for detach / open-port)
	FloatingIP string // host-reachable address for user ports
	MAC        string // per-port MAC -> Network.MAC
	InnerIP    string // echoes the inner ip we requested
}

type CLI struct {
	bin string
	sw  string
}

func New(bin, sw string) *CLI { return &CLI{bin: bin, sw: sw} }

// Attach allocates a tap port on the switch with the given guest inner IP (CIDR).
func (c *CLI) Attach(ctx context.Context, innerIP string) (*Port, error) {
	cmd := exec.CommandContext(ctx, c.bin, "attach", c.sw, "--inner-ip="+innerIP, "--port=0")
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("vswitch attach %s: %w: %s", c.sw, err, errb.String())
	}
	var a attachOutput
	if err := json.Unmarshal(out.Bytes(), &a); err != nil {
		return nil, fmt.Errorf("vswitch attach %s: parse %q: %w", c.sw, out.String(), err)
	}
	return &Port{
		Port:       strconv.FormatUint(uint64(a.Port), 10),
		FloatingIP: a.FloatingIP,
		MAC:        a.PortMAC,
		InnerIP:    a.InnerIP,
	}, nil
}

// TapFDExec returns the argv for Network.TapFD.Exec: sandbox-ctl execs it with
// TAPFD_SOCKET set and receives this port's vnet_hdr tap queue fd via SCM_RIGHTS.
func (c *CLI) TapFDExec(port string) []string {
	return []string{c.bin, "open-port", c.sw, "--port=" + port}
}

// Detach releases the port.
func (c *CLI) Detach(ctx context.Context, port string) error {
	if port == "" {
		return nil
	}
	cmd := exec.CommandContext(ctx, c.bin, "detach", c.sw, "--port="+port)
	var errb bytes.Buffer
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("vswitch detach %s port %s: %w: %s", c.sw, port, err, errb.String())
	}
	return nil
}
