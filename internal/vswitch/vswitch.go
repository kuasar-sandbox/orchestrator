// Package vswitch wraps `connector-ctl vswitch` for per-sandbox port allocation.
//
//	vswitch attach <switch> --inner-ip=<ip> [--port=0] [--transit-*]  -> JSON AttachOutput
//	vswitch open-port <switch> --port=<N>  (TAPFD_SOCKET) -> tap-fd handoff
//	vswitch detach <switch> --port=<N>                    -> plain text
//
// The orchestrator runs in tap mode (the port's tap fd goes to cloud-hypervisor).
// Guest MTU is sandbox/node configuration; the tap-fd handoff only carries fd
// ownership plus the port identity metadata required by the VMM.
package vswitch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
)

// attachOutput mirrors connector pkg/vswitch AttachOutput (subset we use).
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

// TapFD describes how sandbox-ctl should acquire this port's tap queue fd.
type TapFD struct {
	Exec    []string
	Socket  string
	Request string
	Timeout string
}

type CLI struct {
	bin         string
	sw          string
	tapFDSocket string
}

type Option func(*CLI)

func WithTapFDSocket(path string) Option {
	return func(c *CLI) { c.tapFDSocket = path }
}

func New(bin, sw string, opts ...Option) *CLI {
	c := &CLI{bin: bin, sw: sw}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// AttachReq is a per-sandbox attach: the guest inner IP (plain, not CIDR) plus
// optional GENEVE transit parameters (tenant-network overlay) when overridden via
// metadata (see internal/orch override). Zero-valued transit fields are omitted.
type AttachReq struct {
	InnerIP          string // plain inner IP, required
	TransitGatewayIP string // GENEVE gateway IP
	TransitGeneveVNI uint32 // GENEVE VNI
	TransitMAC       string // transit destination MAC
}

// Attach allocates a tap port on the switch for the request's guest inner IP.
func (c *CLI) Attach(ctx context.Context, req AttachReq) (*Port, error) {
	args := []string{"vswitch", "attach", c.sw, "--inner-ip=" + req.InnerIP, "--port=0"}
	if req.TransitGatewayIP != "" {
		args = append(args, "--transit-gateway-ip="+req.TransitGatewayIP)
	}
	if req.TransitGeneveVNI != 0 {
		args = append(args, "--transit-geneve-vni="+strconv.FormatUint(uint64(req.TransitGeneveVNI), 10))
	}
	if req.TransitMAC != "" {
		args = append(args, "--transit-mac-addr="+req.TransitMAC)
	}
	cmd := exec.CommandContext(ctx, c.bin, args...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("connector vswitch attach %s: %w: %s", c.sw, err, errb.String())
	}
	var a attachOutput
	if err := json.Unmarshal(out.Bytes(), &a); err != nil {
		return nil, fmt.Errorf("connector vswitch attach %s: parse %q: %w", c.sw, out.String(), err)
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
	return []string{c.bin, "vswitch", "open-port", c.sw, "--port=" + port}
}

// TapFD returns the configured tapfd transport for a port.
func (c *CLI) TapFD(port string) TapFD {
	if c.tapFDSocket != "" {
		return TapFD{
			Socket:  c.tapFDSocket,
			Request: fmt.Sprintf("VSWITCH=%s PORT=%s", c.sw, port),
		}
	}
	return TapFD{Exec: c.TapFDExec(port)}
}

// Detach releases the port.
func (c *CLI) Detach(ctx context.Context, port string) error {
	if port == "" {
		return nil
	}
	cmd := exec.CommandContext(ctx, c.bin, "vswitch", "detach", c.sw, "--port="+port)
	var errb bytes.Buffer
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("connector vswitch detach %s port %s: %w: %s", c.sw, port, err, errb.String())
	}
	return nil
}
