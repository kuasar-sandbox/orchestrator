// Package vswitch wraps `connector-ctl vswitch` for per-sandbox port allocation.
// When a persistent tapfd socket is configured, attach/detach use TAPFD/1
// PREPARE/RELEASE on that socket; otherwise they fall back to short-lived CLI
// commands.
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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// ErrPortNotAttached reports that a RELEASE target was already free. Callers
// that durably remember connector ownership can treat this exact condition as
// successful replay after a controller restart; all other provider failures
// remain fatal.
var ErrPortNotAttached = errors.New("connector vswitch port not attached")

type tapFDProviderError struct {
	code    string
	message string
}

func (e *tapFDProviderError) Error() string {
	if e.message != "" {
		return fmt.Sprintf("provider error %s: %s", e.code, e.message)
	}
	return fmt.Sprintf("provider error %s", e.code)
}

const (
	tapfdRequestVersion = "TAPFD/1"
	tapfdMaxLineSize    = 512
	tapfdSocketTimeout  = 5 * time.Second
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
	if c.tapFDSocket != "" {
		return c.prepare(ctx, req)
	}
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
	if c.tapFDSocket != "" {
		return c.release(ctx, port)
	}
	cmd := exec.CommandContext(ctx, c.bin, "vswitch", "detach", c.sw, "--port="+port)
	var errb bytes.Buffer
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		if cliPortNotAttached(errb.String(), port) {
			return fmt.Errorf("connector vswitch detach %s port %s: %w", c.sw, port, ErrPortNotAttached)
		}
		return fmt.Errorf("connector vswitch detach %s port %s: %w: %s", c.sw, port, err, errb.String())
	}
	return nil
}

func (c *CLI) prepare(ctx context.Context, req AttachReq) (*Port, error) {
	fields := []string{
		"VSWITCH=" + c.sw,
		"INNER_IP=" + req.InnerIP,
	}
	if req.TransitGatewayIP != "" {
		fields = append(fields, "TRANSIT_GATEWAY_IP="+req.TransitGatewayIP)
	}
	if req.TransitGeneveVNI != 0 {
		fields = append(fields, "TRANSIT_GENEVE_VNI="+strconv.FormatUint(uint64(req.TransitGeneveVNI), 10))
	}
	if req.TransitMAC != "" {
		fields = append(fields, "TRANSIT_MAC="+req.TransitMAC)
	}
	out, err := c.tapfdCall(ctx, "PREPARE", fields...)
	if err != nil {
		return nil, err
	}
	port := out["port"]
	if port == "" {
		return nil, fmt.Errorf("connector tapfd prepare %s: missing port in response", c.sw)
	}
	portNum, err := strconv.ParseUint(port, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("connector tapfd prepare %s: invalid port %q: %w", c.sw, port, err)
	}
	if portNum == 0 {
		return nil, fmt.Errorf("connector tapfd prepare %s: invalid port %q", c.sw, port)
	}
	if mode := out["mode"]; mode != "" && mode != "tap" {
		return nil, fmt.Errorf("connector tapfd prepare %s: prepared port %s is %s, not tap", c.sw, port, mode)
	}
	return &Port{
		Port:       port,
		FloatingIP: out["floating_ip"],
		MAC:        out["mac"],
		InnerIP:    out["ip"],
	}, nil
}

func (c *CLI) release(ctx context.Context, port string) error {
	portNum, err := strconv.ParseUint(port, 10, 32)
	if err != nil {
		return fmt.Errorf("connector tapfd release %s: invalid port %q: %w", c.sw, port, err)
	}
	if portNum == 0 {
		return fmt.Errorf("connector tapfd release %s: invalid port %q", c.sw, port)
	}
	_, err = c.tapfdCall(ctx, "RELEASE", "VSWITCH="+c.sw, "PORT="+port)
	if tapFDPortNotAttached(err, strconv.FormatUint(portNum, 10)) {
		return fmt.Errorf("connector tapfd release %s port %s: %w", c.sw, port, ErrPortNotAttached)
	}
	return err
}

func tapFDPortNotAttached(err error, port string) bool {
	var providerErr *tapFDProviderError
	return errors.As(err, &providerErr) &&
		providerErr.code == "PORT_UNAVAILABLE" &&
		providerErr.message == "port_"+port+":_port_not_attached"
}

func cliPortNotAttached(stderr, port string) bool {
	portNum, err := strconv.ParseUint(port, 10, 32)
	if err != nil || portNum == 0 {
		return false
	}
	want := "port " + strconv.FormatUint(portNum, 10) + ": port not attached"
	for _, line := range strings.Split(stderr, "\n") {
		line = strings.TrimSpace(line)
		line = strings.TrimPrefix(line, "Error: ")
		if line == want {
			return true
		}
	}
	return false
}

func (c *CLI) tapfdCall(ctx context.Context, op string, fields ...string) (map[string]string, error) {
	line := tapfdRequestVersion + " " + op
	for _, field := range fields {
		if err := validateTapFDToken(field); err != nil {
			return nil, err
		}
		line += " " + field
	}
	if len(line)+1 > tapfdMaxLineSize {
		return nil, fmt.Errorf("connector tapfd %s %s: request line too long", strings.ToLower(op), c.sw)
	}

	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", c.tapFDSocket)
	if err != nil {
		return nil, fmt.Errorf("connector tapfd %s %s: dial %s: %w", strings.ToLower(op), c.sw, c.tapFDSocket, err)
	}
	defer conn.Close()
	setTapFDDeadline(ctx, conn)
	if _, err := io.Copy(conn, strings.NewReader(line+"\n")); err != nil {
		return nil, fmt.Errorf("connector tapfd %s %s: write request: %w", strings.ToLower(op), c.sw, err)
	}
	resp, err := readTapFDLine(bufio.NewReader(conn))
	if err != nil {
		return nil, fmt.Errorf("connector tapfd %s %s: read response: %w", strings.ToLower(op), c.sw, err)
	}
	fieldsMap, err := parseTapFDResponse(resp)
	if err != nil {
		return nil, fmt.Errorf("connector tapfd %s %s: %w", strings.ToLower(op), c.sw, err)
	}
	return fieldsMap, nil
}

func setTapFDDeadline(ctx context.Context, conn net.Conn) {
	deadline := time.Now().Add(tapfdSocketTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	_ = conn.SetDeadline(deadline)
}

func validateTapFDToken(tok string) error {
	if tok == "" || strings.ContainsAny(tok, " \t\r\n\x00") || !strings.Contains(tok, "=") {
		return fmt.Errorf("invalid tapfd request token %q", tok)
	}
	return nil
}

func readTapFDLine(r *bufio.Reader) (string, error) {
	var b strings.Builder
	for b.Len() < tapfdMaxLineSize {
		c, err := r.ReadByte()
		if err != nil {
			return "", err
		}
		if c == '\n' {
			return b.String(), nil
		}
		if c == 0 {
			return "", fmt.Errorf("response line contains NUL")
		}
		b.WriteByte(c)
	}
	return "", fmt.Errorf("response line too long: %d > %d", b.Len()+1, tapfdMaxLineSize)
}

func parseTapFDResponse(line string) (map[string]string, error) {
	toks := strings.Fields(strings.TrimRight(strings.TrimSpace(line), "\r"))
	if len(toks) < 2 {
		return nil, fmt.Errorf("malformed response %q", line)
	}
	if toks[0] != tapfdRequestVersion {
		return nil, fmt.Errorf("unsupported response version %q", toks[0])
	}
	fields, err := parseTapFDFields(toks[2:])
	if err != nil {
		return nil, err
	}
	switch toks[1] {
	case "OK":
		return fields, nil
	case "ERR":
		code := fields["code"]
		if code == "" {
			code = "PROVIDER_INTERNAL"
		}
		return nil, &tapFDProviderError{code: code, message: fields["message"]}
	default:
		return nil, fmt.Errorf("unsupported response status %q", toks[1])
	}
}

func parseTapFDFields(toks []string) (map[string]string, error) {
	fields := make(map[string]string, len(toks))
	for _, tok := range toks {
		key, val, ok := strings.Cut(tok, "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("malformed response token %q", tok)
		}
		fields[key] = val
	}
	return fields, nil
}
