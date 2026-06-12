package main

// Minimal Connect+JSON client for envd's process.Process/Start — the e2b
// exec channel the build pipeline drives steps / startCmd / readyCmd
// through, mirroring e2b's own template build (e2b-infra
// packages/orchestrator/pkg/template/build/sandboxtools/command.go):
// `/bin/bash -l -c <cmd>`, the per-op user in Basic auth, envs/cwd from
// the accumulated build context, timeouts via Connect-Timeout-Ms (envd
// kills at the deadline; a DROPPED stream never kills — the process
// stays envd-managed, which is exactly what a template snapshot must
// freeze). Envelopes are hand-rolled — [1B flags][4B big-endian
// len][JSON], flags bit 0x02 = end-of-stream message — keeping the repo
// free of gRPC/protobuf.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type envdExec struct {
	uds   string
	token string // X-Access-Token, once /init armed envd (mmds posture)
	log   *slog.Logger
}

// request opens one process.Start stream. timeout > 0 rides
// Connect-Timeout-Ms (guest-side kill at the deadline); 0 = unbounded
// (start commands must outlive the build).
func (e *envdExec) request(ctx context.Context, user, cwd string, env map[string]string, cmd string, timeout time.Duration) (*http.Response, error) {
	proc := map[string]any{
		"cmd":  "/bin/bash",
		"args": []string{"-l", "-c", cmd},
	}
	if cwd != "" {
		proc["cwd"] = cwd
	}
	if len(env) > 0 {
		envs := make(map[string]string, len(env))
		for k, v := range env {
			envs[k] = v
		}
		// Mirror e2b: keep utilities findable even if a step broke PATH.
		if v, ok := envs["PATH"]; ok {
			envs["PATH"] = v + ":/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
		}
		proc["envs"] = envs
	}
	body, err := json.Marshal(map[string]any{"process": proc})
	if err != nil {
		return nil, err
	}
	frame := make([]byte, 5+len(body))
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(body)))
	copy(frame[5:], body)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://envd/process.Process/Start", bytes.NewReader(frame))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/connect+json")
	req.Header.Set("Connect-Protocol-Version", "1")
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(user+":")))
	if e.token != "" {
		req.Header.Set("X-Access-Token", e.token)
	}
	if timeout > 0 {
		req.Header.Set("Connect-Timeout-Ms", strconv.FormatInt(timeout.Milliseconds(), 10))
	}
	// No client timeout: the response is a stream held for the command's
	// lifetime; cancellation comes from ctx.
	cl := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", e.uds)
			},
		},
	}
	resp, err := cl.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("envd: process.Start: status %d: %s", resp.StatusCode, firstLine(msg))
	}
	return resp, nil
}

// processEvent / endStream are the proto3-JSON shapes of envd's stream.
// exitCode is omitted when zero (proto3 JSON), so success end events may
// carry only {"exited":true,...}.
type processEvent struct {
	Event struct {
		Start *struct {
			Pid uint32 `json:"pid"`
		} `json:"start"`
		Data *struct {
			Stdout string `json:"stdout"`
			Stderr string `json:"stderr"`
		} `json:"data"`
		End *struct {
			ExitCode int32   `json:"exitCode"`
			Exited   bool    `json:"exited"`
			Status   string  `json:"status"`
			Error    *string `json:"error"`
		} `json:"end"`
	} `json:"event"`
}

type endStream struct {
	Error *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type envdFrame struct {
	end bool
	msg []byte
}

func readFrame(r io.Reader) (envdFrame, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return envdFrame{}, err
	}
	n := binary.BigEndian.Uint32(hdr[1:5])
	if n > 32<<20 {
		return envdFrame{}, fmt.Errorf("envd: oversized frame (%d bytes)", n)
	}
	msg := make([]byte, n)
	if _, err := io.ReadFull(r, msg); err != nil {
		return envdFrame{}, err
	}
	return envdFrame{end: hdr[0]&0x02 != 0, msg: msg}, nil
}

// run executes cmd through envd and waits for its end event. Non-zero
// exit (or a stream error) returns an error carrying the output tail.
// quiet suppresses live output forwarding (readiness polls).
func (e *envdExec) run(ctx context.Context, user, cwd string, env map[string]string, cmd string, timeout time.Duration, quiet bool) error {
	if timeout > 0 {
		// Host-side guard slightly beyond the guest-side deadline, so the
		// end event (guest kill) normally wins and carries the better error.
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout+15*time.Second)
		defer cancel()
	}
	resp, err := e.request(ctx, user, cwd, env, cmd, timeout)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var tail bytes.Buffer
	exit, exited := int32(0), false
	for {
		f, err := readFrame(resp.Body)
		if err != nil {
			return fmt.Errorf("envd: stream: %w", err)
		}
		if f.end {
			var es endStream
			if json.Unmarshal(f.msg, &es) == nil && es.Error != nil {
				return fmt.Errorf("envd: %s: %s", es.Error.Code, es.Error.Message)
			}
			break
		}
		var ev processEvent
		if err := json.Unmarshal(f.msg, &ev); err != nil {
			continue
		}
		switch {
		case ev.Event.Data != nil:
			forwardOutput(ev.Event.Data.Stdout, &tail, quiet)
			forwardOutput(ev.Event.Data.Stderr, &tail, quiet)
		case ev.Event.End != nil:
			exit, exited = ev.Event.End.ExitCode, true
			if ev.Event.End.Error != nil {
				return fmt.Errorf("envd: %s (%s)", *ev.Event.End.Error, lastLine(tail.Bytes()))
			}
		}
	}
	if !exited {
		return fmt.Errorf("envd: stream ended before the command exited")
	}
	if exit != 0 {
		return fmt.Errorf("exit status %d (%s)", exit, lastLine(tail.Bytes()))
	}
	return nil
}

// startedCmd is a start command held open the way e2b's template build
// holds it: stream alive while readiness is probed, then cancelled —
// envd keeps the process (it is killed only by its own exit).
type startedCmd struct {
	cancel context.CancelFunc
	done   chan struct{}

	mu      sync.Mutex
	exitErr error // non-zero exit observed while the stream was held
}

// start launches cmd and returns once envd reports the start event.
func (e *envdExec) start(ctx context.Context, user, cwd string, env map[string]string, cmd string) (*startedCmd, error) {
	sctx, cancel := context.WithCancel(ctx)
	resp, err := e.request(sctx, user, cwd, env, cmd, 0)
	if err != nil {
		cancel()
		return nil, err
	}
	for {
		f, err := readFrame(resp.Body)
		if err != nil {
			cancel()
			resp.Body.Close()
			return nil, fmt.Errorf("envd: before start event: %w", err)
		}
		if f.end {
			var es endStream
			msg := "stream closed before the start event"
			if json.Unmarshal(f.msg, &es) == nil && es.Error != nil {
				msg = es.Error.Code + ": " + es.Error.Message
			}
			cancel()
			resp.Body.Close()
			return nil, fmt.Errorf("envd: %s", msg)
		}
		var ev processEvent
		if json.Unmarshal(f.msg, &ev) == nil && ev.Event.Start != nil {
			e.log.Info("envd: start command running", "pid", ev.Event.Start.Pid)
			break
		}
	}

	sc := &startedCmd{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(sc.done)
		defer resp.Body.Close()
		var tail bytes.Buffer
		for {
			f, err := readFrame(resp.Body)
			if err != nil || f.end {
				return
			}
			var ev processEvent
			if json.Unmarshal(f.msg, &ev) != nil {
				continue
			}
			switch {
			case ev.Event.Data != nil:
				forwardOutput(ev.Event.Data.Stdout, &tail, false)
				forwardOutput(ev.Event.Data.Stderr, &tail, false)
			case ev.Event.End != nil:
				// An early exit is fine when clean (one-shot start commands);
				// a failure must fail the build (e2b does the same).
				if ev.Event.End.ExitCode != 0 {
					sc.mu.Lock()
					sc.exitErr = fmt.Errorf("start command exited: status %d (%s)",
						ev.Event.End.ExitCode, lastLine(tail.Bytes()))
					sc.mu.Unlock()
				}
			}
		}
	}()
	return sc, nil
}

// stop releases the stream (the process lives on under envd) and
// reports a failure exit observed while it was held.
func (sc *startedCmd) stop() error {
	sc.cancel()
	<-sc.done
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.exitErr
}

// forwardOutput decodes one base64 data chunk, mirrors it to stderr
// (the journal) unless quiet, and keeps a bounded tail for errors.
func forwardOutput(b64 string, tail *bytes.Buffer, quiet bool) {
	if b64 == "" {
		return
	}
	data, err := base64.StdEncoding.DecodeString(b64)
	if err != nil || len(data) == 0 {
		return
	}
	if !quiet {
		_, _ = os.Stderr.Write(data)
	}
	if tail.Len() > 4096 {
		tail.Reset()
	}
	tail.Write(data)
}

func lastLine(b []byte) string {
	s := strings.TrimSpace(string(b))
	if i := strings.LastIndexByte(s, '\n'); i >= 0 {
		s = s[i+1:]
	}
	if len(s) > 200 {
		s = s[len(s)-200:]
	}
	return s
}
