package configsock

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"
)

// HTTPClient returns an http.Client whose transport dials the given UDS for every
// request (the host in the URL is ignored — use http://localhost/<path>). Shared by
// the run-sandbox / run-builder launchers (task plane) and the manifest-key /
// export-sandbox / import-sandbox CLIs.
func HTTPClient(socket string) *http.Client {
	return HTTPClientWithTimeout(socket, 30*time.Second)
}

func HTTPClientWithTimeout(socket string, timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			// Callers intentionally create a short-lived client per config-socket
			// operation. Do not retain an idle UDS connection in the otherwise
			// unreachable transport, especially while run-builder retries 5xx
			// responses from a restarting or temporarily unhealthy controller.
			DisableKeepAlives: true,
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", socket)
			},
		},
	}
}

type RunSession struct {
	body io.ReadCloser
	once sync.Once
	done chan error
}

func (s *RunSession) Close() error {
	if s == nil {
		return nil
	}
	var err error
	s.once.Do(func() { err = s.body.Close() })
	return err
}

func (s *RunSession) Done() <-chan error {
	if s == nil {
		ch := make(chan error)
		close(ch)
		return ch
	}
	return s.done
}

type RunSessionKeeper struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func OpenRunSessionKeeper(ctx context.Context, socket, kind, runID string) (*RunSessionKeeper, error) {
	first, err := OpenRunSession(ctx, socket, kind, runID)
	if err != nil {
		return nil, err
	}
	keeperCtx, cancel := context.WithCancel(ctx)
	k := &RunSessionKeeper{cancel: cancel, done: make(chan struct{})}
	go k.keep(keeperCtx, socket, kind, runID, first)
	return k, nil
}

func (k *RunSessionKeeper) Close() error {
	if k == nil {
		return nil
	}
	k.cancel()
	<-k.done
	return nil
}

func (k *RunSessionKeeper) keep(ctx context.Context, socket, kind, runID string, session *RunSession) {
	defer close(k.done)
	defer session.Close()
	delay := 20 * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return
		case <-session.Done():
		}
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			next, err := OpenRunSession(ctx, socket, kind, runID)
			if err == nil {
				_ = session.Close()
				session = next
				delay = 20 * time.Millisecond
				break
			}
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			if delay < time.Second {
				delay *= 2
				if delay > time.Second {
					delay = time.Second
				}
			}
		}
	}
}

func OpenRunSession(ctx context.Context, socket, kind, runID string) (*RunSession, error) {
	body, _ := json.Marshal(RunSessionRequest{Kind: kind, RunID: runID})
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, "http://localhost"+PathRunSession, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := HTTPClientWithTimeout(socket, 0).Do(req)
	if err != nil {
		return nil, &transportError{err: err}
	}
	var out RunSessionResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		_ = resp.Body.Close()
		return nil, &transportError{err: fmt.Errorf("configsock: decode run session: %w", err)}
	}
	if out.Error != "" || resp.StatusCode >= http.StatusBadRequest {
		_ = resp.Body.Close()
		return nil, buildResponseError(resp.StatusCode, out.Error)
	}
	if out.Kind != kind || out.RunID != runID {
		_ = resp.Body.Close()
		return nil, errors.New("configsock: run session identity mismatch")
	}
	session := &RunSession{body: resp.Body, done: make(chan error, 1)}
	go func() {
		_, err := io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			session.done <- &transportError{err: err}
			return
		}
		session.done <- nil
	}()
	return session, nil
}

func WaitAssignment(ctx context.Context, socket, kind, runID string) (string, error) {
	body, _ := json.Marshal(AssignmentRequest{Kind: kind, RunID: runID})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost"+PathRunAssignment, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := HTTPClientWithTimeout(socket, 0).Do(req)
	if err != nil {
		return "", &transportError{err: err}
	}
	defer resp.Body.Close()
	var out AssignmentResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", &transportError{err: fmt.Errorf("configsock: decode assignment: %w", err)}
	}
	if out.Error != "" || resp.StatusCode >= http.StatusBadRequest {
		return "", buildResponseError(resp.StatusCode, out.Error)
	}
	if out.TaskID == "" {
		return "", errors.New("empty assignment")
	}
	return out.TaskID, nil
}

type transportError struct{ err error }

func (e *transportError) Error() string { return e.err.Error() }
func (e *transportError) Unwrap() error { return e.err }

// IsTransportError identifies a request whose config-socket connection or
// response was interrupted. Idempotent run-builder operations may retry these;
// provider rejections remain ordinary errors and must not be retried.
func IsTransportError(err error) bool {
	var transport *transportError
	return errors.As(err, &transport)
}

type retryableResponseError struct {
	status int
	err    error
}

func (e *retryableResponseError) Error() string { return e.err.Error() }
func (e *retryableResponseError) Unwrap() error { return e.err }

// IsRetryableError identifies an idempotent config-socket operation that can be
// retried: either the connection/response was interrupted, or the server
// returned a 5xx response. A structured 4xx provider rejection is definitive.
func IsRetryableError(err error) bool {
	if IsTransportError(err) {
		return true
	}
	var response *retryableResponseError
	return errors.As(err, &response)
}

func buildResponseError(status int, message string) error {
	if message == "" {
		message = fmt.Sprintf("configsock: server returned HTTP %d", status)
	}
	err := errors.New(message)
	if status >= http.StatusInternalServerError {
		return &retryableResponseError{status: status, err: err}
	}
	return err
}

func PostSandboxResult(socket, runID, sandboxID string, result SandboxExecutionResult) error {
	return PostSandboxResultContext(context.Background(), socket, runID, sandboxID, result)
}

func PostSandboxResultContext(ctx context.Context, socket, runID, sandboxID string, result SandboxExecutionResult) error {
	body, _ := json.Marshal(SandboxResultRequest{RunID: runID, SandboxID: sandboxID, Result: result})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost"+PathRunSandboxResult, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := HTTPClient(socket).Do(req)
	if err != nil {
		return &transportError{err: err}
	}
	defer resp.Body.Close()
	var out SandboxResultResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return &transportError{err: fmt.Errorf("configsock: decode sandbox result: %w", err)}
	}
	if out.Error != "" || resp.StatusCode >= http.StatusBadRequest {
		return buildResponseError(resp.StatusCode, out.Error)
	}
	return nil
}

func PostBuildResult(socket, runID, buildID string, result BuildResult) error {
	return PostBuildResultContext(context.Background(), socket, runID, buildID, result)
}

func PostBuildResultContext(ctx context.Context, socket, runID, buildID string, result BuildResult) error {
	body, _ := json.Marshal(BuildResultRequest{RunID: runID, BuildID: buildID, Result: result})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost"+PathRunBuildResult, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := HTTPClient(socket).Do(req)
	if err != nil {
		return &transportError{err: err}
	}
	defer resp.Body.Close()
	var out BuildResultResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return &transportError{err: fmt.Errorf("configsock: decode build result: %w", err)}
	}
	if out.Error != "" || resp.StatusCode >= http.StatusBadRequest {
		return buildResponseError(resp.StatusCode, out.Error)
	}
	return nil
}

func PostBuildPhase(socket, runID, buildID, phase, sandboxID, state string) error {
	return PostBuildPhaseContext(context.Background(), socket, runID, buildID, phase, sandboxID, state)
}

func PostBuildPhaseContext(ctx context.Context, socket, runID, buildID, phase, sandboxID, state string) error {
	body, _ := json.Marshal(BuildPhaseRequest{
		RunID: runID, BuildID: buildID, Phase: phase, SandboxID: sandboxID, State: state,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost"+PathRunBuildPhase, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := HTTPClient(socket).Do(req)
	if err != nil {
		return &transportError{err: err}
	}
	defer resp.Body.Close()
	var out BuildPhaseResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return &transportError{err: fmt.Errorf("configsock: decode build phase: %w", err)}
	}
	if out.Error != "" || resp.StatusCode >= http.StatusBadRequest {
		return buildResponseError(resp.StatusCode, out.Error)
	}
	return nil
}

// FetchSandboxTaskSpec fetches one exact-run bootstrap after the caller has
// locked and written the assigned sandbox task pidfile.
func FetchSandboxTaskSpec(ctx context.Context, socket, sandboxID, runID string) (*SandboxTaskSpec, error) {
	body, _ := json.Marshal(SandboxTaskRequest{SandboxID: sandboxID, RunID: runID, Version: ArtifactPrepareSchemaVersion})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost"+PathTaskSandboxBootstrap, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := HTTPClientWithTimeout(socket, 0).Do(req)
	if err != nil {
		return nil, &transportError{err: err}
	}
	defer resp.Body.Close()
	var spec SandboxTaskSpec
	if err := json.NewDecoder(resp.Body).Decode(&spec); err != nil {
		return nil, &transportError{err: fmt.Errorf("configsock: decode sandbox bootstrap: %w", err)}
	}
	if spec.Error != "" || resp.StatusCode >= http.StatusBadRequest {
		return nil, buildResponseError(resp.StatusCode, spec.Error)
	}
	return &spec, nil
}

// CompleteSandboxPrepare submits an idempotent non-secret summary and waits for
// the final LaunchSpec. A transport interruption or 5xx is retryable with the
// same summary; a 409 is definitive.
func CompleteSandboxPrepare(ctx context.Context, socket, sandboxID, runID string, summary ArtifactPrepareSummary) (*LaunchSpec, error) {
	body, _ := json.Marshal(ArtifactPrepareRequest{SandboxID: sandboxID, RunID: runID, Summary: summary})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost"+PathTaskSandboxPrepare, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := HTTPClientWithTimeout(socket, 0).Do(req)
	if err != nil {
		return nil, &transportError{err: err}
	}
	defer resp.Body.Close()
	var out ArtifactPrepareResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, &transportError{err: fmt.Errorf("configsock: decode sandbox prepare response: %w", err)}
	}
	if out.Error != "" || resp.StatusCode >= http.StatusBadRequest {
		return nil, buildResponseError(resp.StatusCode, out.Error)
	}
	if out.Final == nil {
		return nil, errors.New("configsock: sandbox prepare response has no final launch spec")
	}
	return out.Final, nil
}

// FetchBuildTaskSpec fetches the auth-first exact-run bootstrap after the
// caller has locked the assigned build task pidfile.
func FetchBuildTaskSpec(ctx context.Context, socket, buildID, runID string) (*BuildTaskSpec, error) {
	body, _ := json.Marshal(BuildTaskRequest{BuildID: buildID, RunID: runID, Version: BuildTaskSchemaVersion})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost"+PathTaskBuildBootstrap, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := HTTPClientWithTimeout(socket, 0).Do(req)
	if err != nil {
		return nil, &transportError{err: err}
	}
	defer resp.Body.Close()
	var spec BuildTaskSpec
	if err := json.NewDecoder(resp.Body).Decode(&spec); err != nil {
		return nil, &transportError{err: fmt.Errorf("configsock: decode build bootstrap: %w", err)}
	}
	if spec.Error != "" || resp.StatusCode >= http.StatusBadRequest {
		return nil, buildResponseError(resp.StatusCode, spec.Error)
	}
	return &spec, nil
}

// CompleteBuildPrepare submits the immutable task-local root summary and waits
// for the final BuildSpec. Identical retries are safe after response loss or a
// conductor restart; conflicts return a non-retryable 409.
func CompleteBuildPrepare(ctx context.Context, socket, buildID, runID string, summary ArtifactPrepareSummary) (*BuildSpec, error) {
	body, _ := json.Marshal(BuildPrepareRequest{
		BuildID: buildID, RunID: runID, Version: BuildTaskSchemaVersion, Summary: summary,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost"+PathTaskBuildPrepare, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := HTTPClientWithTimeout(socket, 0).Do(req)
	if err != nil {
		return nil, &transportError{err: err}
	}
	defer resp.Body.Close()
	var out BuildPrepareResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, &transportError{err: fmt.Errorf("configsock: decode build prepare response: %w", err)}
	}
	if out.Error != "" || resp.StatusCode >= http.StatusBadRequest {
		return nil, buildResponseError(resp.StatusCode, out.Error)
	}
	if out.Final == nil {
		return nil, errors.New("configsock: build prepare response has no final build spec")
	}
	return out.Final, nil
}
