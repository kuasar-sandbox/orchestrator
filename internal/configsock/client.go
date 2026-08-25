package configsock

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
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
	body, _ := json.Marshal(SandboxTaskRequest{SandboxID: sandboxID, RunID: runID, Version: SnapshotPrepareSchemaVersion})
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
func CompleteSandboxPrepare(ctx context.Context, socket, sandboxID, runID string, summary SnapshotPrepareSummary) (*LaunchSpec, error) {
	body, _ := json.Marshal(SnapshotPrepareRequest{SandboxID: sandboxID, RunID: runID, Summary: summary})
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
	var out SnapshotPrepareResponse
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
func CompleteBuildPrepare(ctx context.Context, socket, buildID, runID string, summary SnapshotPrepareSummary) (*BuildSpec, error) {
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
