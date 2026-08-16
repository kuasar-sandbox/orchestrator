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
		return "", err
	}
	defer resp.Body.Close()
	var out AssignmentResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("configsock: decode assignment: %w", err)
	}
	if out.Error != "" {
		return "", errors.New(out.Error)
	}
	if out.TaskID == "" {
		return "", errors.New("empty assignment")
	}
	return out.TaskID, nil
}

func PostBuildResult(socket, runID, buildID string, result BuildResult) error {
	body, _ := json.Marshal(BuildResultRequest{RunID: runID, BuildID: buildID, Result: result})
	req, err := http.NewRequest(http.MethodPost, "http://localhost"+PathRunBuildResult, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := HTTPClient(socket).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var out BuildResultResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fmt.Errorf("configsock: decode build result: %w", err)
	}
	if out.Error != "" {
		return errors.New(out.Error)
	}
	return nil
}

func PostBuildPhase(socket, runID, buildID, phase, sandboxID, state string) error {
	body, _ := json.Marshal(BuildPhaseRequest{
		RunID: runID, BuildID: buildID, Phase: phase, SandboxID: sandboxID, State: state,
	})
	req, err := http.NewRequest(http.MethodPost, "http://localhost"+PathRunBuildPhase, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := HTTPClient(socket).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var out BuildPhaseResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fmt.Errorf("configsock: decode build phase: %w", err)
	}
	if out.Error != "" {
		return errors.New(out.Error)
	}
	return nil
}

// FetchLaunchSpec dials the config-socket and pulls the LaunchSpec for configID.
// The caller (node-ctl run-sandbox / run-builder) must have written its pidfile first so the
// server's SO_PEERCRED check matches the connecting pid.
func FetchLaunchSpec(socket, configID string) (*LaunchSpec, error) {
	body, _ := json.Marshal(Request{ConfigID: configID})
	req, err := http.NewRequest(http.MethodPost, "http://localhost"+PathTaskLaunchSpec, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := HTTPClient(socket).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var spec LaunchSpec
	if err := json.NewDecoder(resp.Body).Decode(&spec); err != nil {
		return nil, fmt.Errorf("configsock: decode launchspec: %w", err)
	}
	if spec.Error != "" {
		return nil, errors.New(spec.Error)
	}
	return &spec, nil
}

// FetchBuildSpec dials the config-socket and pulls the BuildSpec for
// configID ("build:<bid>"). Same auth contract as FetchLaunchSpec.
func FetchBuildSpec(socket, configID string) (*BuildSpec, error) {
	body, _ := json.Marshal(Request{ConfigID: configID})
	req, err := http.NewRequest(http.MethodPost, "http://localhost"+PathTaskBuildSpec, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := HTTPClient(socket).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var spec BuildSpec
	if err := json.NewDecoder(resp.Body).Decode(&spec); err != nil {
		return nil, fmt.Errorf("configsock: decode buildspec: %w", err)
	}
	if spec.Error != "" {
		return nil, errors.New(spec.Error)
	}
	return &spec, nil
}
