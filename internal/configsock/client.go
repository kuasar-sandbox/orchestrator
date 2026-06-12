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
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", socket)
			},
		},
	}
}

// FetchLaunchSpec dials the config-socket and pulls the LaunchSpec for configID.
// The caller (orchestrator-ctl run-sandbox / run-builder) must have written its pidfile first so the
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
