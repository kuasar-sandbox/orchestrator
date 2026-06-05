package configsock

import (
	"errors"
	"net"
	"time"
)

// FetchLaunchSpec dials the config-socket and pulls the LaunchSpec for configID.
// The caller (orchestrator-ctl run-task) must have written its pidfile first so
// the server's SO_PEERCRED check matches the connecting pid.
func FetchLaunchSpec(socket, configID string) (*LaunchSpec, error) {
	c, err := net.DialTimeout("unix", socket, 5*time.Second)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	if err := writeFrame(c, Request{ConfigID: configID}); err != nil {
		return nil, err
	}
	resp, err := readFrame[LaunchSpec](c)
	if err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, errors.New(resp.Error)
	}
	return resp, nil
}
