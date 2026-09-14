package vswitch

import (
	"context"
	"fmt"

	connector "github.com/kuasar-sandbox/connector/pkg/vswitch"
)

// Stats reads the current switch's pinned maps through connector's existing
// batched API. It starts no CLI process and keeps no second port-owner table.
// Connector's nonblocking shared control lock bounds ownership contention.
func (c *CLI) Stats(ctx context.Context, ports []int) (*connector.StatsOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(ports) == 0 || len(ports) > 64 {
		return nil, fmt.Errorf("connector stats requires 1..64 ports")
	}
	result, err := connector.Stats(c.sw, ports)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
