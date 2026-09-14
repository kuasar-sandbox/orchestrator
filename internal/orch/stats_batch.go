package orch

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"golang.org/x/sync/errgroup"
)

// ReadStats is the bounded trusted adaptation of the same native reads used by
// the public single-sandbox APIs. Only the plugin lease or an in-process Host
// may reach this method without the public API's per-sandbox ownership check.
func (o *Orchestrator) ReadStats(ctx context.Context, request conductorextension.StatsRequest) ([]conductorextension.SandboxStats, error) {
	if len(request.SandboxIDs) < 1 || len(request.SandboxIDs) > conductorextension.MaxStatsSandboxes || len(request.Sections) < 1 || len(request.Sections) > 3 {
		return nil, api.ErrBadRequest
	}
	sections, ids := map[string]bool{}, map[string]bool{}
	for _, section := range request.Sections {
		if sections[section] || (section != "resource" && section != "traffic" && section != "usage") {
			return nil, api.ErrBadRequest
		}
		sections[section] = true
	}
	for _, id := range request.SandboxIDs {
		if id == "" || ids[id] {
			return nil, api.ErrBadRequest
		}
		ids[id] = true
	}
	if _, err := request.Usage.Normalize(); err != nil || (!sections["usage"] && request.Usage != (conductorextension.UsageQuery{})) {
		return nil, api.ErrBadRequest
	}
	ctx, cancel := context.WithTimeout(ctx, conductorextension.StatsTimeout)
	defer cancel()
	var sandboxes []*types.Sandbox
	var network []trafficNetwork
	var ingress []*api.TrafficStats
	if sections["traffic"] {
		sandboxes = make([]*types.Sandbox, len(request.SandboxIDs))
		for i, id := range request.SandboxIDs {
			sb, err := o.st.Get(ctx, id)
			if err != nil {
				if ctx.Err() != nil {
					return nil, fmt.Errorf("%w: %v", api.ErrStatsUnavailable, ctx.Err())
				}
				return nil, err
			}
			if sb == nil {
				return nil, api.ErrNotFound
			}
			sandboxes[i] = sb
		}
		var err error
		network, ingress, err = o.prepareTrafficReads(ctx, sandboxes)
		if err != nil {
			if ctx.Err() != nil {
				return nil, fmt.Errorf("%w: %v", api.ErrStatsUnavailable, ctx.Err())
			}
			return nil, err
		}
	}
	group, readCtx := errgroup.WithContext(ctx)
	group.SetLimit(conductorextension.MaxStatsConcurrency)
	result := make([]conductorextension.SandboxStats, len(request.SandboxIDs))
	var bytes atomic.Int64
	bytes.Store(int64(len(result) + 2)) // JSON array, separators and final newline.
	for i, id := range request.SandboxIDs {
		if readCtx.Err() != nil {
			break
		}
		group.Go(func() error {
			select {
			case o.nativeStatsSlots <- struct{}{}:
				defer func() { <-o.nativeStatsSlots }()
			case <-readCtx.Done():
				return readCtx.Err()
			}
			var sb *types.Sandbox
			var err error
			if sandboxes != nil {
				sb = sandboxes[i]
			} else {
				sb, err = o.st.Get(readCtx, id)
			}
			if err != nil {
				return err
			}
			if sb == nil {
				return api.ErrNotFound
			}
			row := conductorextension.SandboxStats{SandboxID: sb.ID, StableID: sb.StableID()}
			if sections["resource"] {
				row.Resource, err = o.readResourceStats(readCtx, sb)
			}
			if err == nil && sections["traffic"] {
				if ingress != nil {
					row.Traffic = ingress[i]
				} else {
					row.Traffic, err = o.readIngressStats(readCtx, sb)
				}
				if err == nil {
					row.Traffic = combineTraffic(row.Traffic, network[i])
				}
			}
			if err == nil && sections["usage"] {
				row.Usage, err = o.readUsageStats(readCtx, sb, request.Usage)
			}
			if err != nil {
				return err
			}
			if err := o.statsBindingCurrent(readCtx, sb); err != nil {
				return err
			}
			encoded, err := json.Marshal(row)
			if err != nil {
				return err
			}
			if bytes.Add(int64(len(encoded))) > conductorextension.MaxStatsResponseBytes {
				return fmt.Errorf("%w: native stats batch response too large", api.ErrStatsUnavailable)
			}
			result[i] = row
			return nil
		})
	}
	err := group.Wait()
	if ctx.Err() != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrStatsUnavailable, ctx.Err())
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}
