package orch

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/usagereader"
)

func (o *Orchestrator) UsageStats(ctx context.Context, id, apiKey string, query conductorextension.UsageQuery) (json.RawMessage, error) {
	sb, err := o.st.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if !ownsSandbox(sb, apiKey) {
		return nil, api.ErrNotFound
	}
	return o.readUsageStats(ctx, sb, query)
}

func (o *Orchestrator) readUsageStats(ctx context.Context, sb *types.Sandbox, query conductorextension.UsageQuery) (json.RawMessage, error) {
	query, err := query.Normalize()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	if sb.BaseDir == "" {
		return nil, api.ErrStatsUnavailable
	}
	options := usagereader.Options{SandboxID: sb.ID, File: filepath.Join(sb.BaseDir, sb.ID+".usage"),
		Saved: query.View == "saved", History: query.View == "history", Cursor: query.Cursor, Limit: query.Limit}
	if options.Limit == 0 {
		options.Limit = 10
	}
	if sb.RunDir == "" {
		options.Offline = true
	} else {
		options.ControlSocket = filepath.Join(sb.RunDir, "ctl.sock")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	body, err := usagereader.Read(ctx, options)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrStatsUnavailable, err)
	}
	if err := o.statsBindingCurrent(ctx, sb); err != nil {
		return nil, err
	}
	return body, nil
}
