package configsock

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
)

// ReadNativeStats uses only conductor's existing config socket. It has no
// sandbox ctl/file access or independent sandbox-to-runtime binding map.
func ReadNativeStats(ctx context.Context, socket string, query conductorextension.StatsRequest) ([]conductorextension.SandboxStats, error) {
	body, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}
	if len(body) > 64<<10 {
		return nil, fmt.Errorf("native stats request too large")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost"+PathTelemetryStats, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := HTTPClientWithTimeout(socket, conductorextension.StatsTimeout).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("conductor native stats: HTTP %d", resp.StatusCode)
	}
	body, err = io.ReadAll(io.LimitReader(resp.Body, conductorextension.MaxStatsResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > conductorextension.MaxStatsResponseBytes {
		return nil, fmt.Errorf("conductor native stats response too large")
	}
	var rows []conductorextension.SandboxStats
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, err
	}
	if len(rows) != len(query.SandboxIDs) {
		return nil, fmt.Errorf("conductor native stats response has missing objects")
	}
	for i, row := range rows {
		if row.SandboxID != query.SandboxIDs[i] {
			return nil, fmt.Errorf("conductor native stats response has different SandboxID")
		}
		for _, section := range query.Sections {
			if (section == "resource" && row.Resource == nil) || (section == "traffic" && row.Traffic == nil) ||
				(section == "usage" && (len(row.Usage) == 0 || bytes.Equal(bytes.TrimSpace(row.Usage), []byte("null")))) {
				return nil, fmt.Errorf("conductor native stats response has missing %s", section)
			}
		}
	}
	return rows, nil
}
