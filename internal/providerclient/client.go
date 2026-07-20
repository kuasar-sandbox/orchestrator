package providerclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/kuasar-sandbox/orchestrator/internal/placer"
)

type Endpoint struct {
	Name    string
	BaseURL string
	Client  *http.Client
}

type Client struct {
	endpoints []Endpoint
	next      atomic.Uint64
}

func New(endpoints []Endpoint) (*Client, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("providerclient: at least one Provider endpoint is required")
	}
	seen := make(map[string]struct{}, len(endpoints))
	copyEndpoints := make([]Endpoint, len(endpoints))
	for index, endpoint := range endpoints {
		endpoint.BaseURL = strings.TrimRight(endpoint.BaseURL, "/")
		if endpoint.Name == "" || endpoint.BaseURL == "" || endpoint.Client == nil {
			return nil, errors.New("providerclient: incomplete endpoint")
		}
		if _, duplicate := seen[endpoint.Name]; duplicate {
			return nil, errors.New("providerclient: duplicate endpoint name")
		}
		seen[endpoint.Name] = struct{}{}
		copyEndpoints[index] = endpoint
	}
	return &Client{endpoints: copyEndpoints}, nil
}

func (c *Client) Verify(ctx context.Context, group, apiKey string) (bool, error) {
	if group == "" || apiKey == "" {
		return false, nil
	}
	input, err := json.Marshal(placer.VerifyKeyRequest{Group: group, APIKey: apiKey})
	if err != nil {
		return false, err
	}
	start := int(c.next.Add(1)-1) % len(c.endpoints)
	var lastErr error
	for offset := range c.endpoints {
		endpoint := c.endpoints[(start+offset)%len(c.endpoints)]
		request, err := http.NewRequestWithContext(
			ctx, http.MethodPost, endpoint.BaseURL+placer.FinalVerifyKeyPath, bytes.NewReader(input),
		)
		if err != nil {
			return false, err
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := endpoint.Client.Do(request)
		if err != nil {
			lastErr = err
			continue
		}
		if response.StatusCode != http.StatusOK {
			_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
			response.Body.Close()
			lastErr = fmt.Errorf("providerclient: %s returned %s", endpoint.Name, response.Status)
			continue
		}
		var result placer.VerifyKeyResponse
		decoder := json.NewDecoder(io.LimitReader(response.Body, 4096))
		decoder.DisallowUnknownFields()
		err = decoder.Decode(&result)
		if err == nil {
			var trailing any
			if trailingErr := decoder.Decode(&trailing); !errors.Is(trailingErr, io.EOF) {
				err = errors.New("providerclient: response contains trailing data")
			}
		}
		response.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		return result.Authorized, nil
	}
	if lastErr == nil {
		lastErr = errors.New("providerclient: no Provider endpoint is available")
	}
	return false, lastErr
}
