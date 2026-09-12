package telemetry

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"time"
)

const maxRemoteBytes = 32 << 20

// All destinations come from trusted component configuration, never requests
// or RouteEntry payloads. A backend owns its bounded HTTP connection pool.
type remoteClient struct {
	client  *http.Client
	headers map[string]string
}

func newRemoteClient(headers map[string]string) *remoteClient {
	return &remoteClient{headers: maps.Clone(headers), client: &http.Client{
		Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Transport: &http.Transport{Proxy: http.ProxyFromEnvironment, DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}, TLSHandshakeTimeout: 5 * time.Second, MaxConnsPerHost: 8, MaxIdleConns: 8, MaxIdleConnsPerHost: 8, IdleConnTimeout: 30 * time.Second, DisableCompression: true},
	}}
}

// open transfers the successful response body to the caller. The HTTP client's
// deadline still covers reading it, including streamed remote-read responses.
func (c *remoteClient) open(ctx context.Context, endpoint string, body []byte, protocolHeaders map[string]string) (*http.Response, error) {
	if len(body) > maxRemoteBytes {
		return nil, ErrInvalidMetrics
	}
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	for key, value := range c.headers {
		req.Header.Set(key, value)
	}
	for key, value := range protocolHeaders {
		req.Header.Set(key, value)
	}
	response, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	if exception := response.Header.Get("X-ClickHouse-Exception-Code"); response.StatusCode < 200 || response.StatusCode >= 300 || (exception != "" && exception != "0") {
		response.Body.Close()
		// Do not reflect database diagnostics, query text or credentials to guests.
		return nil, fmt.Errorf("telemetry backend HTTP status %d", response.StatusCode)
	}
	return response, nil
}
func (c *remoteClient) request(ctx context.Context, endpoint string, body []byte, protocolHeaders map[string]string) ([]byte, error) {
	response, err := c.open(ctx, endpoint, body, protocolHeaders)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	return readRemoteBody(response.Body)
}
func readRemoteBody(body io.Reader) ([]byte, error) {
	raw, err := io.ReadAll(io.LimitReader(body, maxRemoteBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > maxRemoteBytes {
		return nil, fmt.Errorf("telemetry backend response exceeds limit")
	}
	return raw, nil
}
func (c *remoteClient) Shutdown(context.Context) error { c.client.CloseIdleConnections(); return nil }
