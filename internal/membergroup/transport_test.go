package membergroup

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestStreamClientsShareTLSSettingsWithoutMutatingConfig(t *testing.T) {
	for _, tc := range []struct {
		name        string
		waitForDial bool
	}{
		{name: "handshake_and_initialization", waitForDial: true},
		{name: "concurrent_initializations"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			}))
			defer srv.Close()
			pool := x509.NewCertPool()
			pool.AddCert(srv.Certificate())
			protocols := []string{"h2", "reserved"}
			cfg := &tls.Config{RootCAs: pool, NextProtos: protocols[:1]}
			a, b := newStreamClient(cfg), newStreamClient(cfg)
			defer a.CloseIdleConnections()
			defer b.CloseIdleConnections()
			testCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			ready, start := make(chan struct{}), make(chan struct{})
			if tc.waitForDial {
				// HTTP/2 defaults are initialized before Dial. Release the first TLS
				// handshake together with the other Transport's initialization.
				var readyOnce sync.Once
				tr := a.Transport.(*http.Transport)
				tr.DialContext = func(_ context.Context, network, addr string) (net.Conn, error) {
					conn, err := (&net.Dialer{}).DialContext(testCtx, network, addr)
					readyOnce.Do(func() { close(ready) })
					select {
					case <-start:
					case <-testCtx.Done():
						if conn != nil {
							_ = conn.Close()
						}
						return nil, testCtx.Err()
					}
					return conn, err
				}
			}
			request := func(c *http.Client) error {
				req, err := http.NewRequestWithContext(testCtx, http.MethodGet, srv.URL, nil)
				if err != nil {
					return err
				}
				resp, err := c.Do(req)
				if err != nil {
					return err
				}
				_, err = io.Copy(io.Discard, resp.Body)
				closeErr := resp.Body.Close()
				if err != nil {
					return err
				}
				return closeErr
			}
			done := make(chan error, 2)
			if tc.waitForDial {
				go func() { done <- request(a) }()
				select {
				case <-ready:
				case <-testCtx.Done():
					t.Fatal(testCtx.Err())
				}
			} else {
				// Both initializations may append to the protocol list's spare capacity.
				go func() { <-start; done <- request(a) }()
			}
			go func() { <-start; done <- request(b) }()
			close(start)
			for range 2 {
				if err := <-done; err != nil {
					t.Error(err)
				}
			}
			if !slices.Equal(cfg.NextProtos, []string{"h2"}) {
				t.Errorf("caller TLS protocols changed to %v", cfg.NextProtos)
			}
			if !slices.Equal(protocols, []string{"h2", "reserved"}) {
				t.Errorf("caller TLS protocol backing array changed to %v", protocols)
			}
		})
	}
}
