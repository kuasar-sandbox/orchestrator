package mmdssvc

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestUnixSocketPathStrictEndpoint(t *testing.T) {
	for _, endpoint := range []string{
		"https://example.test/service",
		"unix://host/run/service.sock",
		"unix://relative.sock",
		"unix:///run/service.sock?query=1",
		"unix:///run/service.sock?",
		"unix:///run/%73ervice.sock",
		"unix:///run/service.sock#fragment",
	} {
		if _, err := UnixSocketPath(endpoint); err == nil {
			t.Errorf("accepted invalid Unix service endpoint")
		}
	}
	if got, err := UnixSocketPath("unix:///run/kuasar/mmds/service.sock"); err != nil || got != "/run/kuasar/mmds/service.sock" {
		t.Fatalf("valid Unix service endpoint rejected: err=%v", err)
	}
}

func TestCallConstructsFixedRequest(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "service.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	errCh := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()
		req, err := http.ReadRequest(bufio.NewReader(conn))
		if err != nil {
			errCh <- err
			return
		}
		body, err := io.ReadAll(req.Body)
		if err != nil {
			errCh <- err
			return
		}
		if req.Method != http.MethodGet || req.RequestURI != "/latest/meta-data/credentials" || req.Host != "mmds-service" {
			errCh <- fmt.Errorf("unexpected request: method=%q uri=%q host=%q", req.Method, req.RequestURI, req.Host)
			return
		}
		if len(body) != 0 {
			errCh <- fmt.Errorf("request body = %q", body)
			return
		}
		want := http.Header{
			HeaderSandboxID: []string{"sandbox-1"},
			HeaderService:   []string{"external-mmds"},
		}
		if len(req.Header) != len(want) {
			errCh <- fmt.Errorf("request headers = %#v", req.Header)
			return
		}
		for name, values := range want {
			if got := req.Header.Values(name); len(got) != 1 || got[0] != values[0] {
				errCh <- fmt.Errorf("header %s = %#v", name, got)
				return
			}
		}
		_, err = io.WriteString(conn, "HTTP/1.1 201 Created\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: 2\r\n\r\n{}")
		errCh <- err
	}()

	result := Call(context.Background(), socket, "external-mmds", "/latest/meta-data/credentials", "sandbox-1")
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if result.StatusCode != http.StatusCreated || result.ContentType != "application/json; charset=utf-8" || string(result.Body) != "{}" {
		t.Fatal("result metadata or body mismatch")
	}
}

func TestCallFailures(t *testing.T) {
	t.Run("missing service", func(t *testing.T) {
		got := Call(context.Background(), "", "missing", "/data", "sandbox-1")
		if got.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("status = %d", got.StatusCode)
		}
	})

	t.Run("socket unavailable", func(t *testing.T) {
		got := Call(context.Background(), filepath.Join(t.TempDir(), "missing.sock"), "svc", "/data", "sandbox-1")
		if got.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("status = %d", got.StatusCode)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		socket := filepath.Join(t.TempDir(), "service.sock")
		ln, err := net.Listen("unix", socket)
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()
		go func() {
			conn, err := ln.Accept()
			if err == nil {
				defer conn.Close()
				time.Sleep(requestTimeout + 250*time.Millisecond)
			}
		}()
		got := Call(context.Background(), socket, "svc", "/data", "sandbox-1")
		if got.StatusCode != http.StatusGatewayTimeout {
			t.Fatalf("status = %d", got.StatusCode)
		}
	})
}

func TestCallResponsePolicy(t *testing.T) {
	tests := []struct {
		name       string
		response   string
		wantStatus int
		wantType   string
		wantBody   string
	}{
		{name: "missing content type", response: "HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nx", wantStatus: 200, wantType: "text/plain", wantBody: "x"},
		{name: "invalid content type", response: "HTTP/1.1 200 OK\r\nContent-Type: invalid/\r\nContent-Length: 1\r\n\r\nx", wantStatus: 502, wantType: "text/plain"},
		{name: "duplicate content type", response: "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Type: application/json\r\nContent-Length: 1\r\n\r\nx", wantStatus: 502, wantType: "text/plain"},
		{name: "oversized", response: fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: %d\r\n\r\n%s", maxResponseBytes+1, strings.Repeat("x", maxResponseBytes+1)), wantStatus: 502, wantType: "text/plain"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := callRawResponse(t, tt.response, nil)
			if got.StatusCode != tt.wantStatus || got.ContentType != tt.wantType || string(got.Body) != tt.wantBody {
				t.Fatal("response content metadata mismatch")
			}
		})
	}
}

func TestCallDoesNotFollowRedirect(t *testing.T) {
	var requests atomic.Int32
	got := callRawResponse(t, "HTTP/1.1 302 Found\r\nLocation: /follow\r\nContent-Length: 0\r\n\r\n", func() {
		requests.Add(1)
	})
	if got.StatusCode != http.StatusFound {
		t.Fatalf("status = %d", got.StatusCode)
	}
	if requests.Load() != 1 {
		t.Fatalf("requests = %d", requests.Load())
	}
}

func callRawResponse(t *testing.T, response string, onRequest func()) Result {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "service.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	errCh := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()
		if _, err := http.ReadRequest(bufio.NewReader(conn)); err != nil {
			errCh <- err
			return
		}
		if onRequest != nil {
			onRequest()
		}
		_, err = io.WriteString(conn, response)
		errCh <- err
	}()
	got := Call(context.Background(), socket, "svc", "/data", "sandbox-1")
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	return got
}
