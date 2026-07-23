package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
)

type clusterHTTPServer struct {
	name     string
	addr     string
	useTLS   bool
	server   *http.Server
	listener net.Listener
}

const clusterReadHeaderTimeout = 10 * time.Second

func newClusterHTTPServer(name, addr string, tlsMaterial clustercfg.TLS, handler http.Handler) (*clusterHTTPServer, error) {
	if strings.HasPrefix(addr, "/") && tlsMaterial.Enabled() {
		return nil, fmt.Errorf("cluster: %s certificate-authenticated listener requires TCP", name)
	}
	listener, err := listenClusterEndpoint(addr)
	if err != nil {
		return nil, fmt.Errorf("cluster: %s listen %s: %w", name, addr, err)
	}
	server := &http.Server{
		Handler: h2c.NewHandler(handler, &http2.Server{}), ReadHeaderTimeout: clusterReadHeaderTimeout,
	}
	useTLS := tlsMaterial.Enabled()
	if useTLS {
		tlsConfig, err := tlsMaterial.ServerConfig()
		if err != nil {
			_ = listener.Close()
			return nil, fmt.Errorf("cluster: %s tls: %w", name, err)
		}
		server = &http.Server{Handler: handler, TLSConfig: tlsConfig, ReadHeaderTimeout: clusterReadHeaderTimeout}
	}
	return &clusterHTTPServer{
		name: name, addr: addr, useTLS: useTLS, server: server, listener: listener,
	}, nil
}

func (s *clusterHTTPServer) Serve(ctx context.Context, log *slog.Logger) error {
	go func() {
		<-ctx.Done()
		_ = s.server.Close()
	}()
	log.Info("cluster listener", "name", s.name, "listen", s.addr, "tls", s.useTLS)
	var err error
	if s.useTLS {
		err = s.server.ServeTLS(s.listener, "", "")
	} else {
		err = s.server.Serve(s.listener)
	}
	if err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func listenClusterEndpoint(addr string) (net.Listener, error) {
	if !strings.HasPrefix(addr, "/") {
		return net.Listen("tcp", addr)
	}
	info, err := os.Lstat(addr)
	if err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("cluster: Unix listener path exists and is not a socket")
		}
		if err := os.Remove(addr); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if directory := addr[:strings.LastIndexByte(addr, '/')]; directory != "" {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return nil, err
		}
	}
	listener, err := net.Listen("unix", addr)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(addr, 0o600); err != nil {
		_ = listener.Close()
		_ = os.Remove(addr)
		return nil, err
	}
	return listener, nil
}
