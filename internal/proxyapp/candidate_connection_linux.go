package proxyapp

import (
	"context"
	"fmt"
	"net"
	"time"
)

// connectCandidateConfig connects only B to the service's fixed conductor
// endpoint. SO_PEERCRED retains the original coordinator PID, so the existing
// PID allowlist still authorizes the connection, including after exec. B owns
// the resulting connection and performs its own registration and route sync.
func (s *Service) connectCandidateConfig(child *serviceChild, request uint64) {
	s.mu.Lock()
	socket := s.baseline.config.ConfigSocket
	current := s.child == child
	s.mu.Unlock()
	if !current {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	reply := serviceMessage{Kind: "config_connected", Request: request}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", socket)
	if err == nil {
		reply.file, err = conn.(*net.UnixConn).File()
		conn.Close()
	}
	if err != nil {
		reply.Error = err.Error()
	}
	defer closeFile(reply.file)
	_ = child.wire.send(reply)
}

// The subscriber serializes connection attempts. Correlation also rejects a
// late reply to a canceled attempt; every received file is closed or converted
// into the independent net.Conn owned by the subscriber.
func (s *Service) dialCandidateConfig(ctx context.Context) (net.Conn, error) {
	s.candidateDialMu.Lock()
	defer s.candidateDialMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.candidateDialID++
	request := s.candidateDialID
	if err := s.parent.send(serviceMessage{Kind: "connect_config", Request: request}); err != nil {
		return nil, err
	}
	for {
		select {
		case reply := <-s.configReplies:
			if reply.Request != request {
				closeFile(reply.file)
				continue
			}
			if reply.Error != "" {
				return nil, fmt.Errorf("candidate conductor connection: %s", reply.Error)
			}
			if reply.file == nil {
				return nil, fmt.Errorf("candidate conductor connection missing")
			}
			conn, err := net.FileConn(reply.file)
			reply.file.Close()
			if ctx.Err() != nil && err == nil {
				conn.Close()
				return nil, ctx.Err()
			}
			return conn, err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
