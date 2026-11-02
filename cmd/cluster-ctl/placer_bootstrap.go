package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"syscall"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/clusterclient"
)

// Only the first membership read is retried. Registration, listeners and readiness
// retain their existing initialization order after this returns valid owners.
func waitPlacerMembership(ctx context.Context, reg *clusterclient.Registry, log *slog.Logger, attemptTimeout, delay, maxDelay time.Duration) ([]clusterclient.Endpoint, error) {
	var nextLog time.Time
	waiting := false
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		attempt, cancel := context.WithTimeout(ctx, attemptTimeout)
		membership, err := reg.FetchMembership(attempt)
		if err == nil && len(membership.Versions) == 0 {
			err = fmt.Errorf("placer: registry membership has no members")
		}
		var eps []clusterclient.Endpoint
		if err == nil {
			eps, err = reg.OwnerEndpoints(attempt)
			if err == nil && len(eps) == 0 {
				err = fmt.Errorf("placer: registry membership has no members")
			}
			if err == nil {
				_, err = reg.NodeListEndpoints(attempt)
			}
			if err == nil {
				_, err = reg.ActiveLabel(attempt)
			}
		}
		cancel()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err == nil {
			if waiting {
				log.Info("placer: registry bootstrap recovered")
			}
			return eps, nil
		}
		if !retryPlacerBootstrap(err) {
			return nil, err
		}
		if !waiting || !time.Now().Before(nextLog) {
			log.Warn("placer: waiting for registry membership", "err", err, "retry_in", delay)
			nextLog = time.Now().Add(5 * time.Second)
		}
		waiting = true
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		if delay < maxDelay {
			delay = min(delay*2, maxDelay)
		}
	}
}

// Walk wrappers and joined targets, requiring every failure to be retryable.
// In particular url.Error implements net.Error even for permanent TLS/URL errors.
func retryPlacerBootstrap(err error) bool {
	if err == nil {
		return false
	}
	switch e := err.(type) {
	case *clusterclient.MembershipStatusError:
		switch e.StatusCode {
		case 408, 429, 500, 502, 503, 504:
			return true
		}
		return false
	case *clusterclient.MembershipJSONError, *json.SyntaxError, *json.UnmarshalTypeError, x509.UnknownAuthorityError, x509.HostnameError, x509.CertificateInvalidError, *tls.CertificateVerificationError, tls.RecordHeaderError, *net.AddrError, net.InvalidAddrError:
		return false
	case *url.Error:
		return retryPlacerBootstrap(e.Err)
	case *net.DNSError:
		return e.IsTimeout || e.IsTemporary
	case *net.OpError:
		if e.Op == "dial" && e.Net == "unix" && errors.Is(e.Err, syscall.ENOENT) {
			return true
		}
		return retryPlacerBootstrap(e.Err)
	case interface{ Unwrap() []error }:
		children := e.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !retryPlacerBootstrap(child) {
				return false
			}
		}
		return true
	case interface{ Unwrap() error }:
		return retryPlacerBootstrap(e.Unwrap())
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	switch err {
	case syscall.ECONNREFUSED, syscall.ECONNRESET, syscall.ECONNABORTED, syscall.ENETUNREACH, syscall.EHOSTUNREACH, syscall.ENETDOWN, syscall.EHOSTDOWN, syscall.ETIMEDOUT, syscall.EPIPE:
		return true
	}
	// Only an actual timeout is recognized, never a blanket net.Error match.
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}
