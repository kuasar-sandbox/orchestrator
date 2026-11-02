package clusterclient

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"syscall"
	"testing"
)

func TestFetchMembershipPreservesCauses(t *testing.T) {
	injected := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, syscall.ECONNREFUSED })}
	reg := NewRegistryWithClient("http://bootstrap", injected)
	_, err := reg.FetchMembership(context.Background())
	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("lost dial cause: %v", err)
	}
	injected.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader("busy")), Header: make(http.Header)}, nil
	})
	_, err = reg.FetchMembership(context.Background())
	var status *MembershipStatusError
	if !errors.As(err, &status) || status.StatusCode != 503 {
		t.Fatalf("lost HTTP status: %v", err)
	}
	for _, raw := range []string{"", `{"active":`, `{} {`} {
		injected.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(raw)), Header: make(http.Header)}, nil
		})
		_, err = reg.FetchMembership(context.Background())
		var invalid *MembershipJSONError
		if !errors.As(err, &invalid) {
			t.Fatalf("complete invalid JSON %q: %v", raw, err)
		}
		if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatal("decoder cause lost")
		}
	}
}

func TestMembershipRejectsExcessiveTail(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}" + strings.Repeat(" ", 64<<10) + "garbage")), Header: make(http.Header)}, nil
	})}
	reg := NewRegistryWithClient("http://bootstrap", client)
	if _, err := reg.FetchMembership(t.Context()); err == nil {
		t.Fatal("accepted excessive trailing data")
	}
}
