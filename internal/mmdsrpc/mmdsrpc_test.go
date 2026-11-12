package mmdsrpc

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

func TestConcurrentOpaqueRoundTrips(t *testing.T) {
	master, worker := net.Pipe()
	server := NewServer(master, func(sandboxID, path string) EndpointResponse {
		return EndpointResponse{Found: true, Type: "secret", Present: true, Body: []byte(sandboxID + "\x00" + path)}
	}, nil)
	go server.Serve()
	client := NewClient(worker)
	defer client.Close()

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			sid, path := fmt.Sprintf("sandbox-%d", i), fmt.Sprintf("/route/%d", i)
			response, err := client.Resolve(ctx, sid, path)
			if err != nil {
				t.Errorf("Resolve: %v", err)
				return
			}
			if !response.Found || !response.Present || !bytes.Equal(response.Body, []byte(sid+"\x00"+path)) {
				t.Errorf("response metadata mismatch for request %d", i)
			}
		}()
	}
	wg.Wait()
}

func TestClientFailsPendingRequestWhenConnectionCloses(t *testing.T) {
	master, worker := net.Pipe()
	client := NewClient(worker)
	done := make(chan error, 1)
	go func() {
		_, err := client.Resolve(context.Background(), "sandbox", "/route")
		done <- err
	}()
	var request EndpointRequest
	if err := readFrame(master, &request); err != nil {
		t.Fatal(err)
	}
	_ = master.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("pending request unexpectedly succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("pending request did not fail")
	}
}

func TestHoldRPCDoesNotBlockMetadataAndRequiresDeclaration(t *testing.T) {
	master, worker := net.Pipe()
	entered, release := make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	server := NewServer(master, func(sid, path string) EndpointResponse {
		switch path {
		case "/work":
			return EndpointResponse{Found: true, Type: "hold-traffic"}
		case "/public":
			return EndpointResponse{Found: true, Type: "static", Body: []byte("data")}
		default:
			return EndpointResponse{}
		}
	}, nil).WithHold(ctx, func(ctx context.Context, sid, run, path string, value, match *int64) (*int64, int) {
		if sid != "s" || run != "old-run" || path != "/work" || value == nil || *value != 0 || match == nil || *match != 3 {
			t.Errorf("bad forwarded hold: %q %q %q %v %v", sid, run, path, value, match)
		}
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, 503
		}
		actual := int64(9)
		return &actual, 409
	})
	go server.Serve()
	client := NewClient(worker)
	defer client.Close()
	done := make(chan error, 1)
	zero, three := int64(0), int64(3)
	go func() {
		r, err := client.Hold(ctx, "s", "old-run", "/work", &zero, &three)
		if err == nil && (r.HoldStatus != 409 || r.HoldValue == nil || *r.HoldValue != 9) {
			err = fmt.Errorf("lost comparison result: %+v", r)
		}
		done <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("hold did not reach authority")
	}
	r, err := client.Resolve(ctx, "s", "/public")
	if err != nil || !r.Found || string(r.Body) != "data" {
		t.Fatalf("ordinary MMDS blocked by control request: %+v %v", r, err)
	}
	r, err = client.Hold(ctx, "s", "old-run", "/public", &zero, nil)
	if err != nil || r.HoldStatus != 404 {
		t.Fatalf("ordinary service accepted control operation: %+v %v", r, err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
