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
