package mmdsrpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
)

// ErrClosed is returned by Resolve once the underlying connection has failed
// or been closed; the worker's mmds.Source implementation maps it to 503.
var ErrClosed = errors.New("mmdsrpc: connection closed")

// Client is the worker-side RPC client for one inherited socketpair
// connection to the proxy master. Safe for concurrent use: multiple goroutines
// (one per concurrent guest request) may call Resolve at once -- writes are
// serialized and responses are dispatched back to the right caller by
// RequestID via one reader pump.
type Client struct {
	rw io.ReadWriteCloser

	writeMu sync.Mutex

	mu       sync.Mutex
	nextID   uint64
	pending  map[uint64]chan EndpointResponse
	closed   bool
	closeErr error
}

// NewClient wraps rw (one end of an inherited socketpair) and starts its
// reader pump. Call Close when the worker is shutting down.
func NewClient(rw io.ReadWriteCloser) *Client {
	c := &Client{rw: rw, pending: make(map[uint64]chan EndpointResponse)}
	go c.readLoop()
	return c
}

func (c *Client) readLoop() {
	for {
		var resp EndpointResponse
		if err := readFrame(c.rw, &resp); err != nil {
			c.fail(err)
			return
		}
		c.mu.Lock()
		ch, ok := c.pending[resp.RequestID]
		if ok {
			delete(c.pending, resp.RequestID)
		}
		c.mu.Unlock()
		if ok {
			ch <- resp
			close(ch)
		}
	}
}

func (c *Client) fail(err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.closeErr = err
	pending := c.pending
	c.pending = nil
	c.mu.Unlock()
	for _, ch := range pending {
		close(ch)
	}
}

// Resolve asks the master to resolve sid's specified MMDS route at path,
// blocking until a response arrives, ctx is done, or the connection fails.
func (c *Client) Resolve(ctx context.Context, sid, path string) (EndpointResponse, error) {
	c.mu.Lock()
	if c.closed {
		err := c.closeErr
		if err == nil {
			err = ErrClosed
		}
		c.mu.Unlock()
		return EndpointResponse{}, err
	}
	c.nextID++
	id := c.nextID
	ch := make(chan EndpointResponse, 1)
	c.pending[id] = ch
	c.mu.Unlock()

	req := EndpointRequest{RequestID: id, SandboxID: sid, Path: path}
	if dl, ok := ctx.Deadline(); ok {
		req.DeadlineUnixMS = dl.UnixMilli()
	}
	c.writeMu.Lock()
	err := writeFrame(c.rw, req)
	c.writeMu.Unlock()
	if err != nil {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return EndpointResponse{}, fmt.Errorf("mmdsrpc: write request: %w", err)
	}

	select {
	case resp, ok := <-ch:
		if !ok {
			return EndpointResponse{}, ErrClosed
		}
		return resp, nil
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return EndpointResponse{}, ctx.Err()
	}
}

// Close closes the underlying connection and fails every pending Resolve.
func (c *Client) Close() error {
	c.fail(ErrClosed)
	return c.rw.Close()
}
