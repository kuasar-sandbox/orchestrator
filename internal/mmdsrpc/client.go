package mmdsrpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
)

var ErrClosed = errors.New("mmdsrpc: connection closed")

type Client struct {
	rw io.ReadWriteCloser

	writeMu sync.Mutex
	mu      sync.Mutex
	nextID  uint64
	pending map[uint64]chan EndpointResponse
	closed  bool
	err     error
}

func NewClient(rw io.ReadWriteCloser) *Client {
	client := &Client{rw: rw, pending: map[uint64]chan EndpointResponse{}}
	go client.readLoop()
	return client
}

func (c *Client) readLoop() {
	for {
		var response EndpointResponse
		if err := readFrame(c.rw, &response); err != nil {
			c.fail(err)
			return
		}
		c.mu.Lock()
		result := c.pending[response.RequestID]
		delete(c.pending, response.RequestID)
		c.mu.Unlock()
		if result != nil {
			result <- response
			close(result)
		}
	}
}

func (c *Client) fail(err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed, c.err = true, err
	pending := c.pending
	c.pending = nil
	c.mu.Unlock()
	for _, result := range pending {
		close(result)
	}
}

func (c *Client) Resolve(ctx context.Context, sandboxID, path string) (EndpointResponse, error) {
	c.mu.Lock()
	if c.closed {
		err := c.err
		if err == nil {
			err = ErrClosed
		}
		c.mu.Unlock()
		return EndpointResponse{}, err
	}
	c.nextID++
	id := c.nextID
	result := make(chan EndpointResponse, 1)
	c.pending[id] = result
	c.mu.Unlock()

	request := EndpointRequest{RequestID: id, SandboxID: sandboxID, Path: path}
	c.writeMu.Lock()
	err := writeFrame(c.rw, request)
	c.writeMu.Unlock()
	if err != nil {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return EndpointResponse{}, fmt.Errorf("mmdsrpc: write request: %w", err)
	}

	select {
	case response, ok := <-result:
		if !ok {
			return EndpointResponse{}, ErrClosed
		}
		return response, nil
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return EndpointResponse{}, ctx.Err()
	}
}

func (c *Client) Close() error {
	c.fail(ErrClosed)
	return c.rw.Close()
}
