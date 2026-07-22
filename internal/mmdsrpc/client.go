package mmdsrpc

import (
	"context"
	"errors"
	"io"
	"sync"
)

// Client is the worker-side RPC client: implements the same
// Lookup/ServeStore/ServeRelay surface as internal/mmdsauth.Authority (and
// internal/proxyendpoints.Table), satisfying internal/mmds.EndpointAuthority
// over one duplex connection to the proxy master — this is what makes
// external mode's mmds.Server dispatch identically to internal mode's:
// internal and external modes expose the same endpoint and error
// semantics.
//
// One serialized writer (writeMu) and one reader-pump goroutine dispatching
// responses by request_id, so concurrent guest-handling goroutines can share
// a single connection safely.
//
// Lookup performs the FULL resolve+serve round trip (not just a lookup) and
// caches the result keyed by (sandbox_id,name); the immediately-following
// ServeStore/ServeRelay call internal/mmds.go always makes for the same
// request consumes that cached result instead of a second round trip. This
// coupling is safe only because of that exact calling convention — it is
// not a general-purpose cache.
type Client struct {
	conn io.ReadWriteCloser

	writeMu sync.Mutex

	mu       sync.Mutex
	nextID   uint64
	pending  map[uint64]chan *EndpointResponse
	closed   bool
	closeErr error

	cacheMu sync.Mutex
	cache   map[string]*EndpointResponse
}

// NewClient wraps conn (a socketpair-derived net.Conn in production) and
// starts its reader pump. The caller owns conn's lifetime otherwise; Client
// closes it once the reader pump observes an error (EOF on worker restart,
// etc).
func NewClient(conn io.ReadWriteCloser) *Client {
	c := &Client{
		conn:    conn,
		pending: map[uint64]chan *EndpointResponse{},
		cache:   map[string]*EndpointResponse{},
	}
	go c.readLoop()
	return c
}

func (c *Client) readLoop() {
	for {
		m, err := readFrame(c.conn)
		if err != nil {
			c.closeWith(err)
			return
		}
		if m.Type != typeResponse || m.Response == nil {
			continue
		}
		c.mu.Lock()
		ch, ok := c.pending[m.RequestID]
		if ok {
			delete(c.pending, m.RequestID)
		}
		c.mu.Unlock()
		if ok {
			ch <- m.Response
		}
	}
}

func (c *Client) closeWith(err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.closeErr = err
	pending := c.pending
	c.pending = map[uint64]chan *EndpointResponse{}
	c.mu.Unlock()
	for _, ch := range pending {
		close(ch) // wakes every in-flight call() with a closed channel -> treated as failure
	}
	_ = c.conn.Close()
}

// call performs one request/response round trip. If ctx is done first, a
// Cancel frame is sent so the master frees the corresponding inflight slot,
// and call returns ctx.Err().
func (c *Client) call(ctx context.Context, req *EndpointRequest) (*EndpointResponse, error) {
	c.mu.Lock()
	if c.closed {
		err := c.closeErr
		c.mu.Unlock()
		if err == nil {
			err = errors.New("mmdsrpc: connection closed")
		}
		return nil, err
	}
	c.nextID++
	id := c.nextID
	ch := make(chan *EndpointResponse, 1)
	c.pending[id] = ch
	c.mu.Unlock()

	if dl, ok := ctx.Deadline(); ok {
		req.DeadlineUnixMS = dl.UnixMilli()
	}
	c.writeMu.Lock()
	err := writeFrame(c.conn, &wireMsg{Type: typeRequest, RequestID: id, Request: req})
	c.writeMu.Unlock()
	if err != nil {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, err
	}

	select {
	case resp, ok := <-ch:
		if !ok || resp == nil {
			return nil, errors.New("mmdsrpc: connection closed while awaiting a response")
		}
		return resp, nil
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		c.writeMu.Lock()
		_ = writeFrame(c.conn, &wireMsg{Type: typeCancel, RequestID: id})
		c.writeMu.Unlock()
		return nil, ctx.Err()
	}
}

// Lookup resolves the exact (sandbox_id, path) to its declared name+backend
// type, performing the full resolve+serve round trip and caching the result
// for the immediately-following ServeStore/ServeRelay call. found=false
// means no endpoint owns that path (or the RPC itself failed — the caller
// falls through to the built-in envd response either way).
func (c *Client) Lookup(ctx context.Context, sandboxID, path string) (name, backendType string, found bool) {
	resp, err := c.call(ctx, &EndpointRequest{SandboxID: sandboxID, Path: path})
	if err != nil || resp == nil || !resp.Found {
		return "", "", false
	}
	key := sandboxID + "\x00" + resp.Name
	c.cacheMu.Lock()
	c.cache[key] = resp
	c.cacheMu.Unlock()
	return resp.Name, resp.BackendType, true
}

func (c *Client) take(sandboxID, name string) *EndpointResponse {
	key := sandboxID + "\x00" + name
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	resp := c.cache[key]
	delete(c.cache, key)
	return resp
}

// ServeStore returns the store value from the round trip Lookup already
// performed for this (sandboxID,name).
func (c *Client) ServeStore(_ context.Context, sandboxID, name string) (value []byte, contentType string, revision int64, present bool) {
	resp := c.take(sandboxID, name)
	if resp == nil || !resp.Present {
		if resp != nil {
			return nil, "", resp.Revision, false
		}
		return nil, "", 0, false
	}
	return resp.Body, resp.ContentType, resp.Revision, true
}

// ServeRelay returns the relay result from the round trip Lookup already
// performed for this (sandboxID,name).
func (c *Client) ServeRelay(_ context.Context, sandboxID, name string) (status int, contentType string, body []byte, ok bool) {
	resp := c.take(sandboxID, name)
	if resp == nil || !resp.Present {
		return 0, "", nil, false
	}
	return resp.Status, resp.ContentType, resp.Body, true
}
