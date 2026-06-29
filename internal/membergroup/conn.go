package membergroup

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"sync"
	"time"
)

type clientConn struct {
	r      io.ReadCloser
	w      *io.PipeWriter
	cancel context.CancelFunc
	local  net.Addr
	remote net.Addr
	once   sync.Once
	done   chan struct{}
}

type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *bufferedConn) Read(b []byte) (int, error) {
	if c.r != nil && c.r.Buffered() > 0 {
		return c.r.Read(b)
	}
	return c.Conn.Read(b)
}

func newClientConn(r io.ReadCloser, w *io.PipeWriter, cancel context.CancelFunc, local, remote string) *clientConn {
	return &clientConn{r: r, w: w, cancel: cancel, local: nameAddr(local), remote: nameAddr(remote), done: make(chan struct{})}
}

func (c *clientConn) Read(b []byte) (int, error)  { return c.r.Read(b) }
func (c *clientConn) Write(b []byte) (int, error) { return c.w.Write(b) }

func (c *clientConn) Close() error {
	c.once.Do(func() {
		_ = c.w.Close()
		_ = c.r.Close()
		if c.cancel != nil {
			c.cancel()
		}
		close(c.done)
	})
	return nil
}

func (c *clientConn) LocalAddr() net.Addr              { return c.local }
func (c *clientConn) RemoteAddr() net.Addr             { return c.remote }
func (c *clientConn) SetDeadline(time.Time) error      { return nil }
func (c *clientConn) SetReadDeadline(time.Time) error  { return nil }
func (c *clientConn) SetWriteDeadline(time.Time) error { return nil }

type serverConn struct {
	ctx    context.Context
	r      io.ReadCloser
	w      http.ResponseWriter
	rc     *http.ResponseController
	local  net.Addr
	remote net.Addr
	mu     sync.Mutex
	once   sync.Once
	done   chan struct{}
}

func newServerConn(ctx context.Context, r io.ReadCloser, w http.ResponseWriter, rc *http.ResponseController, remote string) *serverConn {
	return &serverConn{ctx: ctx, r: r, w: w, rc: rc, local: nameAddr("local"), remote: nameAddr(remote), done: make(chan struct{})}
}

func (c *serverConn) Read(b []byte) (int, error) { return c.r.Read(b) }

func (c *serverConn) Write(b []byte) (int, error) {
	select {
	case <-c.done:
		return 0, net.ErrClosed
	case <-c.ctx.Done():
		return 0, c.ctx.Err()
	default:
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	n, err := c.w.Write(b)
	if c.rc != nil {
		_ = c.rc.Flush()
	}
	return n, err
}

func (c *serverConn) Close() error {
	c.once.Do(func() {
		_ = c.r.Close()
		close(c.done)
	})
	return nil
}

func (c *serverConn) LocalAddr() net.Addr              { return c.local }
func (c *serverConn) RemoteAddr() net.Addr             { return c.remote }
func (c *serverConn) SetDeadline(time.Time) error      { return nil }
func (c *serverConn) SetReadDeadline(time.Time) error  { return nil }
func (c *serverConn) SetWriteDeadline(time.Time) error { return nil }
