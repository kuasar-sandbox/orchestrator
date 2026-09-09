package proxy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
)

var errHTTPRequestRewrite = errors.New("proxy: guest request rewrite rejected")

// ForwardHTTP completes one HTTP exchange over an already-connected backend.
// It clones r, applies rewrite before transport normalization, and streams the
// response or relays an HTTP/1.1 WebSocket upgrade until both pumps finish. br
// preserves bytes read by an earlier CONNECT handshake. The caller owns backend
// and must close it on return; no connection is pooled or request replayed.
//
// onResponse, when non-nil, runs exactly once when a response is accepted, before
// streaming it. Errors are returned only while w still belongs to the caller:
// after committing an ordinary response or hijacking, failures only close the
// transport and never cause a second HTTP response.
func ForwardHTTP(w http.ResponseWriter, r *http.Request, backend net.Conn, br *bufio.Reader, rewrite func(*http.Request) error, onResponse func()) error {
	stopContextClose := context.AfterFunc(r.Context(), func() { _ = backend.Close() })
	defer stopContextClose()
	out := cloneForwardHTTPRequest(r)
	if rewrite != nil {
		if err := rewrite(out); err != nil {
			return fmt.Errorf("%w: %w", errHTTPRequestRewrite, err)
		}
	}
	websocket := isWebSocketRequest(r) && isWebSocketRequest(out)
	normalizeForwardHTTPRequest(out, websocket)
	if br == nil {
		br = bufio.NewReader(backend)
	}
	if err := out.Write(backend); err != nil {
		return err
	}
	resp, err := http.ReadResponse(br, out)
	// An informational response is not the WebSocket handshake's final answer.
	for err == nil && websocket && resp.StatusCode >= 100 && resp.StatusCode < 200 && resp.StatusCode != http.StatusSwitchingProtocols {
		_ = resp.Body.Close()
		resp, err = http.ReadResponse(br, out)
	}
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		if onResponse != nil {
			onResponse()
		}
		WriteHTTPResponse(w, resp)
		return nil
	}
	if !websocket || resp.ProtoMajor != 1 || resp.ProtoMinor != 1 || !isWebSocketUpgrade(resp.Header) {
		return errors.New("proxy: unexpected upstream protocol upgrade")
	}
	client, rw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		return fmt.Errorf("proxy: hijack WebSocket: %w", err)
	}
	defer client.Close()
	stopClientClose := context.AfterFunc(r.Context(), func() { _ = client.Close() })
	defer stopClientClose()
	if onResponse != nil {
		onResponse()
	}
	// Preserve wrapper response headers and endpoint handshake headers, while
	// replacing hop-specific headers with the single negotiated Upgrade.
	headers := w.Header().Clone()
	copyHeader(headers, resp.Header)
	normalizeWebSocketHeaders(headers)
	resp.Header = headers
	resp.Body, resp.ContentLength, resp.TransferEncoding, resp.Close = nil, 0, nil, false
	if err := resp.Write(rw); err != nil {
		return nil // Hijack transferred response ownership, even if 101 failed.
	}
	if err := rw.Flush(); err != nil {
		return nil
	}
	// Drain net/http's read-ahead only, then read the raw connection. Reading
	// further through its connReader would cancel r.Context on a valid H1
	// half-close and truncate the backend's trailing frames.
	reader := bufio.NewReader(io.MultiReader(io.LimitReader(rw.Reader, int64(rw.Reader.Buffered())), client))
	downstream := &h1ConnectStream{Conn: client, reader: reader}
	relayConnectStreams(downstream, &bufferedConn{Conn: backend, r: br})
	return nil
}

func cloneForwardHTTPRequest(r *http.Request) *http.Request {
	out := r.Clone(r.Context())
	if out.URL == nil {
		out.URL = &url.URL{}
	}
	return out
}

func normalizeForwardHTTPRequest(out *http.Request, websocket bool) {
	out.RequestURI = ""
	out.URL.Scheme = "http"
	if out.Host != "" {
		out.URL.Host = out.Host
	} else if out.URL.Host == "" {
		out.URL.Host = "sandbox"
	}
	out.Close = !websocket
	if websocket {
		normalizeWebSocketHeaders(out.Header)
	} else {
		removeHopHeaders(out.Header)
	}
}

func isWebSocketRequest(r *http.Request) bool {
	return r.Method == http.MethodGet && r.ProtoMajor == 1 && r.ProtoMinor == 1 && isWebSocketUpgrade(r.Header)
}

func isWebSocketUpgrade(h http.Header) bool {
	upgrade := h.Values("Upgrade")
	if len(upgrade) != 1 || !strings.EqualFold(strings.TrimSpace(upgrade[0]), "websocket") {
		return false
	}
	for _, value := range h.Values("Connection") {
		for _, token := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), "upgrade") {
				return true
			}
		}
	}
	return false
}

func normalizeWebSocketHeaders(h http.Header) {
	for _, value := range h.Values("Connection") {
		for _, token := range strings.Split(value, ",") {
			h.Del(strings.TrimSpace(token))
		}
	}
	removeHopHeaders(h)
	h.Set("Connection", "Upgrade")
	h.Set("Upgrade", "websocket")
}

func removeHopHeaders(h http.Header) {
	for _, k := range []string{
		"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate",
		"Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade",
	} {
		h.Del(k)
	}
}

// WriteHTTPResponse copies an upstream response to the client and flushes as data
// arrives, preserving streaming semantics without keeping a reusable upstream
// connection alive.
func WriteHTTPResponse(w http.ResponseWriter, resp *http.Response) {
	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	if resp.Body != nil {
		_, _ = io.Copy(flushWriter{w}, resp.Body)
	}
}

func copyHeader(dst, src http.Header) {
	for k, vals := range src {
		for _, v := range vals {
			dst.Add(k, v)
		}
	}
}
