package membertransport

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestPacketDeliveryByLabel(t *testing.T) {
	mux := NewMux()
	tr := New("registry.1.hash", "a", nil, nil)
	if err := mux.Register(tr); err != nil {
		t.Fatalf("register: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, PacketPath+url.PathEscape(tr.Label), bytes.NewReader([]byte("hello")))
	req.Header.Set(HeaderFrom, "b")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d body=%q", rec.Code, rec.Body.String())
	}

	select {
	case pkt := <-tr.PacketCh():
		if string(pkt.Buf) != "hello" || pkt.From != "b" {
			t.Fatalf("packet = %+v", pkt)
		}
	case <-time.After(time.Second):
		t.Fatal("packet not delivered")
	}
}

func TestPacketUnknownLabel(t *testing.T) {
	mux := NewMux()
	req := httptest.NewRequest(http.MethodPost, PacketPath+"missing", bytes.NewReader([]byte("hello")))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d body=%q", rec.Code, rec.Body.String())
	}
}

func TestOpenLocalStream(t *testing.T) {
	mux := NewMux()
	tr := New("registry.1.hash", "a", nil, nil)
	if err := mux.Register(tr); err != nil {
		t.Fatalf("register: %v", err)
	}

	client, err := mux.OpenLocalStream(tr.Label)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer client.Close()

	server := <-tr.StreamCh()
	defer server.Close()

	done := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 5)
		_, _ = io.ReadFull(server, buf)
		done <- buf
	}()
	if _, err := client.Write([]byte("hello")); err != nil {
		t.Fatalf("client write: %v", err)
	}
	if got := string(<-done); got != "hello" {
		t.Fatalf("server read = %q", got)
	}
}

func TestWriteToUsesResolverAndHTTPMux(t *testing.T) {
	mux := NewMux()
	dst := New("registry.1.hash", "b", nil, nil)
	if err := mux.Register(dst); err != nil {
		t.Fatalf("register dst: %v", err)
	}

	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec.Result(), nil
	})}
	src := New("registry.1.hash", "a", StaticResolver{"b": "http://registry-b"}, client)
	if _, err := src.WriteTo([]byte("hello"), "b"); err != nil {
		t.Fatalf("write: %v", err)
	}
	select {
	case pkt := <-dst.PacketCh():
		if string(pkt.Buf) != "hello" || pkt.From != "a" {
			t.Fatalf("packet = %+v", pkt)
		}
	case <-time.After(time.Second):
		t.Fatal("packet not delivered")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
