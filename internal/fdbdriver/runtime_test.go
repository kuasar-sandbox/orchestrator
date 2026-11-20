//go:build fdb && cgo

package fdbdriver

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
)

func TestCommitClassification(t *testing.T) {
	for _, test := range []struct {
		code  int
		known bool
	}{{1007, true}, {1020, true}, {1021, false}, {1031, false}, {1025, false}} {
		err := fdb.Error{Code: test.code}
		if got := knownNotCommitted(err); got != test.known {
			t.Fatalf("predicate(%d)=%v", test.code, got)
		}
		classified := classifyCommit(err)
		if errors.Is(classified, metadata.ErrUnknown) == test.known {
			t.Fatalf("classify(%d)=%v", test.code, classified)
		}
		var native fdb.Error
		if errors.As(classified, &native) {
			t.Fatal("classified error unwraps native retry type")
		}
	}
	if !errors.Is(classifyCommit(context.DeadlineExceeded), metadata.ErrUnknown) {
		t.Fatal("post-commit timeout is unknown")
	}
}
func TestEnvelopeIntegrityAndChunkThreshold(t *testing.T) {
	for _, size := range []int{0, 1, chunkBytes} {
		payload := bytes.Repeat([]byte{0x5a}, size)
		raw := envelope(payload, 0)
		raw = raw[:len(raw)-4]
		copy(raw[:10], []byte{0, 0, 0, 0, 0, 0, 0, 1, 0, 1})
		value, err := decodeRecord(fdb.Transaction{}, fdb.Key("test"), raw)
		if err != nil || !value.exists || !bytes.Equal(value.payload, payload) {
			t.Fatalf("size %d: %v", size, err)
		}
		raw[len(raw)-1] ^= 1
		if _, err = decodeRecord(fdb.Transaction{}, fdb.Key("test"), raw); err == nil {
			t.Fatalf("size %d corrupted envelope accepted", size)
		}
	}
	raw := envelope([]byte("x"), 0)
	raw = raw[:len(raw)-4]
	if _, err := recordStamp(raw); err == nil {
		t.Fatal("placeholder persisted stamp accepted")
	}
}
func TestCursorScopeAndKeyIsolation(t *testing.T) {
	s := &Store{prefix: []byte("unit/\x00")}
	a := s.key("routes", "ab")
	b := s.key("routes", "a")
	if bytes.HasPrefix(a, b) {
		t.Fatal("group length escaping")
	}
	last := mainKey(a, "route")
	cursor := encodeCursor(a, last)
	if _, err := decodeCursor(b, cursor); err == nil {
		t.Fatal("cross-scope cursor accepted")
	}
	decoded, err := decodeCursor(a, cursor)
	if err != nil || !bytes.Equal(decoded, last) {
		t.Fatal("cursor roundtrip")
	}
	chunks := chunkRange(last)
	if !bytes.HasPrefix(chunks.Begin.FDBKey(), s.prefix) || bytes.HasPrefix(chunks.Begin.FDBKey(), a) {
		t.Fatal("chunks must stay under database prefix outside record scopes")
	}
}
