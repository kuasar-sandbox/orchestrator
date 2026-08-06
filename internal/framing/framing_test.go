package framing

import (
	"bytes"
	"errors"
	"testing"
)

type testMsg struct {
	A string `json:"a"`
	B int    `json:"b"`
}

func TestWriteReadFrameRoundTrip(t *testing.T) {
	want := testMsg{A: "hello", B: 7}
	b, err := EncodeChecked(want, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := WriteFrame(&buf, b); err != nil {
		t.Fatal(err)
	}
	got, err := ReadFrame(&buf, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, b) {
		t.Fatalf("ReadFrame = %q, want %q", got, b)
	}
}

func TestEncodeCheckedRejectsOversized(t *testing.T) {
	_, err := EncodeChecked(testMsg{A: "aaaaaaaaaa"}, 4)
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err = %v, want ErrTooLarge", err)
	}
}

func TestReadFrameRejectsOversizedLengthPrefix(t *testing.T) {
	var buf bytes.Buffer
	// A well-formed frame for a 100-byte body, bounded to 4 bytes max.
	body := bytes.Repeat([]byte("x"), 100)
	if err := WriteFrame(&buf, body); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFrame(&buf, 4); err == nil {
		t.Fatal("expected an error for a frame length exceeding maxFrame")
	}
}

func TestReadFrameRejectsZeroLength(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFrame(&buf, 1<<20); err == nil {
		t.Fatal("expected an error for a zero-length frame")
	}
}
