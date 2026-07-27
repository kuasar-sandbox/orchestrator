package execsession

import (
	"errors"
	"strings"
	"testing"
)

func TestDecodeRequestValidForms(t *testing.T) {
	tests := []struct {
		name          string
		body          string
		contentLength int64
		wantTTL       int64
	}{
		{name: "empty", contentLength: 0},
		{name: "empty unknown length", contentLength: -1},
		{name: "object", body: `{}`, contentLength: 2},
		{name: "zero", body: `{"ttlSeconds":0}`, contentLength: -1},
		{name: "positive", body: ` { "ttlSeconds" : 37 } `, contentLength: -1, wantTTL: 37},
		{name: "int64 max", body: `{"ttlSeconds":9223372036854775807}`, contentLength: -1, wantTTL: 9223372036854775807},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := DecodeRequest(strings.NewReader(test.body), test.contentLength)
			if err != nil {
				t.Fatalf("DecodeRequest() error = %v", err)
			}
			if got.TTLSeconds != test.wantTTL {
				t.Fatalf("TTLSeconds = %d, want %d", got.TTLSeconds, test.wantTTL)
			}
		})
	}
}

func TestDecodeRequestRejectsInvalidJSONContract(t *testing.T) {
	tests := map[string]string{
		"whitespace only":  " \n\t ",
		"null body":        `null`,
		"array":            `[]`,
		"string":           `"value"`,
		"number":           `1`,
		"unknown field":    `{"other":1}`,
		"null ttl":         `{"ttlSeconds":null}`,
		"string ttl":       `{"ttlSeconds":"1"}`,
		"fractional ttl":   `{"ttlSeconds":1.5}`,
		"exponent ttl":     `{"ttlSeconds":1e2}`,
		"negative ttl":     `{"ttlSeconds":-1}`,
		"overflow ttl":     `{"ttlSeconds":9223372036854775808}`,
		"second object":    `{} {}`,
		"trailing garbage": `{} x`,
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := DecodeRequest(strings.NewReader(body), int64(len(body)))
			if !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("DecodeRequest() error = %v, want invalid request", err)
			}
		})
	}
}

func TestDecodeRequestEnforcesCompleteRawBodyLimit(t *testing.T) {
	exact := `{}` + strings.Repeat(" ", int(MaxRequestBodyBytes)-2)
	request, err := DecodeRequest(strings.NewReader(exact), int64(len(exact)))
	if err != nil || request.TTLSeconds != 0 {
		t.Fatalf("exact-limit body = %+v, %v", request, err)
	}

	for name, body := range map[string]string{
		"trailing whitespace": exact + " ",
		"trailing data":       exact + "x",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := DecodeRequest(strings.NewReader(body), -1)
			if !errors.Is(err, ErrRequestTooLarge) {
				t.Fatalf("DecodeRequest() error = %v, want too large", err)
			}
		})
	}

	reader := &countingReader{Reader: strings.NewReader("{}"), reads: new(int)}
	if _, err := DecodeRequest(reader, MaxRequestBodyBytes+1); !errors.Is(err, ErrRequestTooLarge) {
		t.Fatalf("known oversized body error = %v", err)
	}
	if *reader.reads != 0 {
		t.Fatalf("known oversized body performed %d reads", *reader.reads)
	}
}

type countingReader struct {
	*strings.Reader
	reads *int
}

func (r *countingReader) Read(p []byte) (int, error) {
	*r.reads++
	return r.Reader.Read(p)
}
