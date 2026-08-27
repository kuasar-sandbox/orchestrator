package execsession

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/execadmission/limits"
)

func TestDecodeRequestValidForms(t *testing.T) {
	tests := []struct {
		name           string
		body           string
		contentLength  int64
		wantTTL        int64
		wantConditions []string
	}{
		{name: "empty", contentLength: 0},
		{name: "empty unknown length", contentLength: -1},
		{name: "object", body: `{}`, contentLength: 2},
		{name: "zero", body: `{"ttlSeconds":0}`, contentLength: -1},
		{name: "positive", body: ` { "ttlSeconds" : 37 } `, contentLength: -1, wantTTL: 37},
		{name: "int64 max", body: `{"ttlSeconds":9223372036854775807}`, contentLength: -1, wantTTL: 9223372036854775807},
		{name: "conditions omitted", body: `{"ttlSeconds":1}`, contentLength: -1, wantTTL: 1},
		{name: "conditions empty", body: `{"conditions":[]}`, contentLength: -1},
		{name: "conditions ordered", body: `{"conditions":[{"expr":"request.cwd == '/'"},{"expr":"request.user == '1000:1000'"}]}`, contentLength: -1,
			wantConditions: []string{"request.cwd == '/'", "request.user == '1000:1000'"}},
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
			if len(got.Conditions) != len(test.wantConditions) {
				t.Fatalf("Conditions = %+v, want %q", got.Conditions, test.wantConditions)
			}
			for i := range test.wantConditions {
				if got.Conditions[i].Expr != test.wantConditions[i] {
					t.Fatalf("Conditions[%d] = %q, want %q", i, got.Conditions[i].Expr, test.wantConditions[i])
				}
			}
			if len(test.wantConditions) == 0 && got.Conditions != nil {
				t.Fatalf("unrestricted Conditions = %#v, want nil", got.Conditions)
			}
		})
	}
}

func TestDecodeRequestRejectsInvalidJSONContract(t *testing.T) {
	tests := map[string]string{
		"whitespace only":      " \n\t ",
		"null body":            `null`,
		"array":                `[]`,
		"string":               `"value"`,
		"number":               `1`,
		"unknown field":        `{"other":1}`,
		"case variant":         `{"TTLSeconds":1}`,
		"upper-case field":     `{"TTLSECONDS":1}`,
		"duplicate ttl":        `{"ttlSeconds":1,"ttlSeconds":2}`,
		"escaped duplicate":    `{"ttlSeconds":1,"\u0074tlSeconds":2}`,
		"null ttl":             `{"ttlSeconds":null}`,
		"string ttl":           `{"ttlSeconds":"1"}`,
		"fractional ttl":       `{"ttlSeconds":1.5}`,
		"exponent ttl":         `{"ttlSeconds":1e2}`,
		"negative ttl":         `{"ttlSeconds":-1}`,
		"overflow ttl":         `{"ttlSeconds":9223372036854775808}`,
		"second object":        `{} {}`,
		"trailing garbage":     `{} x`,
		"conditions null":      `{"conditions":null}`,
		"conditions object":    `{"conditions":{}}`,
		"condition string":     `{"conditions":["true"]}`,
		"condition null":       `{"conditions":[null]}`,
		"condition unknown":    `{"conditions":[{"expr":"true","other":1}]}`,
		"condition duplicate":  `{"conditions":[{"expr":"true","expr":"false"}]}`,
		"condition missing":    `{"conditions":[{}]}`,
		"condition expr null":  `{"conditions":[{"expr":null}]}`,
		"condition expr empty": `{"conditions":[{"expr":""}]}`,
		"duplicate conditions": `{"conditions":[],"conditions":[]}`,
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

func TestDecodeRequestEnforcesConditionBounds(t *testing.T) {
	tests := []struct {
		name       string
		conditions []Condition
	}{
		{name: "too many", conditions: make([]Condition, limits.MaxConditions+1)},
		{name: "expression bytes", conditions: []Condition{{Expr: strings.Repeat("x", limits.MaxConditionExprBytes+1)}}},
		{name: "total expression bytes", conditions: []Condition{
			{Expr: strings.Repeat("x", 900)}, {Expr: strings.Repeat("y", 900)},
			{Expr: strings.Repeat("z", 900)}, {Expr: strings.Repeat("a", 900)},
			{Expr: strings.Repeat("b", 900)},
		}},
	}
	for index := range tests[0].conditions {
		tests[0].conditions[index].Expr = "true"
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body, err := json.Marshal(struct {
				Conditions []Condition `json:"conditions"`
			}{Conditions: test.conditions})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := DecodeRequest(strings.NewReader(string(body)), int64(len(body))); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("DecodeRequest() error = %v, want invalid request", err)
			}
		})
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
