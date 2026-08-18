package strictjson

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDecodeAllowUnknownDoesNotOwnExtensionNulls(t *testing.T) {
	var out struct {
		CPU json.RawMessage `json:"cpu"`
	}
	if err := DecodeAllowUnknown([]byte(`{"cpu":2,"extension":null}`), &out); err != nil {
		t.Fatalf("DecodeAllowUnknown: %v", err)
	}
	if string(out.CPU) != "2" {
		t.Fatalf("CPU = %s", out.CPU)
	}
}

func TestDecodeStillRejectsOwnedNullAndDuplicate(t *testing.T) {
	var out struct {
		CPU json.RawMessage `json:"cpu"`
	}
	for _, raw := range []string{`{"cpu":null}`, `{"cpu":1,"cpu":2}`} {
		if err := Decode([]byte(raw), &out); err == nil {
			t.Fatalf("Decode(%s) succeeded", raw)
		}
	}
	if err := DecodeAllowUnknown([]byte(`{"cpu":1,"cpu":2}`), &out); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate error = %v", err)
	}
}
