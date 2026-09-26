package main

import (
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestSandboxResultErrorBoundSurvivesJSONRoundTrip(t *testing.T) {
	for _, suffix := range []string{"é", "界", "🙂"} {
		for prefix := 1021; prefix <= 1024; prefix++ {
			message := sanitizeSandboxResultError(strings.Repeat("x", prefix) + suffix + "\n\x00")
			if !utf8.ValidString(message) {
				t.Fatalf("truncation split a UTF-8 rune: prefix=%d suffix=%q", prefix, suffix)
			}
			wire, err := json.Marshal(message)
			if err != nil {
				t.Fatal(err)
			}
			var decoded string
			if err := json.Unmarshal(wire, &decoded); err != nil {
				t.Fatal(err)
			}
			if len(decoded) > 1024 || decoded != message {
				t.Fatalf("wire result exceeded or changed bounded diagnostics: before=%d after=%d", len(message), len(decoded))
			}
			if strings.ContainsAny(decoded, "\n\r\t\x00") {
				t.Fatal("control character survived sanitization")
			}
		}
	}
}
