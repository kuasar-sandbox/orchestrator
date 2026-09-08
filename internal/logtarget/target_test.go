package logtarget

import (
	"net/url"
	"strings"
	"testing"
)

func TestFormatStableOrderingAndValues(t *testing.T) {
	fields := map[string]string{"Z": "", "A": "a,b=c%2C+d 中文", "LINE": "first\nsecond"}
	first := Format("app", fields)
	if !strings.HasPrefix(first, "journald=app,A=") || !strings.HasSuffix(first, ",Z=") {
		t.Fatalf("order: %q", first)
	}
	for i := 0; i < 100; i++ {
		if got := Format("app", fields); got != first {
			t.Fatalf("unstable order: %q vs %q", got, first)
		}
	}
	parts := strings.Split(first, ",")
	if len(parts) != len(fields)+1 {
		t.Fatalf("value injected fields: %q", first)
	}
	for _, part := range parts[1:] {
		key, encoded, ok := strings.Cut(part, "=")
		decoded, err := url.PathUnescape(encoded)
		if !ok || err != nil || decoded != fields[key] {
			t.Fatalf("field %q: decoded=%q err=%v", key, decoded, err)
		}
	}
}

func TestFormatHasNoInheritedFields(t *testing.T) {
	withFields := Format("app", map[string]string{"STREAM": "stdout"})
	without := Format("app", nil)
	if withFields != "journald=app,STREAM=stdout" || without != "journald=app" {
		t.Fatalf("targets %q %q", withFields, without)
	}
}
