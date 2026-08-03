package configsock

import (
	"strings"
	"testing"
)

func TestReadinessSocketPath(t *testing.T) {
	got := ReadinessSocketPath("/run/sandbox", "sandbox-id")
	if got != "/run/sandbox/sandbox-id/ready.sock" {
		t.Fatalf("ReadinessSocketPath = %q", got)
	}
}

func TestReadReadiness(t *testing.T) {
	tests := []struct {
		name    string
		wire    string
		wantErr bool
	}{
		{name: "exact", wire: "control_ready\nready\n"},
		{name: "EOF before control", wire: "", wantErr: true},
		{name: "EOF before ready", wire: "control_ready\n", wantErr: true},
		{name: "unknown", wire: "unknown\nready\n", wantErr: true},
		{name: "duplicate", wire: "control_ready\ncontrol_ready\n", wantErr: true},
		{name: "reverse", wire: "ready\ncontrol_ready\n", wantErr: true},
		{name: "trailing event", wire: "control_ready\nready\nready\n", wantErr: true},
		{name: "unterminated control", wire: "control_ready", wantErr: true},
		{name: "line too long", wire: strings.Repeat("x", readinessLineLimit+1) + "\nready\n", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ReadReadiness(strings.NewReader(tt.wire))
			if (err != nil) != tt.wantErr {
				t.Fatalf("ReadReadiness(%q) error = %v, want error=%t", tt.wire, err, tt.wantErr)
			}
		})
	}
}
