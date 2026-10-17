package config

import (
	"strings"
	"testing"
	"time"
)

func TestProxyShutdownConfiguration(t *testing.T) {
	for _, test := range []struct {
		name, yaml string
		want       time.Duration
		invalid    bool
	}{
		{"omitted", "", 0, false},
		{"zero", "shutdown_timeout: 0s\n", 0, false},
		{"programmatic", "shutdown_timeout: 2s\ninplace_restart: false\n", 2 * time.Second, false},
		{"signal", "shutdown_timeout: 2s\ninplace_restart: true\n", 2 * time.Second, false},
		{"signal-without-timeout", "inplace_restart: true\n", 0, true},
		{"signal-zero", "inplace_restart: true\nshutdown_timeout: 0s\n", 0, true},
		{"negative", "shutdown_timeout: -1s\n", 0, true},
		{"malformed", "shutdown_timeout: soon\n", 0, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg, err := DecodeProxy(strings.NewReader("data_listen: 127.0.0.1:8080\n" + test.yaml))
			if (err != nil) != test.invalid {
				t.Fatalf("configuration error=%v", err)
			}
			if err == nil && cfg.ShutdownTimeoutDur() != test.want {
				t.Fatalf("timeout=%s want=%s", cfg.ShutdownTimeoutDur(), test.want)
			}
		})
	}
}
