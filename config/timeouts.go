package config

import (
	"fmt"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

// SandboxTimeoutsConfig is node-local runtime policy for ordinary sandboxes and
// every Builder phase. It is never inherited from a portable artifact.
type SandboxTimeoutsConfig struct {
	AppStart string `yaml:"app_start,omitempty" json:"app_start,omitempty"`
}

func (t SandboxTimeoutsConfig) Runtime() rtconfig.TimeoutsConfig {
	return rtconfig.TimeoutsConfig{AppStart: t.AppStart}
}

func (t SandboxTimeoutsConfig) validate() error {
	if t.AppStart == "" {
		return nil
	}
	d, err := time.ParseDuration(t.AppStart)
	if err != nil || d <= 0 {
		return fmt.Errorf("config: sandbox.timeouts.app_start must be a positive duration")
	}
	return nil
}

func (t *SandboxTimeoutsConfig) UnmarshalJSON(raw []byte) error {
	type wire SandboxTimeoutsConfig
	var decoded wire
	if err := strictjson.Decode(raw, &decoded); err != nil {
		return err
	}
	result := SandboxTimeoutsConfig(decoded)
	if err := result.validate(); err != nil {
		return err
	}
	*t = result
	return nil
}
