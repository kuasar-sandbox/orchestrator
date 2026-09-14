package sandboxcfg

import rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"

func (p Params) runtimeUsage() (rtconfig.UsageConfig, error) {
	u := p.Usage
	sample, flush, err := u.Intervals()
	if err != nil {
		return rtconfig.UsageConfig{}, err
	}
	// Native YAML rejects explicit empty intervals; use its own defaults when
	// a programmatic caller leaves the node policy unset, including disabled.
	if u.SampleInterval == "" {
		u.SampleInterval = sample.String()
	}
	if u.FlushInterval == "" {
		u.FlushInterval = flush.String()
	}
	return u, nil
}
