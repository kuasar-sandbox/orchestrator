package clustercfg

import "testing"

func TestValidateRejectsBadValues(t *testing.T) {
	base := func() Config { c := Default(); c.Domain = "d"; return c }
	cases := map[string]func(*Config){
		"empty external addr":  func(c *Config) { c.GroupConfig.Providers[ProviderKey] = "external:" },
		"bad zone_admit_max":   func(c *Config) { c.Scaler.ZoneAdmitMax = "purple" },
		"negative place_cands": func(c *Config) { c.Scaler.PlaceCandidates = -1 },
		"bad data_plane_auth":  func(c *Config) { c.Router.DataPlaneAuth = "maybe" },
	}
	for name, mut := range cases {
		c := base()
		mut(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: Validate accepted an invalid config", name)
		}
	}
	ok := base()
	if err := ok.Validate(); err != nil {
		t.Fatalf("a valid default config failed Validate: %v", err)
	}
}
