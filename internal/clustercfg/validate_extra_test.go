package clustercfg

import "testing"

// TestValidateRejectsBadValues exercises each role's Validate over the bad values it
// owns (durations/membership → registry, zones/candidates → scaler, auth → router).
func TestValidateRejectsBadValues(t *testing.T) {
	t.Run("registry bad duration", func(t *testing.T) {
		c := DefaultRegistry()
		c.RouteLink.ParkTimeout = "bad"
		if err := c.Validate(); err == nil {
			t.Error("Validate accepted a bad route_link.park_timeout")
		}
	})
	t.Run("registry member id outside active membership", func(t *testing.T) {
		c := DefaultRegistry()
		c.Member.ID = "missing"
		if err := c.Validate(); err == nil {
			t.Error("Validate accepted a member.id outside active membership")
		}
	})
	t.Run("scaler bad zone_admit_max", func(t *testing.T) {
		c := DefaultScaler()
		c.Placement.ZoneAdmitMax = "purple"
		if err := c.Validate(); err == nil {
			t.Error("Validate accepted a bad zone_admit_max")
		}
	})
	t.Run("scaler negative candidates", func(t *testing.T) {
		c := DefaultScaler()
		c.Placement.Candidates = -1
		if err := c.Validate(); err == nil {
			t.Error("Validate accepted a negative placement.candidates")
		}
	})
	t.Run("router bad data_plane", func(t *testing.T) {
		c := DefaultRouter()
		c.Domain = "d"
		c.Auth.DataPlane = "maybe"
		if err := c.Validate(); err == nil {
			t.Error("Validate accepted a bad auth.data_plane")
		}
	})
	t.Run("valid defaults pass", func(t *testing.T) {
		r := DefaultRegistry()
		if err := r.Validate(); err != nil {
			t.Fatalf("a valid default registry failed Validate: %v", err)
		}
		rt := DefaultRouter()
		rt.Domain = "d"
		if err := rt.Validate(); err != nil {
			t.Fatalf("a valid default router failed Validate: %v", err)
		}
		s := DefaultScaler()
		if err := s.Validate(); err != nil {
			t.Fatalf("a valid default scaler failed Validate: %v", err)
		}
	})
}
