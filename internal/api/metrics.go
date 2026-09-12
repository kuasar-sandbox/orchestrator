package api

import "net/http"

func (a *API) sandboxMetrics(w http.ResponseWriter, r *http.Request) {
	// Get is the existing read-only, exact SandboxID ownership check. In
	// particular, neither a paused sandbox nor a migration token causes a Wake.
	if _, err := a.core.Get(r.Context(), r.PathValue("id"), apiKeyFrom(r.Context())); err != nil {
		a.fail(w, err)
		return
	}
	if a.metricsProxy == nil {
		writeErr(w, http.StatusServiceUnavailable, "telemetry unavailable")
		return
	}
	a.metricsProxy.ServeHTTP(w, r)
}
