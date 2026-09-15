package configsock

import (
	"errors"
	"net/http"
	"net/url"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
)

func (s *Server) handleAdminBuildAction(w http.ResponseWriter, r *http.Request) {
	peer, ok := peerFrom(r.Context())
	if !ok || !s.adminAuthed(peer) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "not authorized (admin)"})
		return
	}
	var result api.BuildActionResult
	var err error
	if r.Method == http.MethodDelete {
		var options api.DeleteBuildOptions
		options, err = api.ParseDeleteBuildOptions(r)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		result, err = s.deps.BuilderActionAdmin.DeleteBuildAdmin(r.Context(), r.PathValue("tid"), options)
	} else {
		result, err = s.deps.BuilderActionAdmin.CancelBuildAdmin(r.Context(), r.PathValue("bid"))
	}
	if err != nil {
		status := http.StatusInternalServerError
		switch {
		case errors.Is(err, api.ErrNotFound):
			status = http.StatusNotFound
		case errors.Is(err, api.ErrBadRequest):
			status = http.StatusBadRequest
		case errors.Is(err, api.ErrBuildOwned):
			status = http.StatusConflict
		}
		message := "internal error"
		if status != http.StatusInternalServerError {
			message = err.Error()
		}
		writeJSON(w, status, map[string]string{"error": message})
		return
	}
	if result.Pending {
		w.Header().Set("Location", "/templates/"+url.PathEscape(result.TemplateID)+"/builds/"+url.PathEscape(result.BuildID)+"/status")
		w.WriteHeader(http.StatusAccepted)
	} else {
		w.WriteHeader(http.StatusNoContent)
	}
}
