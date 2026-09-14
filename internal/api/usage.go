package api

import (
	"net/http"
	"net/url"
	"strconv"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
)

func (a *API) usageStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	var query conductorextension.UsageQuery
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		a.failStats(w, ErrBadRequest)
		return
	}
	for key, entries := range values {
		if len(entries) != 1 || entries[0] == "" {
			err = ErrBadRequest
			break
		}
		switch key {
		case "view":
			query.View = entries[0]
		case "cursor":
			query.Cursor, err = strconv.ParseInt(entries[0], 10, 64)
		case "limit":
			query.Limit, err = strconv.Atoi(entries[0])
			if query.Limit < 1 {
				err = ErrBadRequest
			}
		default:
			err = ErrBadRequest
		}
		if err != nil {
			break
		}
	}
	if err != nil || (query.View != "history" && (values.Has("cursor") || values.Has("limit"))) {
		a.failStats(w, ErrBadRequest)
		return
	}
	if _, err := query.Normalize(); err != nil {
		a.failStats(w, ErrBadRequest)
		return
	}
	body, err := a.core.UsageStats(r.Context(), r.PathValue("id"), apiKeyFrom(r.Context()), query)
	if err != nil {
		a.failStats(w, err)
		return
	}
	writeJSON(w, http.StatusOK, body)
}
