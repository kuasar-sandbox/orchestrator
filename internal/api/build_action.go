package api

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
)

var ErrBuildOwned = errors.New("build still has execution or cleanup ownership")

// DeleteBuildOptions is an action value, never a stored Build definition.
type DeleteBuildOptions struct{ Cancel bool }

type BuildActionResult struct {
	TemplateID string
	BuildID    string
	Pending    bool
}

func ParseDeleteBuildOptions(r *http.Request) (DeleteBuildOptions, error) {
	var options DeleteBuildOptions
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return options, fmt.Errorf("invalid DELETE query")
	}
	for key, values := range query {
		if key != "cancel" || len(values) != 1 || (values[0] != "true" && values[0] != "false") {
			return options, fmt.Errorf("DELETE query accepts cancel=true or cancel=false exactly once")
		}
		options.Cancel = values[0] == "true"
	}
	header, err := singleOptionalHeader(r.Header, builderHeader)
	if err != nil {
		return options, err
	}
	if header != nil {
		// Decode walks keys before decoding values: duplicate keys and null are
		// rejected, including values hidden by a later key or higher priority.
		var fields map[string]bool
		if err := strictjson.Decode([]byte(*header), &fields); err != nil {
			return options, fmt.Errorf("invalid DELETE Builder Header: %w", err)
		}
		for key, value := range fields {
			if key != "cancel" {
				return options, fmt.Errorf("DELETE Builder Header only accepts cancel")
			}
			options.Cancel = value
		}
	}
	return options, nil
}

func (a *API) cancelBuild(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" || len(r.Header.Values(builderHeader)) != 0 {
		writeErr(w, http.StatusBadRequest, "Cancel accepts no action options")
		return
	}
	result, err := a.core.CancelBuild(r.Context(), apiKeyFrom(r.Context()), r.PathValue("tid"), r.PathValue("bid"))
	a.writeBuildAction(w, result, err, false)
}

func (a *API) deleteBuild(w http.ResponseWriter, r *http.Request) {
	options, err := ParseDeleteBuildOptions(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	result, err := a.core.DeleteBuild(r.Context(), apiKeyFrom(r.Context()), r.PathValue("tid"), options)
	a.writeBuildAction(w, result, err, true)
}

func (a *API) writeBuildAction(w http.ResponseWriter, result BuildActionResult, err error, remove bool) {
	if err != nil {
		if errors.Is(err, ErrBuildOwned) {
			writeErr(w, http.StatusConflict, err.Error())
		} else {
			a.fail(w, err)
		}
		return
	}
	if result.Pending {
		if remove {
			w.Header().Set("Location", "/templates/"+url.PathEscape(result.TemplateID)+"/builds/"+url.PathEscape(result.BuildID)+"/status")
		}
		w.WriteHeader(http.StatusAccepted)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
