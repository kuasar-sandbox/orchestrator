package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
)

type groupCore interface {
	GroupGet(context.Context, string, string) (nodegroup.Detail, error)
	GroupList(context.Context, string, string, int) (nodegroup.Page, error)
	GroupPut(context.Context, string, string, nodegroup.PutRequest) (nodegroup.Result, error)
	GroupDelete(context.Context, string, string, int64) (nodegroup.Result, error)
}

func (a *API) groups(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	core, ok := a.core.(groupCore)
	if !ok {
		writeErr(w, http.StatusNotImplemented, "Groups are not available")
		return
	}
	query, queryErr := url.ParseQuery(r.URL.RawQuery)
	if queryErr != nil || len(r.Header.Values("X-API-KEY")) != 1 {
		a.failGroup(w, nodegroup.ErrInvalid)
		return
	}
	for key, values := range query {
		if len(values) != 1 || (key != "group" && key != "cursor" && key != "limit" && key != "expected_version") {
			a.failGroup(w, nodegroup.ErrInvalid)
			return
		}
	}
	key, group := apiKeyFrom(r.Context()), query.Get("group")
	var result any
	var err error
	switch r.Method {
	case http.MethodGet:
		if query.Has("expected_version") {
			err = nodegroup.ErrInvalid
			break
		}
		if query.Has("group") {
			if query.Has("cursor") || query.Has("limit") {
				err = nodegroup.ErrInvalid
				break
			}
			result, err = core.GroupGet(r.Context(), key, group)
		} else {
			limit := nodegroup.MaxPageSize
			if query.Has("limit") {
				limit, err = strconv.Atoi(query.Get("limit"))
				if err != nil || limit < 1 || limit > nodegroup.MaxPageSize {
					err = nodegroup.ErrInvalid
					break
				}
			}
			result, err = core.GroupList(r.Context(), key, query.Get("cursor"), limit)
		}
	case http.MethodPut:
		if group == "" || len(query) != 1 {
			err = nodegroup.ErrInvalid
			break
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		var raw []byte
		raw, err = io.ReadAll(r.Body)
		var large *http.MaxBytesError
		if errors.As(err, &large) {
			err = nodegroup.ErrTooLarge
		}
		if err != nil {
			break
		}
		var req nodegroup.PutRequest
		if err = json.Unmarshal(raw, &req); err == nil {
			result, err = core.GroupPut(r.Context(), key, group, req)
		} else if !errors.Is(err, nodegroup.ErrTooLarge) {
			err = fmt.Errorf("%w: malformed Group body", nodegroup.ErrInvalid)
		}
	case http.MethodDelete:
		if group == "" || len(query) != 2 || !query.Has("expected_version") {
			err = nodegroup.ErrInvalid
			break
		}
		var expected int64
		expected, err = strconv.ParseInt(query.Get("expected_version"), 10, 64)
		if err != nil || expected < 0 {
			err = nodegroup.ErrInvalid
			break
		}
		result, err = core.GroupDelete(r.Context(), key, group, expected)
	}
	if err != nil {
		a.failGroup(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (a *API) failGroup(w http.ResponseWriter, err error) {
	status := nodegroup.HTTPStatus(err)
	message := err.Error()
	if status == http.StatusInternalServerError {
		message = "Group operation failed"
	}
	body := map[string]any{"code": status, "message": message}
	var version *nodegroup.VersionError
	if errors.As(err, &version) {
		body["version"] = version.Version
	}
	writeJSON(w, status, body)
}
