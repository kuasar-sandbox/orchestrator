package routeapi

import (
	"context"
	"net/http"
)

const (
	PermitPath        = "/internal/system/permit"
	ReserveRoutePath  = "/internal/route/reserve"
	ResumeRoutePath   = "/internal/route/resume"
	DeleteRoutePath   = "/internal/route/delete"
	ListRoutesPath    = "/internal/route/list"
	WatchRoutesPath   = "/internal/route/watch"
	RegisterBuildPath = "/internal/build/register"
)

type MutationService interface {
	RefreshPermit(context.Context, PermitRequest) (PermitResponse, error)
	ReserveSandbox(context.Context, ReserveSandboxRequest) (RouteMutationResponse, error)
	ResumeSandbox(context.Context, ResumeSandboxRequest) (RouteMutationResponse, error)
	DeleteSandbox(context.Context, DeleteSandboxRequest) (RouteMutationResponse, error)
	ListRoutes(context.Context, ListRoutesRequest) (ListRoutesResponse, error)
	WatchRoutes(context.Context, WatchRoutesRequest) (WatchRoutesResponse, error)
	RegisterBuild(context.Context, RegisterBuildRequest) (BuildMutationResponse, error)
}

func NewMutationHandler(service MutationService, trust TrustVerifier) http.Handler {
	mux := http.NewServeMux()
	handleMutation(mux, PermitPath, trust, service.RefreshPermit, PermitRequest.Validate,
		func(request PermitRequest, response PermitResponse) error { return response.ValidateFor(request) })
	handleMutation(mux, ReserveRoutePath, trust, service.ReserveSandbox, ReserveSandboxRequest.Validate,
		func(request ReserveSandboxRequest, response RouteMutationResponse) error {
			return response.ValidateFor(request.RequestIdentity, request.Group, request.RouteKey)
		})
	handleMutation(mux, ResumeRoutePath, trust, service.ResumeSandbox, ResumeSandboxRequest.Validate,
		func(request ResumeSandboxRequest, response RouteMutationResponse) error {
			return response.ValidateFor(request.RequestIdentity, request.Group, request.RouteKey)
		})
	handleMutation(mux, DeleteRoutePath, trust, service.DeleteSandbox, DeleteSandboxRequest.Validate,
		func(request DeleteSandboxRequest, response RouteMutationResponse) error {
			return response.ValidateFor(request.RequestIdentity, request.Group, request.RouteKey)
		})
	handleMutation(mux, ListRoutesPath, trust, service.ListRoutes, ListRoutesRequest.Validate,
		func(request ListRoutesRequest, response ListRoutesResponse) error {
			return response.ValidateFor(request)
		})
	handleMutation(mux, WatchRoutesPath, trust, service.WatchRoutes, WatchRoutesRequest.Validate,
		func(request WatchRoutesRequest, response WatchRoutesResponse) error {
			return response.ValidateFor(request)
		})
	handleMutation(mux, RegisterBuildPath, trust, service.RegisterBuild, RegisterBuildRequest.Validate,
		func(request RegisterBuildRequest, response BuildMutationResponse) error {
			return response.ValidateFor(request)
		})
	return mux
}

func handleMutation[Request any, Response any](
	mux *http.ServeMux,
	path string,
	trust TrustVerifier,
	call func(context.Context, Request) (Response, error),
	validateRequest func(Request) error,
	validateResponse func(Request, Response) error,
) {
	mux.HandleFunc("POST "+path, func(w http.ResponseWriter, request *http.Request) {
		if !authorizeInternal(w, request, trust) {
			return
		}
		var input Request
		if err := decodeRequest(request, &input); err != nil || validateRequest(input) != nil {
			http.Error(w, "invalid trusted control-plane request", http.StatusBadRequest)
			return
		}
		response, err := call(request.Context(), input)
		if err != nil {
			http.Error(w, "control plane unavailable", http.StatusServiceUnavailable)
			return
		}
		if err := validateResponse(input, response); err != nil {
			http.Error(w, "invalid control-plane service response", http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, response)
	})
}
