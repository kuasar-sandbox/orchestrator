package orch

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	proxypkg "github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/transportauth"
)

// ClusterAPIHandler is the only remote cluster-mode e2b API entry. It verifies
// the Router identity and the exact protected object Binding before any read or
// side effect reaches the ordinary node-local API handler.
func (n *FinalClusterNode) ClusterAPIHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if err := transportauth.VerifyRequest(request, transportauth.RoleRouter); err != nil {
			http.Error(w, "untrusted Router identity", http.StatusForbidden)
			return
		}
		if status, kind, err := n.verifyDirectAPIRequest(request); err != nil {
			w.Header().Set(proxypkg.HeaderProxyError, kind)
			http.Error(w, err.Error(), status)
			return
		}
		next.ServeHTTP(w, request)
	})
}

func (n *FinalClusterNode) verifyDirectAPIRequest(request *http.Request) (int, string, error) {
	local, err := n.session.Current(request.Context())
	if err != nil {
		return http.StatusServiceUnavailable, proxypkg.ProxyErrorRouteError, err
	}
	nodeID := request.Header.Get(proxypkg.HeaderNodeID)
	nodeEpoch, epochErr := strconv.ParseUint(request.Header.Get(proxypkg.HeaderNodeEpoch), 10, 64)
	if nodeID != local.NodeID {
		return http.StatusConflict, proxypkg.ProxyErrorWrongBinding, errors.New("direct request targets another node")
	}
	if epochErr != nil || nodeEpoch == 0 || nodeEpoch != local.NodeEpoch {
		return http.StatusConflict, proxypkg.ProxyErrorWrongNodeEpoch, errors.New("direct request targets another NodeEpoch")
	}

	kindName := request.Header.Get(clusterstate.DirectHeaderExecutionKind)
	objectID := request.Header.Get(clusterstate.DirectHeaderObjectID)
	group := request.Header.Get(clusterstate.DirectHeaderGroup)
	if objectID == "" || group == "" || request.Header.Get(proxypkg.HeaderRegistryGeneration) == "" ||
		request.Header.Get(proxypkg.HeaderBindingDigest) == "" {
		return http.StatusConflict, proxypkg.ProxyErrorWrongBinding, errors.New("direct request has an incomplete execution fence")
	}

	var kind clusterstate.ExecutionKind
	var metadata map[string]string
	switch kindName {
	case "sandbox":
		kind = clusterstate.ExecutionKindSandbox
		sandbox, getErr := n.store.Get(request.Context(), objectID)
		if getErr != nil {
			return http.StatusServiceUnavailable, proxypkg.ProxyErrorRouteError, getErr
		}
		if sandbox == nil {
			return http.StatusNotFound, proxypkg.ProxyErrorNotFound, errors.New("Sandbox execution is not found")
		}
		metadata = sandbox.Metadata
		if directPathObjectID(request.URL.Path, "/sandboxes/") != objectID {
			return http.StatusConflict, proxypkg.ProxyErrorWrongBinding, errors.New("Sandbox URL identifies another execution")
		}
	case "build":
		kind = clusterstate.ExecutionKindBuild
		build, getErr := n.store.GetBuild(request.Context(), objectID)
		if getErr != nil {
			return http.StatusServiceUnavailable, proxypkg.ProxyErrorRouteError, getErr
		}
		if build == nil {
			return http.StatusNotFound, proxypkg.ProxyErrorNotFound, errors.New("Build execution is not found")
		}
		metadata = build.Metadata
		if !directBuildPathMatches(request.URL.Path, objectID, build.TemplateID) {
			return http.StatusConflict, proxypkg.ProxyErrorWrongBinding, errors.New("Build URL identifies another execution")
		}
	default:
		return http.StatusConflict, proxypkg.ProxyErrorWrongBinding, errors.New("direct request has an invalid execution kind")
	}

	binding, opaque, err := clusterstate.ExecutionBindingFromMetadata(metadata)
	if err != nil {
		return http.StatusConflict, proxypkg.ProxyErrorWrongBinding, err
	}
	digest, err := clusterstate.ExecutionBindingDigest(opaque)
	if err != nil {
		return http.StatusConflict, proxypkg.ProxyErrorWrongBinding, err
	}
	if binding.Kind != kind || binding.ObjectID != objectID || binding.Group != group ||
		binding.NodeID != local.NodeID || binding.NodeEpoch != local.NodeEpoch ||
		binding.RegistryGeneration != request.Header.Get(proxypkg.HeaderRegistryGeneration) ||
		digest != request.Header.Get(proxypkg.HeaderBindingDigest) {
		return http.StatusConflict, proxypkg.ProxyErrorWrongBinding, errors.New("direct request does not match the protected execution Binding")
	}
	if kind == clusterstate.ExecutionKindSandbox &&
		(binding.RouteKey == "" || binding.RouteKey != request.Header.Get(clusterstate.DirectHeaderRouteKey)) {
		return http.StatusConflict, proxypkg.ProxyErrorWrongBinding, errors.New("direct request does not match the protected Route key")
	}
	if kind == clusterstate.ExecutionKindBuild && (binding.RouteKey != "" || request.Header.Get(clusterstate.DirectHeaderRouteKey) != "") {
		return http.StatusConflict, proxypkg.ProxyErrorWrongBinding, errors.New("Build direct request carries a Route key")
	}
	return 0, "", nil
}

func directBuildPathMatches(path, buildID, templateID string) bool {
	if id := directPathObjectID(path, "/builds/"); id != "" {
		return id == buildID && directPathObjectID(path, "/templates/") == templateID
	}
	return strings.Contains(path, "/files/") && directPathObjectID(path, "/templates/") == templateID
}

func directPathObjectID(path, marker string) string {
	index := strings.Index(path, marker)
	if index < 0 {
		return ""
	}
	value := path[index+len(marker):]
	if slash := strings.IndexByte(value, '/'); slash >= 0 {
		value = value[:slash]
	}
	return value
}
