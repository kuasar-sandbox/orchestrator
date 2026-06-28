package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

const NodeOwnerRPCPath = "/internal/node-owner"

type nodeOwnerRequest struct {
	Op              string                    `json:"op"`
	NodeID          string                    `json:"node_id,omitempty"`
	Fingerprint     string                    `json:"fingerprint,omitempty"`
	ManifestKeyType string                    `json:"manifest_key_type,omitempty"`
	ManifestKey     string                    `json:"manifest_key,omitempty"`
	ManifestKeyRef  string                    `json:"manifest_key_ref,omitempty"`
	ExpiresUnix     int64                     `json:"expires_unix,omitempty"`
	BuildID         string                    `json:"build_id,omitempty"`
	Resources       *routesync.BuildResources `json:"resources,omitempty"`
	SID             string                    `json:"sid,omitempty"`
}

type nodeOwnerResponse struct {
	OK    bool        `json:"ok,omitempty"`
	Node  *NodeRecord `json:"node,omitempty"`
	Found bool        `json:"found,omitempty"`
	Error string      `json:"error,omitempty"`
}

func ServeNodeOwner(w http.ResponseWriter, req *http.Request, owner NodeOwner) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var in nodeOwnerRequest
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var out nodeOwnerResponse
	var err error
	switch in.Op {
	case "put_manifest_key":
		keyType, keyValue := in.ManifestKeyType, in.ManifestKey
		if keyType == "ref" {
			keyValue = in.ManifestKeyRef
		}
		err = owner.PutManifestKey(req.Context(), in.NodeID, in.Fingerprint, keyType, keyValue, in.ExpiresUnix)
		out.OK = err == nil
	case "drop_manifest_key":
		err = owner.DropManifestKey(req.Context(), in.NodeID, in.Fingerprint)
		out.OK = err == nil
	case "admit_build":
		out.OK = owner.AdmitBuild(req.Context(), in.NodeID, in.BuildID, in.Resources)
	case "release_build":
		owner.ReleaseBuild(req.Context(), in.BuildID)
		out.OK = true
	case "runtime":
		out.Node, out.Found, err = owner.Runtime(req.Context(), in.NodeID)
		out.OK = err == nil
	case "delete_sandbox":
		err = owner.DeleteSandbox(req.Context(), in.NodeID, in.SID)
		out.OK = err == nil
	default:
		err = fmt.Errorf("registry: unknown node-owner op %q", in.Op)
	}
	writeNodeOwnerResponse(w, out, err)
}

func writeNodeOwnerResponse(w http.ResponseWriter, out nodeOwnerResponse, err error) {
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		out.Error = err.Error()
		w.WriteHeader(http.StatusConflict)
	}
	_ = json.NewEncoder(w).Encode(&out)
}

type HTTPNodeOwner struct {
	endpoint string
	client   *http.Client
}

func NewHTTPNodeOwner(endpoint string, client *http.Client) *HTTPNodeOwner {
	if client == nil {
		client = http.DefaultClient
	}
	return &HTTPNodeOwner{endpoint: strings.TrimRight(endpoint, "/"), client: client}
}

func (o *HTTPNodeOwner) PutManifestKey(ctx context.Context, nodeID, fingerprint, keyType, keyValue string, expiresUnix int64) error {
	req := nodeOwnerRequest{Op: "put_manifest_key", NodeID: nodeID, Fingerprint: fingerprint, ManifestKeyType: keyType, ExpiresUnix: expiresUnix}
	if keyType == "ref" {
		req.ManifestKeyRef = keyValue
	} else {
		req.ManifestKey = keyValue
	}
	_, err := o.call(ctx, req)
	return err
}

func (o *HTTPNodeOwner) DropManifestKey(ctx context.Context, nodeID, fingerprint string) error {
	_, err := o.call(ctx, nodeOwnerRequest{Op: "drop_manifest_key", NodeID: nodeID, Fingerprint: fingerprint})
	return err
}

func (o *HTTPNodeOwner) AdmitBuild(ctx context.Context, nodeID, buildID string, want *routesync.BuildResources) bool {
	out, err := o.call(ctx, nodeOwnerRequest{Op: "admit_build", NodeID: nodeID, BuildID: buildID, Resources: want})
	return err == nil && out.OK
}

func (o *HTTPNodeOwner) ReleaseBuild(ctx context.Context, buildID string) {
	_, _ = o.call(ctx, nodeOwnerRequest{Op: "release_build", BuildID: buildID})
}

func (o *HTTPNodeOwner) Runtime(ctx context.Context, nodeID string) (*NodeRecord, bool, error) {
	out, err := o.call(ctx, nodeOwnerRequest{Op: "runtime", NodeID: nodeID})
	return out.Node, out.Found, err
}

func (o *HTTPNodeOwner) DeleteSandbox(ctx context.Context, nodeID, sid string) error {
	_, err := o.call(ctx, nodeOwnerRequest{Op: "delete_sandbox", NodeID: nodeID, SID: sid})
	return err
}

func (o *HTTPNodeOwner) call(ctx context.Context, in nodeOwnerRequest) (nodeOwnerResponse, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(&in); err != nil {
		return nodeOwnerResponse{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.endpoint+NodeOwnerRPCPath, &buf)
	if err != nil {
		return nodeOwnerResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := o.client.Do(req)
	if err != nil {
		return nodeOwnerResponse{}, err
	}
	defer resp.Body.Close()
	var out nodeOwnerResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nodeOwnerResponse{}, err
	}
	if out.Error != "" {
		return out, fmt.Errorf("%s", out.Error)
	}
	if resp.StatusCode >= 300 {
		return out, fmt.Errorf("registry: node-owner rpc status %s", resp.Status)
	}
	return out, nil
}
