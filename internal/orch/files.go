package orch

import (
	"context"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/api"
)

// FilesUpload backs GET /templates/{tid}/files/{hash}: it authorizes the build
// behind the (transient) template id, reports whether the COPY context object
// is already in the store, and returns a presigned PUT URL for the client to
// upload it to. The object key is scoped to this template id; the ownership
// check here is what prevents a tenant from minting an upload URL for another
// tenant's build (the bucket itself is private — no creds reach the client).
func (o *Orchestrator) FilesUpload(ctx context.Context, apiKey, templateID, hash string) (bool, string, error) {
	if o.files == nil {
		return false, "", api.ErrFilesUnsupported
	}
	b, err := o.st.GetBuildByTemplateID(ctx, templateID)
	if err != nil {
		return false, "", err
	}
	if b == nil || !ownsBuild(b, apiKey) {
		return false, "", api.ErrNotFound // unknown or not-owned → 404 (no ownership leak)
	}
	present, err := o.files.Exists(ctx, templateID, hash)
	if err != nil {
		return false, "", err
	}
	url, err := o.files.PresignPut(ctx, templateID, hash)
	if err != nil {
		return false, "", err
	}
	return present, url, nil
}
