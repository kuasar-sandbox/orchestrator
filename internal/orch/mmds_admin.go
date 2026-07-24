package orch

// MMDS endpoints admin plane: thin store/relay-auth wrappers the local
// control socket's mmds admin routes (internal/configsock) call, mirroring
// admin.go's manifest-key wrappers. Errors are mapped from internal/store's
// sentinel errors to internal/configsock's so configsock does not need to
// import the store package to distinguish "not found"/"wrong backend" (404)
// from a genuine failure (500).

import (
	"context"
	"errors"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/mmdsauth"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
)

// MMDSAuthority exposes the shared internal-mode endpoint authority so
// cmd/node-ctl can pass the same instance into mmds.New() that this file's
// admin mutations wake via Notify — a PUT/DELETE landing while a guest GET
// is parked must wake it immediately, not just on timeout.
func (o *Orchestrator) MMDSAuthority() *mmdsauth.Authority { return o.mmdsAuth }

// releaseMmdsEndpoints tells external proxies that sandboxID's declared MMDS
// endpoints are gone and releases their (sandbox_id,name) state from
// mmdsAuth/mmdsRelay — the common cleanup every sandbox-row-delete site
// (Kill, ExportSandbox's move path) needs once sandboxID's
// sandbox_mmds_endpoints rows are gone for good (via FK cascade): without
// the publish, an external proxy's cached secret plaintext for this
// sandbox_id only gets swept on some later, unrelated full resync; without
// the release, sandboxID can never again receive the admin mutation that
// would otherwise be the only thing able to free it from those two
// node-wide, otherwise-unpruned maps. mmdsEndpoints is the endpoint list
// snapshotted before the delete (nothing is left to list afterward).
func (o *Orchestrator) releaseMmdsEndpoints(sandboxID string, mmdsEndpoints []store.MMDSEndpointStatus) {
	for _, ep := range mmdsEndpoints {
		o.publishMmdsDelete(sandboxID, ep.Name)
	}
	if len(mmdsEndpoints) > 0 {
		o.mmdsAuth.ForgetSandbox(sandboxID)
		if o.mmdsRelay != nil {
			o.mmdsRelay.ForgetSandbox(sandboxID)
		}
	}
}

func (o *Orchestrator) SetMMDSStoreValue(ctx context.Context, sid, name string, value []byte, contentType string, expiresUnix int64) (int64, error) {
	revision, err := o.st.SetMMDSStoreValue(ctx, sid, name, value, contentType, expiresUnix)
	err = mapMMDSAdminErr(err)
	o.mmdsAdminMetric("store", "put", err)
	if err == nil {
		o.mmdsAuth.Notify(sid, name)
		o.publishMmdsEntry(ctx, sid, name)
	}
	return revision, err
}

func (o *Orchestrator) ClearMMDSStoreValue(ctx context.Context, sid, name string) (int64, error) {
	revision, err := o.st.ClearMMDSStoreValue(ctx, sid, name)
	err = mapMMDSAdminErr(err)
	o.mmdsAdminMetric("store", "delete", err)
	if err == nil {
		o.mmdsAuth.Notify(sid, name)
		o.publishMmdsEntry(ctx, sid, name)
	}
	return revision, err
}

func (o *Orchestrator) SetMMDSRelayAuth(ctx context.Context, sid, name string, value []byte) (int64, error) {
	revision, err := o.st.SetMMDSRelayAuth(ctx, sid, name, value)
	err = mapMMDSAdminErr(err)
	o.mmdsAdminMetric("relay", "put", err)
	if err == nil {
		o.mmdsAuth.Notify(sid, name)
		o.publishMmdsEntry(ctx, sid, name)
	}
	return revision, err
}

func (o *Orchestrator) ClearMMDSRelayAuth(ctx context.Context, sid, name string) (int64, error) {
	revision, err := o.st.ClearMMDSRelayAuth(ctx, sid, name)
	err = mapMMDSAdminErr(err)
	o.mmdsAdminMetric("relay", "delete", err)
	if err == nil {
		o.mmdsAuth.Notify(sid, name)
		o.publishMmdsEntry(ctx, sid, name)
	}
	return revision, err
}

// mmdsAdminMetric records one mmds_admin_mutations_total{backend_type,op,result}
// observation. result is a bounded enum derived from err's shape, never
// error text (only bounded enums may be used as metric labels).
func (o *Orchestrator) mmdsAdminMetric(backendType, op string, err error) {
	result := "ok"
	switch {
	case errors.Is(err, configsock.ErrMMDSEndpointNotFound), errors.Is(err, configsock.ErrMMDSEndpointWrongBackend):
		result = "not_found"
	case err != nil:
		result = "error"
	}
	o.mx.Inc(`mmds_admin_mutations_total{backend_type="` + backendType + `",op="` + op + `",result="` + result + `"}`)
}

func mapMMDSAdminErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrMMDSEndpointNotFound):
		return configsock.ErrMMDSEndpointNotFound
	case errors.Is(err, store.ErrMMDSEndpointWrongBackend):
		return configsock.ErrMMDSEndpointWrongBackend
	default:
		return err
	}
}
