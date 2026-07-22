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

func (o *Orchestrator) SetMMDSStoreValue(ctx context.Context, sid, name string, value []byte, contentType string, expiresUnix int64) error {
	err := mapMMDSAdminErr(o.st.SetMMDSStoreValue(ctx, sid, name, value, contentType, expiresUnix))
	o.mmdsAdminMetric("store", "put", err)
	if err == nil {
		o.mmdsAuth.Notify(sid, name)
		o.publishMmdsEntry(ctx, sid, name)
	}
	return err
}

func (o *Orchestrator) ClearMMDSStoreValue(ctx context.Context, sid, name string) error {
	err := mapMMDSAdminErr(o.st.ClearMMDSStoreValue(ctx, sid, name))
	o.mmdsAdminMetric("store", "delete", err)
	if err == nil {
		o.mmdsAuth.Notify(sid, name)
		o.publishMmdsEntry(ctx, sid, name)
	}
	return err
}

func (o *Orchestrator) SetMMDSRelayAuth(ctx context.Context, sid, name string, value []byte) error {
	err := mapMMDSAdminErr(o.st.SetMMDSRelayAuth(ctx, sid, name, value))
	o.mmdsAdminMetric("relay", "put", err)
	if err == nil {
		o.mmdsAuth.Notify(sid, name)
		o.publishMmdsEntry(ctx, sid, name)
	}
	return err
}

func (o *Orchestrator) ClearMMDSRelayAuth(ctx context.Context, sid, name string) error {
	err := mapMMDSAdminErr(o.st.ClearMMDSRelayAuth(ctx, sid, name))
	o.mmdsAdminMetric("relay", "delete", err)
	if err == nil {
		o.mmdsAuth.Notify(sid, name)
		o.publishMmdsEntry(ctx, sid, name)
	}
	return err
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
