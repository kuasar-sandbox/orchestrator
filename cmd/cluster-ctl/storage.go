package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metastore"
	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/orchestrator/internal/storage"
	"github.com/kuasar-sandbox/orchestrator/internal/storage/sqlengine"
)

type metadataEngine interface {
	storage.Engine
	storage.Observer
}

func openMetadataDatabase(ctx context.Context, cfg *clustercfg.RegistryConfig) (*metastore.Store, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var engine metadataEngine
	var err error
	switch cfg.Storage.Backend {
	case "fdb":
		engine, err = openFoundationDB(cfg.FoundationDB.ClusterFile, cfg.Storage.Namespace)
	case "sqlite":
		engine, err = sqlengine.OpenSQLite(ctx, cfg.Storage.SQLitePath, cfg.Storage.Namespace)
	case "postgres":
		dsn := os.Getenv(cfg.Storage.PostgresDSNEnv)
		if dsn == "" {
			return nil, fmt.Errorf("registry: environment variable %s must contain the PostgreSQL connection string", cfg.Storage.PostgresDSNEnv)
		}
		engine, err = sqlengine.OpenPostgres(ctx, dsn, cfg.Storage.Namespace)
	default:
		return nil, fmt.Errorf("registry: unknown storage backend %q", cfg.Storage.Backend)
	}
	if err != nil {
		return nil, err
	}
	return metastore.New(engine, engine), nil
}
