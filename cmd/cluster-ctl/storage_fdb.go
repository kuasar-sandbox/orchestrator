//go:build fdb && cgo

package main

import "github.com/kuasar-sandbox/orchestrator/internal/fdbdriver"

func openFoundationDB(file, prefix string) (metadataEngine, error) {
	return fdbdriver.OpenEngine(fdbdriver.EngineOptions{ClusterFile: file, Prefix: prefix})
}
