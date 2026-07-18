package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
)

const linuxBootIDPath = "/proc/sys/kernel/random/boot_id"

func clusterIdentityCmd(args []string, _ *slog.Logger) error {
	if len(args) == 0 {
		return errors.New("usage: node-ctl cluster-identity <init|status> [flags]")
	}
	switch args[0] {
	case "init":
		return clusterIdentityInit(args[1:])
	case "status":
		return clusterIdentityStatus(args[1:])
	default:
		return fmt.Errorf("cluster-identity: unknown command %q (init|status)", args[0])
	}
}

func clusterIdentityInit(args []string) error {
	fs := flag.NewFlagSet("cluster-identity init", flag.ContinueOnError)
	cfgPath := fs.String("config", "/etc/node-ctl/conductor.yaml", "conductor config")
	nodeID := fs.String("node-id", "", "new stable node ID (defaults to configured cluster.node_id)")
	dataEndpoint := fs.String("data-endpoint", "", "stable Router-reachable endpoint (defaults to configured cluster.data_endpoint)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, st, err := openClusterIdentityStore(*cfgPath)
	if err != nil {
		return err
	}
	defer st.Close()
	if *nodeID == "" {
		*nodeID = cfg.Cluster.NodeID
	}
	if *dataEndpoint == "" {
		*dataEndpoint = cfg.Cluster.DataEndpoint
	}
	if *nodeID == "" || *dataEndpoint == "" {
		return errors.New("cluster-identity init requires explicit node-id and data-endpoint in flags or conductor config")
	}
	bootID, err := readBootID(linuxBootIDPath)
	if err != nil {
		return err
	}
	identity, err := st.EnrollClusterIdentity(context.Background(), *nodeID, bootID, *dataEndpoint)
	if err != nil {
		return err
	}
	return writeClusterIdentity(os.Stdout, identity)
}

func clusterIdentityStatus(args []string) error {
	fs := flag.NewFlagSet("cluster-identity status", flag.ContinueOnError)
	cfgPath := fs.String("config", "/etc/node-ctl/conductor.yaml", "conductor config")
	if err := fs.Parse(args); err != nil {
		return err
	}
	_, st, err := openClusterIdentityStore(*cfgPath)
	if err != nil {
		return err
	}
	defer st.Close()
	identity, err := st.GetClusterIdentity(context.Background())
	if err != nil {
		return err
	}
	return writeClusterIdentity(os.Stdout, identity)
}

func openClusterIdentityStore(path string) (*config.Config, *store.Store, error) {
	cfg, err := config.Load(path)
	if err != nil {
		return nil, nil, err
	}
	box, err := secretbox.NewFromColonHex(cfg.EncryptionKeySpec())
	if err != nil {
		return nil, nil, err
	}
	if err := os.MkdirAll(filepath.Dir(cfg.Paths.DBPath), 0o700); err != nil {
		return nil, nil, fmt.Errorf("cluster-identity: create database directory: %w", err)
	}
	st, err := store.Open(cfg.Paths.DBPath, box)
	if err != nil {
		return nil, nil, err
	}
	if err := os.Chmod(cfg.Paths.DBPath, 0o600); err != nil {
		st.Close()
		return nil, nil, fmt.Errorf("cluster-identity: protect database: %w", err)
	}
	return cfg, st, nil
}

func readBootID(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("cluster-identity: read boot ID: %w", err)
	}
	bootID := strings.TrimSpace(string(raw))
	if bootID == "" {
		return "", errors.New("cluster-identity: boot ID is empty")
	}
	return bootID, nil
}

func writeClusterIdentity(out *os.File, identity store.ClusterIdentity) error {
	response := struct {
		NodeID       string `json:"node_id"`
		EnrollmentID string `json:"enrollment_id"`
		NodeEpoch    uint64 `json:"node_epoch"`
		SessionSeq   uint64 `json:"session_seq"`
		BootID       string `json:"boot_id"`
		DataEndpoint string `json:"data_endpoint"`
	}{
		NodeID:       identity.NodeID,
		EnrollmentID: identity.EnrollmentID,
		NodeEpoch:    identity.NodeEpoch,
		SessionSeq:   identity.SessionSeq,
		BootID:       identity.BootID,
		DataEndpoint: identity.DataEndpoint,
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(response)
}
