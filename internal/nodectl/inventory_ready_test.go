package nodectl

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestControllerInventoryReadiness(t *testing.T) {
	dir := shortTestDir(t)
	state := makeState(4<<30, 0)
	socket := filepath.Join(dir, "resource.sock")
	recovered, proceed := make(chan struct{}), make(chan struct{})
	var once sync.Once
	resume := func() { once.Do(func() { close(proceed) }) }
	t.Cleanup(resume)
	server := &Server{Path: socket, State: state, Inventory: &Inventory{
		ControllerSocket: socket, CgroupScanPaths: []string{filepath.Join(dir, "cgroups")},
		Logf: func(format string, _ ...any) {
			if strings.HasPrefix(format, "inventory recovered ") {
				close(recovered)
				<-proceed
			}
		},
	}}
	if state.InventoryReady() {
		t.Fatal("new State was already authoritative")
	}
	done := make(chan error, 1)
	go func() { done <- server.Listen() }()
	select {
	case <-recovered:
	case err := <-done:
		t.Fatalf("Listen returned before inventory observation: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("inventory did not reach its completion boundary")
	}
	if state.InventoryReady() {
		t.Fatal("State became ready before inventory recovery returned")
	}
	resume()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop(); _ = server.Owner.Close() })
	if !state.InventoryReady() {
		t.Fatal("successful Listen did not publish recovered inventory")
	}
	if err := server.Stop(); err != nil {
		t.Fatal(err)
	}
	if state.InventoryReady() {
		t.Fatal("stopped controller remained ready")
	}
}

func TestControllerInventoryFailureNeverPublishesReadiness(t *testing.T) {
	dir := shortTestDir(t)
	socket := filepath.Join(dir, "resource.sock")
	state := makeState(4<<30, 0)
	if err := os.MkdirAll(retentionDir(socket), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(retentionDir(socket), "invalid.json"), []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	server := &Server{Path: socket, State: state, Inventory: &Inventory{ControllerSocket: socket}}
	if err := server.Listen(); err == nil {
		_ = server.Stop()
		_ = server.Owner.Close()
		t.Fatal("malformed durable inventory was accepted")
	}
	if state.InventoryReady() {
		t.Fatal("failed inventory recovery published an empty authoritative State")
	}
	if _, err := os.Lstat(socket); !os.IsNotExist(err) {
		t.Fatalf("failed inventory recovery created a listener: %v", err)
	}
}
