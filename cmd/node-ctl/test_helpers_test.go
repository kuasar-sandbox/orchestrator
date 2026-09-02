package main

import (
	"os"
	"testing"
)

// shortNodeCtlTestDir keeps generated Unix socket paths within sun_path even
// when the test name itself is long.
func shortNodeCtlTestDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "nc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}
