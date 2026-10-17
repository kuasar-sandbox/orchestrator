package proxyapp

import (
	"net"
	"os"
	"path/filepath"
)

func listenUnix(path string) (net.Listener, error) {
	return listenUnixMode(path, false)
}

func listenUnixMode(path string, exclusive bool) (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	if !exclusive {
		_ = os.Remove(path)
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = listener.Close()
		return nil, err
	}
	return listener, nil
}
