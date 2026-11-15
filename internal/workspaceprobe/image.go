// Package workspaceprobe supplies a small static Linux program to inspect
// workspace mount targets in a bare or E2B guest. Both binaries are generated
// from the assembly in source using the Go toolchain.
package workspaceprobe

//go:generate go run ./generate.go
