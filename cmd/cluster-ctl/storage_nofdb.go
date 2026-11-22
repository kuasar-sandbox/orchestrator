//go:build !fdb || !cgo

package main

import "errors"

func openFoundationDB(string, string) (metadataEngine, error) {
	return nil, errors.New("registry: FoundationDB requires the native fdb build; this binary supports SQLite and PostgreSQL")
}
