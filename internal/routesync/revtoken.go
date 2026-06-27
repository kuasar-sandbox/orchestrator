package routesync

import (
	"fmt"
	"strconv"
	"strings"
)

// RevToken binds an incremental route subscription offset to a source fingerprint.
// The fingerprint is expected to change when the route source restarts with a
// fresh in-memory history, preventing an owner from replaying an offset against a
// different instance's changelog.
type RevToken struct {
	Fingerprint string
	Seq         int64
}

func MakeRevToken(fingerprint string, seq int64) string {
	if fingerprint == "" || seq <= 0 {
		return ""
	}
	return fingerprint + ":" + strconv.FormatInt(seq, 10)
}

func ParseRevToken(token string) (RevToken, error) {
	fp, raw, ok := strings.Cut(token, ":")
	if !ok || fp == "" || raw == "" {
		return RevToken{}, fmt.Errorf("routesync: invalid rev token")
	}
	seq, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || seq <= 0 {
		return RevToken{}, fmt.Errorf("routesync: invalid rev token")
	}
	return RevToken{Fingerprint: fp, Seq: seq}, nil
}

func CheckRevToken(token, fingerprint string) (int64, bool) {
	if token == "" || fingerprint == "" {
		return 0, false
	}
	parsed, err := ParseRevToken(token)
	if err != nil || parsed.Fingerprint != fingerprint {
		return 0, false
	}
	return parsed.Seq, true
}
