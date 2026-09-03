package proxyapp

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"strings"
	"testing"

	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
)

func TestFreezeConfigDetachesAndCanonicalizes(t *testing.T) {
	cfg := testProxyConfig(t)
	effective, err := FreezeConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	wantDigest := sha256.Sum256(effective.raw)
	if effective.digest != wantDigest || effective.Digest() != wantDigest {
		t.Fatal("effective digest mismatch")
	}
	cfg.Paths.RunRoot = "/mutated"
	cfg.Traffic.MaxInflight.Total = 1
	first := effective.Config()
	if first.Paths.RunRoot != "/run/test-proxy" || first.Traffic.MaxInflight.Total != 32 || first.Traffic.MaxInflight.Exec != 2 {
		t.Fatalf("detached config=%+v traffic=%+v", first.Paths, first.Traffic)
	}
	first.Paths.RunRoot = "/also-mutated"
	first.Traffic.MaxInflight.Exec = 9
	if got := effective.Config(); got.Paths.RunRoot != "/run/test-proxy" || got.Traffic.MaxInflight.Exec != 2 {
		t.Fatalf("Config returned shared state: %+v", got)
	}
	var round publicconfig.Proxy
	if err := json.Unmarshal(effective.raw, &round); err != nil {
		t.Fatal(err)
	}
	if round.Paths.RunRoot != "/run/test-proxy" || round.Traffic.MaxInflight.Total != 32 || round.Traffic.MaxInflight.Exec != 2 {
		t.Fatalf("round trip paths=%+v traffic=%+v", round.Paths, round.Traffic)
	}
}

func TestEffectiveConfigFromRawRejectsTamperAndNonCanonicalJSON(t *testing.T) {
	effective, err := FreezeConfig(testProxyConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := effectiveConfigFromRaw(effective.raw, effective.digest); err != nil {
		t.Fatal(err)
	}
	tampered := append([]byte(nil), effective.raw...)
	tampered[len(tampered)-2] ^= 1
	if _, err := effectiveConfigFromRaw(tampered, effective.digest); err == nil {
		t.Fatal("tampered config accepted")
	}
	var indented bytes.Buffer
	if err := json.Indent(&indented, effective.raw, "", "  "); err != nil {
		t.Fatal(err)
	}
	pretty := indented.Bytes()
	if _, err := effectiveConfigFromRaw(pretty, sha256.Sum256(pretty)); err == nil || !strings.Contains(err.Error(), "not canonical") {
		t.Fatalf("non-canonical error=%v", err)
	}
	unknown := append(append([]byte(nil), effective.raw[:len(effective.raw)-1]...), []byte(`,"unknown":true}`)...)
	if _, err := effectiveConfigFromRaw(unknown, sha256.Sum256(unknown)); err == nil {
		t.Fatal("unknown field accepted")
	}
}

func testProxyConfig(t *testing.T) *publicconfig.Proxy {
	t.Helper()
	cfg, err := publicconfig.DecodeProxy(strings.NewReader("paths:\n  run_root: /run/test-proxy\ndata_listen: 127.0.0.1:0\nworkers: 2\nroute_capacity: 16\nauth: enforce\npark_timeout: 1s\ntraffic:\n  max_inflight:\n    total: 32\n    exec: 2\n"))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}
