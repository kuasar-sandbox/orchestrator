package filestore

import (
	"context"
	"net/url"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/testsecret"
)

func TestPluginStaticCredentialsAndProviderPriority(t *testing.T) {
	broker := testsecret.Start(t, map[string][]byte{"s3/access": []byte("resolved-access"), "s3/secret": []byte("resolved-secret"), "empty": {}})
	ctx := context.Background()
	cfg := &config.FilesStorageConfig{Region: "us-east-1", Bucket: "bucket", PresignExpiry: "1h", AccessKey: "@test/s3/access", SecretKey: "@test/s3/secret"}
	store, err := NewWithCredentials(ctx, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := store.cli.Options().Credentials.Retrieve(ctx)
	if err != nil || credentials.AccessKeyID != "resolved-access" || credentials.SecretAccessKey != "resolved-secret" || credentials.SessionToken != "" || credentials.CanExpire {
		t.Fatalf("credentials: %+v, %v", credentials, err)
	}
	broker.Set("s3/access", nil)
	broker.Set("s3/secret", nil)
	presigned, err := store.PresignPut(ctx, "template", "digest")
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(presigned)
	if err != nil || !strings.HasPrefix(u.Query().Get("X-Amz-Credential"), "resolved-access/") {
		t.Fatalf("presign did not retain resolved credentials: %v", err)
	}
	if broker.Count("s3/access") != 1 || broker.Count("s3/secret") != 1 || cfg.AccessKey != "@test/s3/access" || cfg.SecretKey != "@test/s3/secret" {
		t.Fatal("static client refetched credentials or rewrote configuration")
	}
	if _, err := NewWithCredentials(ctx, cfg, nil); err == nil {
		t.Fatal("missing plugin credentials did not fail")
	}
	provider := &sequenceProvider{values: []Credentials{{AccessKeyID: "provider-access", SecretAccessKey: "provider-secret"}}}
	store, err = NewWithCredentials(ctx, cfg, provider)
	if err != nil {
		t.Fatal(err)
	}
	credentials, err = store.cli.Options().Credentials.Retrieve(ctx)
	if err != nil || credentials.AccessKeyID != "provider-access" {
		t.Fatalf("authoritative provider lost priority: %v", err)
	}
	cfg.AccessKey, cfg.SecretKey = "literal-access", "@test/empty"
	if _, err := NewWithCredentials(ctx, cfg, nil); err == nil {
		t.Fatal("empty plugin credential was accepted")
	}
}

func TestNativeEmptySecretRetainsExistingCredentialBehavior(t *testing.T) {
	ctx := context.Background()
	cfg := &config.FilesStorageConfig{Region: "us-east-1", Bucket: "bucket", PresignExpiry: "1h", AccessKey: "native-access"}
	store, err := NewWithCredentials(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("native static validation moved into construction: %v", err)
	}
	if _, err := store.cli.Options().Credentials.Retrieve(ctx); err == nil {
		t.Fatal("empty native secret unexpectedly became usable")
	}
}
