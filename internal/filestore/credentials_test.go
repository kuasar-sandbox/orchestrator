package filestore

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRuntimeCredentialsAdapterUsesPrimedValueThenRefreshes(t *testing.T) {
	next := Credentials{
		AccessKeyID: "refreshed", SecretAccessKey: "refreshed-secret", SessionToken: "session",
		CanExpire: true, Expires: time.Now().Add(time.Hour),
	}
	provider := &sequenceProvider{values: []Credentials{next}}
	first := Credentials{AccessKeyID: "initial", SecretAccessKey: "initial-secret"}
	adapter := &awsCredentialsProvider{provider: provider, first: &first}

	initial, err := adapter.Retrieve(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if initial.AccessKeyID != "initial" || provider.calls != 0 {
		t.Fatalf("initial=%+v provider calls=%d", initial, provider.calls)
	}
	refreshed, err := adapter.Retrieve(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if refreshed.AccessKeyID != "refreshed" || refreshed.SessionToken != "session" || !refreshed.CanExpire || provider.calls != 1 {
		t.Fatalf("refreshed=%+v provider calls=%d", refreshed, provider.calls)
	}
}

func TestRuntimeCredentialsNeverFallbackAfterProviderError(t *testing.T) {
	provider := &sequenceProvider{err: errors.New("credential service unavailable")}
	adapter := &awsCredentialsProvider{provider: provider}
	if _, err := adapter.Retrieve(context.Background()); err == nil || !strings.Contains(err.Error(), "credential service unavailable") {
		t.Fatalf("error=%v", err)
	}
}

func TestRuntimeCredentialsValidation(t *testing.T) {
	for name, value := range map[string]Credentials{
		"missing access": {SecretAccessKey: "secret"},
		"missing secret": {AccessKeyID: "access"},
		"missing expiry": {AccessKeyID: "access", SecretAccessKey: "secret", CanExpire: true},
		"expired":        {AccessKeyID: "access", SecretAccessKey: "secret", CanExpire: true, Expires: time.Now().Add(-time.Second)},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateCredentials(value); err == nil {
				t.Fatal("credentials accepted")
			}
		})
	}
}

type sequenceProvider struct {
	values []Credentials
	err    error
	calls  int
}

func (p *sequenceProvider) Retrieve(context.Context) (Credentials, error) {
	p.calls++
	if p.err != nil {
		return Credentials{}, p.err
	}
	value := p.values[0]
	p.values = p.values[1:]
	return value, nil
}
