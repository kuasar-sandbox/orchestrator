package orch

import (
	"errors"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// testCACertPEM is a self-signed X.509 certificate (CN=test-ca) parseable by
// x509.AppendCertsFromPEM. Mirrors the builder-test constant.
const testCACertPEM = `-----BEGIN CERTIFICATE-----
MIIDBTCCAe2gAwIBAgIUbWpRZGX/cczZpPOfcQzODmJMdNwwDQYJKoZIhvcNAQEL
BQAwEjEQMA4GA1UEAwwHdGVzdC1jYTAeFw0yNjA4MDQxMTU5MjNaFw0yNjA4MDUx
MTU5MjNaMBIxEDAOBgNVBAMMB3Rlc3QtY2EwggEiMA0GCSqGSIb3DQEBAQUAA4IB
DwAwggEKAoIBAQC2xO647J/yYuOueFHc2PXnAKvHkPNb4HbWH5FLPfe2nmvUd3bY
lbELV6teLY6yN+NmtvSJ63j2OF+RNIpC3ZFhsWrBDxEuP0juMqKnZ9WVnB/9KsLX
OjfohHY4k1qoQYFxT9yU8eJxmY82Wjd/yf5tV8xHC1zL4UAd0y7rOTyAvozBOXZv
uLr9K3eQgQfylpmP1tYwVoQVvUUf1D5fk5yD9vcUtok2e+7ktBN1f667URznf8hP
hLaQ5C2bZCgOOh78huTrcFqU9LORyk8AS/QYg8esglmrgy3y5iz0M4IN30csTqZN
SvKk9u/2eW26htiq5IOvKu0Xj5xW1wbOmvFlAgMBAAGjUzBRMB0GA1UdDgQWBBQQ
cFRflziG3f5nJyv8FwuE1d0LcTAfBgNVHSMEGDAWgBQQcFRflziG3f5nJyv8FwuE
1d0LcTAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQAoHtMT9F8m
HT+8tQke93pIbS9pq41PiOeBuDdF/yrgB5IKlVticAhzUCHHAG8UpEuqU2OjbF30
tIgwoUe1A+vNeanPjiOq3+rWADvMbgcleVWRUfxYlyxAdZ2yq+PfiqTI96UQIw3n
STeBSM7HZ6i/DqbAV+GvFaGmEC0OsOxOQAxPPuK8hFLU2eJ3HIdluW7stLcXtMe7
MuijqSVF8COlC+zKndt52yoJpU70bHZzLnEHYU7NvBeHgfUHqGBfvmyg3auGlSfE
peTd6+1IyyBTa6XbTg9wcMRPZE0uB+xsns0ArNR+jALUzNgoe7tBChAaFNyQPD1u
8NdJbsFlXbvO
-----END CERTIFICATE-----
`

func TestValidateRegistryTLS(t *testing.T) {
	tests := []struct {
		name         string
		reg          *types.BuildRegistryOptions
		fromTemplate bool
		wantErr      string // substring; "" = expect nil
	}{
		{
			name:    "nil registry allowed",
			reg:     nil,
			wantErr: "",
		},
		{
			name:    "nil tls allowed",
			reg:     &types.BuildRegistryOptions{TLS: nil},
			wantErr: "",
		},
		{
			name:    "empty tls rejected",
			reg:     &types.BuildRegistryOptions{TLS: &types.BuildRegistryTLSOptions{}},
			wantErr: "builder.registry.tls is empty",
		},
		{
			name:    "ca bundle accepted",
			reg:     &types.BuildRegistryOptions{TLS: &types.BuildRegistryTLSOptions{CABundlePEM: testCACertPEM}},
			wantErr: "",
		},
		{
			name:    "skip verify accepted",
			reg:     &types.BuildRegistryOptions{TLS: &types.BuildRegistryTLSOptions{InsecureSkipVerify: true}},
			wantErr: "",
		},
		{
			name:    "ca and skip verify mutually exclusive",
			reg:     &types.BuildRegistryOptions{TLS: &types.BuildRegistryTLSOptions{CABundlePEM: testCACertPEM, InsecureSkipVerify: true}},
			wantErr: "mutually exclusive",
		},
		{
			name:         "fromTemplate rejected",
			reg:          &types.BuildRegistryOptions{TLS: &types.BuildRegistryTLSOptions{InsecureSkipVerify: true}},
			fromTemplate: true,
			wantErr:      "applies only to fromImage builds",
		},
		{
			name:    "invalid pem rejected",
			reg:     &types.BuildRegistryOptions{TLS: &types.BuildRegistryTLSOptions{CABundlePEM: "not a certificate"}},
			wantErr: "no parseable X.509 certificate",
		},
		{
			name:    "plain text rejected",
			reg:     &types.BuildRegistryOptions{TLS: &types.BuildRegistryTLSOptions{CABundlePEM: "just some text\n"}},
			wantErr: "no parseable X.509 certificate",
		},
		{
			name:    "private key only rejected",
			reg:     &types.BuildRegistryOptions{TLS: &types.BuildRegistryTLSOptions{CABundlePEM: "-----BEGIN RSA PRIVATE KEY-----\n-----END RSA PRIVATE KEY-----\n"}},
			wantErr: "no parseable X.509 certificate",
		},
		{
			name:    "oversize ca rejected",
			reg:     &types.BuildRegistryOptions{TLS: &types.BuildRegistryTLSOptions{CABundlePEM: strings.Repeat("x", maxCABundlePEMSize+1)}},
			wantErr: "exceeds",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRegistryTLS(tt.reg, tt.fromTemplate)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validateRegistryTLS() unexpected err: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validateRegistryTLS() = nil, want error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("validateRegistryTLS() = %q, want error containing %q", err, tt.wantErr)
			}
			if !errors.Is(err, api.ErrBadRequest) {
				t.Fatalf("validateRegistryTLS() err not ErrBadRequest: %v", err)
			}
		})
	}
}
