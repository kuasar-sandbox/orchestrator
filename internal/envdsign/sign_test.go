package envdsign

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestSignatureMatchesEnvdVectors(t *testing.T) {
	if got := Signature("/path/to/resource.txt", "user", OperationRead, "secret-access-token", nil); got != "v1_/0lpZh0CqgZvu5XIwq2wcliltum4wAS2H36jVjFApV0" {
		t.Fatalf("signature without expiration=%q", got)
	}
	exp := int64(1893456000)
	if got := Signature("/path/to/demo.txt", "root", OperationWrite, "secret-access-token", &exp); got != "v1_yfnoyV8dxo/VJbiiA4CMiUAqS0KqkipoZ9urGq7tPyI" {
		t.Fatalf("signature with expiration=%q", got)
	}
}

func TestCheckDataPlaneAuth(t *testing.T) {
	now := time.Unix(1700000000, 0)
	exp := int64(1893456000)
	sig := Signature("/tmp/a.txt", "", OperationRead, "tok", &exp)

	req := httptest.NewRequest(http.MethodGet, "/files?path=/tmp/a.txt&signature_expiration=1893456000&signature="+url.QueryEscape(sig), nil)
	res := CheckDataPlaneAuth(req, EnvdPort, "tok", now)
	if !res.OK || !res.Signed || res.Token {
		t.Fatalf("signed auth result=%+v", res)
	}

	req = httptest.NewRequest(http.MethodGet, "/files?path=/tmp/a.txt&signature_expiration=1893456000&signature="+url.QueryEscape(sig), nil)
	req.Header.Set(AccessTokenHeader, "wrong")
	res = CheckDataPlaneAuth(req, EnvdPort, "tok", now)
	if res.OK || res.Err != ErrInvalidAccessToken {
		t.Fatalf("wrong header should not fall back to signature: %+v", res)
	}

	req = httptest.NewRequest(http.MethodGet, "/health?signature="+sig, nil)
	res = CheckDataPlaneAuth(req, EnvdPort, "tok", now)
	if res.OK || res.Err != ErrMissingCredential {
		t.Fatalf("non-file signature result=%+v", res)
	}
}

func TestValidateFileRequestExpiration(t *testing.T) {
	exp := int64(100)
	sig := Signature("/tmp/a.txt", "", OperationRead, "tok", &exp)
	req := httptest.NewRequest(http.MethodGet, "/files?path=/tmp/a.txt&signature_expiration=100&signature="+url.QueryEscape(sig), nil)
	if err := ValidateFileRequest(req, "tok", time.Unix(101, 0)); err != ErrExpiredSignature {
		t.Fatalf("expired err=%v", err)
	}
}
