// Package envdsign validates envd /files pre-signed URLs.
package envdsign

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	EnvdPort  = 49983
	FilesPath = "/files"

	AccessTokenHeader = "X-Access-Token"

	OperationRead  = "read"
	OperationWrite = "write"
)

var (
	ErrInvalidAccessToken = errors.New("invalid access token")
	ErrMissingCredential  = errors.New("missing access token or file signature")
	ErrNotFileSignature   = errors.New("not an envd file signature request")
	ErrMissingSignature   = errors.New("missing signature")
	ErrInvalidExpiration  = errors.New("invalid signature_expiration")
	ErrInvalidSignature   = errors.New("invalid signature")
	ErrExpiredSignature   = errors.New("signature is already expired")
)

type Result struct {
	OK     bool
	Token  bool
	Signed bool
	Err    error
}

func Operation(method string) (string, bool) {
	switch method {
	case http.MethodGet:
		return OperationRead, true
	case http.MethodPost:
		return OperationWrite, true
	default:
		return "", false
	}
}

func IsFileSignatureCandidate(method, path string, port int) bool {
	if port != EnvdPort || path != FilesPath {
		return false
	}
	_, ok := Operation(method)
	return ok
}

func CheckDataPlaneAuth(r *http.Request, port int, accessToken string, now time.Time) Result {
	if accessToken == "" {
		return Result{OK: true}
	}

	if tok := r.Header.Get(AccessTokenHeader); tok != "" {
		if subtle.ConstantTimeCompare([]byte(tok), []byte(accessToken)) == 1 {
			return Result{OK: true, Token: true}
		}
		return Result{Err: ErrInvalidAccessToken}
	}

	if !IsFileSignatureCandidate(r.Method, r.URL.Path, port) {
		return Result{Err: ErrMissingCredential}
	}
	if err := ValidateFileRequest(r, accessToken, now); err != nil {
		return Result{Err: err}
	}
	return Result{OK: true, Signed: true}
}

func Signature(path, username, operation, accessToken string, expiration *int64) string {
	parts := []string{path, operation, username, accessToken}
	if expiration != nil {
		parts = append(parts, strconv.FormatInt(*expiration, 10))
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, ":")))
	return "v1_" + base64.RawStdEncoding.EncodeToString(sum[:])
}

func ValidateFileRequest(r *http.Request, accessToken string, now time.Time) error {
	operation, ok := Operation(r.Method)
	if !ok || r.URL.Path != FilesPath {
		return ErrNotFileSignature
	}
	q := r.URL.Query()
	got := q.Get("signature")
	if got == "" {
		return ErrMissingSignature
	}

	var exp *int64
	if _, ok := q["signature_expiration"]; ok {
		v, err := strconv.ParseInt(q.Get("signature_expiration"), 10, 64)
		if err != nil {
			return ErrInvalidExpiration
		}
		exp = &v
	}

	want := Signature(q.Get("path"), q.Get("username"), operation, accessToken, exp)
	if subtle.ConstantTimeCompare([]byte(want), []byte(got)) != 1 {
		return ErrInvalidSignature
	}
	if exp != nil && *exp < now.Unix() {
		return ErrExpiredSignature
	}
	return nil
}
