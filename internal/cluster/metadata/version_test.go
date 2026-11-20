package metadata

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"reflect"
	"testing"
)

func testVersion(commit uint64, order uint16) Version {
	var v Version
	binary.BigEndian.PutUint64(v[:8], commit)
	binary.BigEndian.PutUint16(v[8:], order)
	return v
}

func TestVersionKeepsBatchOrderAndWireIdentity(t *testing.T) {
	old, next := testVersion(100, 255), testVersion(100, 256)
	if !bytes.Equal(old[:8], next[:8]) || old.Compare(next) >= 0 || next.Compare(old) <= 0 {
		t.Fatal("complete batch order was lost")
	}
	for _, v := range []Version{{}, old, next, testVersion(101, 0)} {
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		var restored Version
		if err = json.Unmarshal(raw, &restored); err != nil || restored != v {
			t.Fatalf("version roundtrip: %s %v", raw, err)
		}
	}
}

func TestVersionRejectsTruncationAndIncompleteStamp(t *testing.T) {
	for _, wire := range []string{`1`, `null`, `"01"`, `"ffffffffffffffffffff"`, `"zz000000000000000000"`} {
		v := testVersion(5, 1)
		if err := json.Unmarshal([]byte(wire), &v); err == nil {
			t.Fatalf("accepted %s", wire)
		}
		if v != testVersion(5, 1) {
			t.Fatal("bad decode changed destination")
		}
	}
	if (Version{}).IsCommitted() {
		t.Fatal("missing version is not committed")
	}
	if !testVersion(5, 1).IsCommitted() {
		t.Fatal("valid stamp rejected")
	}
}

func TestDurableBuildSchemaExcludesRequestEnvelope(t *testing.T) {
	typ := reflect.TypeOf(BuildRecord{})
	for _, name := range []string{"registrationMMDSSecrets", "registrationCredentials", "registrationWorkspaces", "RegistrationMMDSSecrets", "RegistrationCredentials", "RegistrationWorkspaces"} {
		if _, exists := typ.FieldByName(name); exists {
			t.Fatalf("request-only field in durable schema: %s", name)
		}
	}
	for _, name := range []string{"RegistrationMMDSValuesDigest", "RegistrationCredentialsDigest", "RegistrationWorkspacesDigest"} {
		if _, exists := typ.FieldByName(name); !exists {
			t.Fatalf("missing durable digest: %s", name)
		}
	}
}

func TestDirectoryTokenPreservesEveryBucket(t *testing.T) {
	var token DirectoryToken
	token[0], token[255] = testVersion(12, 255), testVersion(12, 256)
	got, err := ParseDirectoryToken(token.String())
	if err != nil || got != token {
		t.Fatalf("directory token: %v", err)
	}
	if _, err = ParseDirectoryToken("invalid"); err == nil {
		t.Fatal("accepted malformed cursor")
	}
}
