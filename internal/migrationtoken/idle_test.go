package migrationtoken

import "testing"

func TestIdleTimeoutMigrationRoundTrip(t *testing.T) {
	zero, five := int64(0), int64(5)
	for _, value := range []*int64{nil, &zero, &five} {
		p := validPayload()
		p.IdleTimeout = value
		token, err := Seal(testMaterial, p)
		if err != nil {
			t.Fatal(err)
		}
		got, err := Open(testMaterial, token)
		if err != nil {
			t.Fatal(err)
		}
		if (got.IdleTimeout == nil) != (value == nil) || (value != nil && *got.IdleTimeout != *value) {
			t.Fatal(got.IdleTimeout)
		}
	}
}
