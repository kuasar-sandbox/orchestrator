package execsession

import (
	"errors"
	"math"
	"testing"
)

func TestExpiryUnix(t *testing.T) {
	const signingUnix = int64(1_800_000_000)
	for _, test := range []struct {
		name string
		ttl  int64
		want int64
	}{
		{name: "long lived", ttl: 0, want: 0},
		{name: "expiring", ttl: 37, want: signingUnix + 37},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := ExpiryUnix(signingUnix, test.ttl)
			if err != nil || got != test.want {
				t.Fatalf("ExpiryUnix() = %d, %v; want %d", got, err, test.want)
			}
		})
	}
}

func TestExpiryUnixRejectsArithmeticAndTimeRepresentationOverflow(t *testing.T) {
	// time.Time stores Unix seconds after adding the year-1-to-1970 offset.
	// The next second does not overflow int64 addition here, but it does wrap the
	// time.Time representation and compare before the signing instant.
	const unixToInternal = int64(62_135_596_800)
	const signingUnix = int64(1)
	maxRepresentable := int64(math.MaxInt64) - unixToInternal
	maxTTL := maxRepresentable - signingUnix
	if got, err := ExpiryUnix(signingUnix, maxTTL); err != nil || got != maxRepresentable {
		t.Fatalf("maximum representable expiry = %d, %v", got, err)
	}
	for _, test := range []struct {
		name    string
		signing int64
		ttl     int64
	}{
		{name: "negative ttl", signing: signingUnix, ttl: -1},
		{name: "invalid signing time", signing: 0, ttl: 1},
		{name: "time representation", signing: signingUnix, ttl: maxTTL + 1},
		{name: "addition", signing: signingUnix, ttl: math.MaxInt64},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ExpiryUnix(test.signing, test.ttl); !errors.Is(err, ErrInvalidTTL) {
				t.Fatalf("ExpiryUnix() error = %v, want invalid TTL", err)
			}
		})
	}
}
