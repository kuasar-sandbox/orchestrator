package execsession

import (
	"errors"
	"math"
	"time"
)

var ErrInvalidTTL = errors.New("invalid exec session ttlSeconds")

// ExpiryUnix converts a relative TTL at the node's actual signing time into the
// optional exp claim. It rejects both int64 addition overflow and values that
// wrap time.Time's internal representation.
func ExpiryUnix(signingUnix, ttlSeconds int64) (int64, error) {
	if signingUnix <= 0 || ttlSeconds < 0 || ttlSeconds > math.MaxInt64-signingUnix {
		return 0, ErrInvalidTTL
	}
	if ttlSeconds == 0 {
		return 0, nil
	}
	expiresUnix := signingUnix + ttlSeconds
	if !time.Unix(expiresUnix, 0).After(time.Unix(signingUnix, 0)) {
		return 0, ErrInvalidTTL
	}
	return expiresUnix, nil
}
