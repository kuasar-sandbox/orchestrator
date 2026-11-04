package types

import (
	"fmt"
	"math"
	"time"
)

func ValidateIdleTimeout(seconds *int64) error {
	if seconds != nil && (*seconds < 0 || *seconds > math.MaxInt64/int64(time.Second)) {
		return fmt.Errorf("idleTimeout must be a nonnegative integer number of seconds representable as a duration")
	}
	return nil
}
func CloneIdleTimeout(v *int64) *int64 {
	if v == nil {
		return nil
	}
	c := *v
	return &c
}
