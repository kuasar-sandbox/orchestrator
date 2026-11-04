package types

import (
	"math"
	"testing"
	"time"
)

func TestIdleTimeoutBounds(t *testing.T) {
	if err := ValidateIdleTimeout(nil); err != nil {
		t.Fatal(err)
	}
	for _, n := range []int64{-1, 0, 1, math.MaxInt64 / int64(time.Second), math.MaxInt64/int64(time.Second) + 1, math.MaxInt64} {
		err := ValidateIdleTimeout(&n)
		want := n >= 0 && n <= math.MaxInt64/int64(time.Second)
		if (err == nil) != want {
			t.Fatal(n, err)
		}
	}
}
