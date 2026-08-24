package util

import (
	"math"
	"testing"
)

func TestParseSizeExactDecimal(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  uint64
	}{
		{input: "4096", want: 4096},
		{input: "0.5KiB", want: 512},
		{input: "1.5GiB", want: 3 << 29},
		{input: "536875008B", want: 512<<20 + 4096},
		{input: "18446744073709551615", want: math.MaxUint64},
	} {
		got, err := ParseSize(tc.input)
		if err != nil {
			t.Fatalf("ParseSize(%q): %v", tc.input, err)
		}
		if got != tc.want {
			t.Fatalf("ParseSize(%q)=%d, want %d", tc.input, got, tc.want)
		}
	}
}

func TestParseSizeRejectsInexactAndOverflow(t *testing.T) {
	for _, input := range []string{
		"0.1B",
		"1.0000000000000000001B",
		"18446744073709551616",
		"1e3",
		"NaN",
		"Inf",
		"-1GiB",
		"1..0GiB",
	} {
		if got, err := ParseSize(input); err == nil {
			t.Fatalf("ParseSize(%q)=%d, want error", input, got)
		}
	}
}
