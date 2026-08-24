// Package util holds tiny utilities used by multiple top-level
// packages (manifest tree, store, cache, nodectl) that don't fit any
// single domain. Keep this package narrow — only universal helpers
// belong here.
package util

import (
	"fmt"
	"math/big"
	"strings"
)

// ParseSize parses a human-readable byte size string such as "128KiB",
// "512KB", "1MiB", or a bare decimal ("4096" = 4096 bytes). Returns
// the size in bytes.
//
// Recognised suffixes (case-insensitive):
//
//	B  (or no suffix)  → 1
//	KiB / KB / K       → 1024
//	MiB / MB / M       → 1024 * 1024
//	GiB / GB / G       → 1024 * 1024 * 1024
//	TiB / TB / T       → 1024 * 1024 * 1024 * 1024
//
// The IEC (KiB/MiB/…) and short (KB/MB/…) forms are both treated as
// powers of 1024 because every caller in this repository uses them
// interchangeably for in-memory or on-disk capacities, never for
// wire-rate units. Decimal (KB = 1000) semantics are not supported.
func ParseSize(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("util: empty size")
	}
	// Byte sizes must have one exact integer representation. Deliberately do
	// not accept signs, exponents, NaN or Inf through a floating-point parser.
	i := 0
	dots := 0
	for i < len(s) && (s[i] == '.' || (s[i] >= '0' && s[i] <= '9')) {
		if s[i] == '.' {
			dots++
		}
		i++
	}
	num, unit := s[:i], strings.TrimSpace(strings.ToLower(s[i:]))
	if num == "" || dots > 1 {
		return 0, fmt.Errorf("util: size %q missing numeric part", s)
	}
	var mult uint64
	switch unit {
	case "", "b":
		mult = 1
	case "k", "kb", "kib":
		mult = 1 << 10
	case "m", "mb", "mib":
		mult = 1 << 20
	case "g", "gb", "gib":
		mult = 1 << 30
	case "t", "tb", "tib":
		mult = 1 << 40
	default:
		return 0, fmt.Errorf("util: unknown size unit %q in %q", unit, s)
	}

	parts := strings.Split(num, ".")
	whole := parts[0]
	fraction := ""
	if len(parts) == 2 {
		fraction = parts[1]
	}
	if whole == "" {
		whole = "0"
	}
	if fraction == "" && strings.HasSuffix(num, ".") {
		return 0, fmt.Errorf("util: bad size %q", s)
	}
	digits := whole + fraction
	if digits == "" {
		return 0, fmt.Errorf("util: bad size %q", s)
	}
	numerator, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		return 0, fmt.Errorf("util: bad size %q", s)
	}
	numerator.Mul(numerator, new(big.Int).SetUint64(mult))
	denominator := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(len(fraction))), nil)
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(numerator, denominator, remainder)
	if remainder.Sign() != 0 {
		return 0, fmt.Errorf("util: size %q is not an exact whole-byte value", s)
	}
	if !quotient.IsUint64() {
		return 0, fmt.Errorf("util: size %q overflows uint64 bytes", s)
	}
	return quotient.Uint64(), nil
}
