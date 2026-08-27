// Package limits centralizes every bound used by exec-condition admission.
package limits

import (
	"errors"
	"strings"
	"time"
)

const (
	MaxConditions         = 16
	MaxConditionExprBytes = 1024
	MaxConditionBytes     = 4096
	MaxExecTokenBytes     = 8 << 10

	MaxASTNodes                  = 512
	MaxASTDepth                  = 64
	MaxComprehensionDepth        = 8
	MaxStaticCost         uint64 = 100_000
	MaxRuntimeCost        uint64 = 10_000

	ProgramCacheCapacity = 256
	FirstRequestTimeout  = 10 * time.Second

	// StaticCostInputSize is the conservative cardinality supplied to CEL's
	// static estimator for request-backed strings, lists, and maps. Runtime
	// evaluation remains independently bounded by MaxRuntimeCost.
	StaticCostInputSize uint64 = 1024
)

var ErrInvalidConditions = errors.New("invalid exec conditions")

// ValidateExpressions enforces the condition-source wire bounds without
// parsing CEL. It never includes source text in returned errors.
func ValidateExpressions(expressions []string) error {
	if len(expressions) > MaxConditions {
		return ErrInvalidConditions
	}
	total := 0
	for _, expression := range expressions {
		if strings.TrimSpace(expression) == "" || len(expression) > MaxConditionExprBytes {
			return ErrInvalidConditions
		}
		total += len(expression)
		if total > MaxConditionBytes {
			return ErrInvalidConditions
		}
	}
	return nil
}

// NormalizeExpressions converts an empty slice to nil and returns a defensive
// copy after validating source bounds.
func NormalizeExpressions(expressions []string) ([]string, error) {
	if len(expressions) == 0 {
		return nil, nil
	}
	if err := ValidateExpressions(expressions); err != nil {
		return nil, err
	}
	return append([]string(nil), expressions...), nil
}
