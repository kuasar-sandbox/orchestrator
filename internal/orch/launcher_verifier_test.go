package orch

import (
	"context"
	"errors"
)

var errFakeCannotVerify = errors.New("fake launcher cannot verify runner processes")

// Unrelated launcher fakes conservatively decline process-empty proof. Tests
// exercising runner exit use exitedRunnerLauncher with an explicit result.
func (*assignmentOrderLauncher) UnitEmpty(context.Context, string) (bool, error) {
	return false, errFakeCannotVerify
}
func (*countingLauncher) UnitEmpty(context.Context, string) (bool, error) {
	return false, errFakeCannotVerify
}
func (*multiPoolLauncher) UnitEmpty(context.Context, string) (bool, error) {
	return false, errFakeCannotVerify
}
func (*orderedCleanupLauncher) UnitEmpty(context.Context, string) (bool, error) {
	return false, errFakeCannotVerify
}
func (*reconcileLauncher) UnitEmpty(context.Context, string) (bool, error) {
	return false, errFakeCannotVerify
}
func (*runPoolTestLauncher) UnitEmpty(context.Context, string) (bool, error) {
	return false, errFakeCannotVerify
}
func (*sandboxFinalizerLauncher) UnitEmpty(context.Context, string) (bool, error) {
	return false, errFakeCannotVerify
}
