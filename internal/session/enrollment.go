package session

import (
	"context"
	"errors"
)

type NodeEnrollment struct {
	NodeID       string
	EnrollmentID string
	NodeEpoch    uint64
	DataEndpoint string
}

func (e NodeEnrollment) Validate() error {
	if e.NodeID == "" || e.EnrollmentID == "" || e.NodeEpoch == 0 || e.DataEndpoint == "" {
		return errors.New("session: incomplete node enrollment")
	}
	return nil
}

type IdentityRetirement struct {
	NodeID        string
	EnrollmentID  string
	LastNodeEpoch uint64
}

func (r IdentityRetirement) Validate() error {
	if r.NodeID == "" || r.EnrollmentID == "" || r.LastNodeEpoch == 0 {
		return errors.New("session: incomplete identity retirement")
	}
	return nil
}

// EnrollmentAuthority serializes registration installation and permanent
// retirement for each explicitly enrolled node identity. Implementations must
// reject unknown or retired identities and an epoch whose data endpoint differs
// from enrollment. The callback must run at most once while that identity is
// protected from the opposite operation; a completed retirement therefore
// cannot race with a previously validated registration installation. Before a
// callback starts, the authority may reject without calling it. Once started,
// the method must return that callback's result and cannot report a later error
// after the Holder has published the state change.
type EnrollmentAuthority interface {
	RunSessionRegistration(context.Context, Registration, func() error) error
	RunIdentityRetirement(context.Context, IdentityRetirement, func() (bool, error)) (bool, error)
}
