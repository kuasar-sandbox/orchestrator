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

// EnrollmentAuthority checks registration against the explicitly enrolled node
// identity. Implementations must reject unknown or retired identities and an
// epoch whose data endpoint differs from enrollment. Permanent retirement must
// close registration before it can be confirmed, so Holders can then discard
// the corresponding tuple high watermark without allowing the identity back.
type EnrollmentAuthority interface {
	ValidateSessionRegistration(context.Context, NodeEnrollment) error
	ValidateIdentityRetirement(context.Context, IdentityRetirement) error
}
