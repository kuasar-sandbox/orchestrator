package cluster

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrHandoffRetry = errors.New("cluster: handoff retry")
	ErrHandoffMoved = errors.New("cluster: handoff moved")
)

type HandoffPhase string

const (
	HandoffStableOld HandoffPhase = "stable_old"
	HandoffPreparing HandoffPhase = "preparing"
	HandoffCatchup   HandoffPhase = "catchup"
	HandoffSwitching HandoffPhase = "switching"
	HandoffStableNew HandoffPhase = "stable_new"
	HandoffOldGrace  HandoffPhase = "old_grace"
)

type HandoffGate struct {
	Move        KeyMove
	FromVersion int64
	ToVersion   int64
	Phase       HandoffPhase
	GraceUntil  time.Time
}

type HandoffDecision struct {
	Allow      bool
	Retry      bool
	Moved      bool
	Version    int64
	NewOwners  []string
	RetryAfter time.Duration
	Reason     string
}

type HandoffError struct {
	Decision HandoffDecision
}

func (e HandoffError) Error() string {
	if e.Decision.Moved {
		return fmt.Sprintf("%s: version %d owners %v", ErrHandoffMoved, e.Decision.Version, e.Decision.NewOwners)
	}
	return fmt.Sprintf("%s: %s", ErrHandoffRetry, e.Decision.Reason)
}

func (e HandoffError) Unwrap() error {
	if e.Decision.Moved {
		return ErrHandoffMoved
	}
	return ErrHandoffRetry
}

func DecisionError(d HandoffDecision) error {
	if d.Allow {
		return nil
	}
	return HandoffError{Decision: d}
}

// DecideWrite applies the membership transition gate for one affected shard.
// It is intentionally transport-free: registry-member RPC can translate Retry
// and Moved decisions into HTTP/gRPC responses or local forwarding.
func (g HandoffGate) DecideWrite(requestVersion int64, now time.Time) HandoffDecision {
	switch g.Phase {
	case HandoffStableOld, HandoffPreparing, HandoffCatchup:
		if requestVersion == g.FromVersion {
			return HandoffDecision{Allow: true, Version: g.FromVersion}
		}
		if requestVersion == g.ToVersion {
			return HandoffDecision{Retry: true, Version: g.FromVersion, RetryAfter: 50 * time.Millisecond, Reason: "membership not flipped"}
		}
	case HandoffSwitching:
		return HandoffDecision{Retry: true, Version: g.FromVersion, RetryAfter: 50 * time.Millisecond, Reason: "membership switching"}
	case HandoffStableNew:
		if requestVersion == g.ToVersion {
			return HandoffDecision{Allow: true, Version: g.ToVersion}
		}
		if requestVersion == g.FromVersion {
			return HandoffDecision{Moved: true, Version: g.ToVersion, NewOwners: append([]string(nil), g.Move.To...), Reason: "membership moved"}
		}
	case HandoffOldGrace:
		if requestVersion == g.ToVersion {
			return HandoffDecision{Allow: true, Version: g.ToVersion}
		}
		if requestVersion == g.FromVersion && (g.GraceUntil.IsZero() || now.Before(g.GraceUntil)) {
			return HandoffDecision{Moved: true, Version: g.ToVersion, NewOwners: append([]string(nil), g.Move.To...), Reason: "membership moved"}
		}
	}
	return HandoffDecision{Retry: true, Version: g.ToVersion, RetryAfter: 100 * time.Millisecond, Reason: "membership version not accepted"}
}
