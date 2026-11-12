package routesync

import (
	"errors"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
)

// HoldActivity is the authority's absolute count. A zero with no timestamps
// denotes a sandbox that has never used hold. A release carries the paired
// wall and boot clocks of the new zero interval even if no busy update was seen.
type HoldActivity struct {
	Value           int64      `json:"value"`
	IdleSince       *time.Time `json:"idle_since,omitempty"`
	IdleSinceBootNS int64      `json:"idle_since_boot_ns,omitempty"`
}

func (h *HoldActivity) Valid() bool {
	return h != nil && h.Value >= 0 && ((h.IdleSince == nil && h.IdleSinceBootNS == 0) ||
		(h.IdleSince != nil && !h.IdleSince.IsZero() && h.IdleSinceBootNS > 0))
}

// A partial object must never decode as the legitimate never-used zero.
func (h *HoldActivity) UnmarshalJSON(data []byte) error {
	var wire struct {
		Value           *int64     `json:"value"`
		IdleSince       *time.Time `json:"idle_since,omitempty"`
		IdleSinceBootNS int64      `json:"idle_since_boot_ns,omitempty"`
	}
	if err := strictjson.Decode(data, &wire); err != nil {
		return err
	}
	if wire.Value == nil {
		return errors.New("routesync: missing hold value")
	}
	decoded := HoldActivity{Value: *wire.Value, IdleSince: wire.IdleSince, IdleSinceBootNS: wire.IdleSinceBootNS}
	if !decoded.Valid() {
		return errors.New("routesync: invalid hold value or idle time")
	}
	*h = decoded
	return nil
}
