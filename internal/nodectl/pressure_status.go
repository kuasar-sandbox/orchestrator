package nodectl

// PressureStatus is the local admin projection. It carries no credentials,
// snapshot contents, or operator placement instructions. Durations are nanoseconds.
type PressureStatus struct {
	Pools               []PoolSnapshot     `json:"pools,omitempty"`
	RunnerPools         []RunnerPoolStatus `json:"runner_pools,omitempty"`
	UnattributedRunners int                `json:"unattributed_runners,omitempty"`
	Enabled             bool               `json:"enabled"`
	WorkerBlocked       string             `json:"worker_blocked,omitempty"`
	PressureSnapshot
	Sandboxes []SandboxPressureStatus   `json:"sandboxes"`
	Recent    []PressureOperationResult `json:"recent_operations,omitempty"`
}

type SandboxPressureStatus struct {
	ResourcePool         string `json:"resource_pool,omitempty"`
	SourcePool           string `json:"source_pool,omitempty"`
	DestinationPool      string `json:"destination_pool,omitempty"`
	ID                   string `json:"sandbox_id"`
	State                string `json:"state"`
	PauseReason          string `json:"pause_reason,omitempty"`
	RecoveryObligation   bool   `json:"recovery_obligation"`
	Version              uint64 `json:"pressure_version"`
	RunningSinceUnixNano int64  `json:"running_since_unix_nano,omitempty"`
	WaitingSinceUnixNano int64  `json:"waiting_since_unix_nano,omitempty"`
}

type RunnerPoolStatus struct {
	Kind         string         `json:"kind"`
	Name         string         `json:"name"`
	ResourcePool string         `json:"resource_pool"`
	Unit         string         `json:"unit"`
	IdleTarget   int            `json:"idle_target"`
	Capacity     *int           `json:"capacity"` // null is unlimited
	Priority     int            `json:"priority"`
	Total        int            `json:"total"`
	Reserved     int            `json:"reserved"` // effective starts not covered by an idle/starting instance
	States       map[string]int `json:"states"`
}

type PressureOperationResult struct {
	SourcePool      string `json:"source_pool,omitempty"`
	DestinationPool string `json:"destination_pool,omitempty"`
	SandboxID       string `json:"sandbox_id"`
	Operation       string `json:"operation"`
	Result          string `json:"result"`
	AtUnixNano      int64  `json:"at_unix_nano"`
}
