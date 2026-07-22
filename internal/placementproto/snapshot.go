package placementproto

const LoadModelVersion uint16 = 1

type PlacementLoadSnapshot struct {
	NodeID           string `json:"node_id"`
	NodeEpoch        uint64 `json:"node_epoch"`
	SessionSeq       uint64 `json:"session_seq"`
	DataEndpoint     string `json:"data_endpoint"`
	SampleSeq        uint64 `json:"sample_seq"`
	LoadModelVersion uint16 `json:"load_model_version"`
	RuntimeDigest    string `json:"runtime_digest,omitempty"`
	CatalogDigest    string `json:"catalog_digest,omitempty"`

	Draining  bool   `json:"draining"`
	WaterZone string `json:"water_zone"`

	SandboxSlotCapacity       uint64 `json:"sandbox_slot_capacity"`
	SandboxSlotHardLimit      uint64 `json:"sandbox_slot_hard_limit"`
	SandboxSlotUsed           uint64 `json:"sandbox_slot_used"`
	SandboxQueueDepth         uint64 `json:"sandbox_queue_depth"`
	SandboxQueueLimit         uint64 `json:"sandbox_queue_limit"`
	SandboxRateTokenAvailable bool   `json:"sandbox_rate_token_available"`

	SandboxResourceController bool   `json:"sandbox_resource_controller"`
	NodeAllocatedMemory       uint64 `json:"node_allocated_memory"`
	AllocatablePoolMemory     uint64 `json:"allocatable_pool_memory"`
	BuildReservedMemory       uint64 `json:"build_reserved_memory"`
	EmergencyReservedMemory   uint64 `json:"emergency_reserved_memory"`
	StartupAllocatedMemory    uint64 `json:"startup_allocated_memory"`
	StartupPoolMemory         uint64 `json:"startup_pool_memory"`

	BuildSlotCapacity       uint64 `json:"build_slot_capacity"`
	BuildSlotHardLimit      uint64 `json:"build_slot_hard_limit"`
	BuildSlotsUsed          uint64 `json:"build_slots_used"`
	BuildCPUCapacity        uint64 `json:"build_cpu_capacity"`
	BuildCPUUsed            uint64 `json:"build_cpu_used"`
	BuildMemoryCapacity     uint64 `json:"build_memory_capacity"`
	BuildMemoryUsed         uint64 `json:"build_memory_used"`
	BuildStorageCapacity    uint64 `json:"build_storage_capacity"`
	BuildStorageUsed        uint64 `json:"build_storage_used"`
	BuildQueueDepth         uint64 `json:"build_queue_depth"`
	BuildQueueLimit         uint64 `json:"build_queue_limit"`
	BuildRateTokenAvailable bool   `json:"build_rate_token_available"`
}
