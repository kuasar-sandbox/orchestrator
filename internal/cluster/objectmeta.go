package cluster

// ObjectMetadataKey carries the system-owned ExecutionBinding in Sandbox and
// Build metadata. Nodes persist the value opaquely; only cluster components
// interpret it.
const ObjectMetadataKey = "kuasar-sandbox.cluster"

func cloneStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}
