package cluster

// These are business service-routing keys, independent of database placement.
const NodeListRoutingKey = "node_list"

func ImportSourceRoutingKey(sourceID string) string {
	return "import/source/" + sourceID
}
