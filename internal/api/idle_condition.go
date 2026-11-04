package api

import (
	"encoding/json"
	"regexp"
)

var idleTimestampPattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,9})?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$`)

func validIdleTimestamp(raw json.RawMessage) bool {
	if len(raw) == 0 || string(raw) == "null" {
		return true
	}
	var value string
	return json.Unmarshal(raw, &value) == nil && idleTimestampPattern.MatchString(value)
}
