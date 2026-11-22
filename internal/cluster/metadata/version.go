package metadata

import "github.com/kuasar-sandbox/orchestrator/internal/storage/record"

// Version orders revisions of one key across deletion/recreation within one
// database namespace. It is not a business generation or a global commit clock.
type Version = record.Version

type Versioned[T any] struct {
	Exists  bool
	Version Version
	Value   T
}
