package orch

import (
	"github.com/kuasar-sandbox/orchestrator/internal/logtarget"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// sandboxJournalTarget projects the current task identity into one complete
// output target. StableID's default belongs to the domain model, not the runtime.
func sandboxJournalTarget(tag string, sb *types.Sandbox) string {
	return logtarget.Format(tag, map[string]string{
		"KUASAR_STABLE_ID":  sb.StableID(),
		"KUASAR_SANDBOX_ID": sb.ID,
		"KUASAR_RUN_ID":     sb.RunID,
	})
}
