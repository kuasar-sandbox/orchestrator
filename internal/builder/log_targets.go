package builder

import (
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/logtarget"
)

// buildJournalTarget supplies the identity of this build attempt to one host
// output. It does not reuse a phase sandbox identity or inherited environment.
func buildJournalTarget(s *configsock.BuildSpec, tag string) string {
	return logtarget.Format(tag, map[string]string{
		"KUASAR_BUILD_ID": s.BuildID,
		"KUASAR_RUN_ID":   s.RunID,
	})
}
