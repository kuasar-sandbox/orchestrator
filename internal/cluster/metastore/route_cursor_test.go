package metastore

import (
	"errors"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
)

func TestRouteCursorRejectsInvalidIdentity(t *testing.T) {
	for name, factory := range backendCases {
		t.Run(name, func(t *testing.T) {
			s := newBackendStore(t, factory(t))
			scope := s.routeScope("g")
			for _, cursor := range []metadata.Cursor{
				"malformed", encodeCursor(s.routeScope("h"), s.routeScope("h").Child("route")),
				encodeCursor(scope, scope), encodeCursor(scope, scope.Child("")),
				encodeCursor(scope, scope.Child("route").Child("extra")),
				encodeCursor(scope, scope.Child(strings.Repeat("x", 4097))),
			} {
				if _, err := s.ListRoutes(t.Context(), "g", cursor, 100); !errors.Is(err, metadata.ErrInvalidCursor) {
					t.Fatalf("invalid cursor accepted: %v", err)
				}
			}
			// Long route keys use the existing hashed physical representation;
			// cursors must retain their complete logical identity across pages.
			for _, id := range []string{"ordinary", strings.Repeat("x", 257), strings.Repeat("y", 4096)} {
				cursor := encodeCursor(scope, scope.Child(id))
				if _, err := s.ListRoutes(t.Context(), "g", cursor, 100); err != nil {
					t.Fatalf("valid cursor length=%d rejected: %v", len(id), err)
				}
			}
		})
	}
}
