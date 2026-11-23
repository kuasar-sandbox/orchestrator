package store

import (
	"errors"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
)

func TestGroupListCursorAcceptsMaximumEscapedName(t *testing.T) {
	for _, character := range []string{"<", "&", `"`} {
		t.Run(character, func(t *testing.T) {
			s := testStore(t)
			pair := testKeyPair("1", "2")
			key := groupToken(t, pair)
			name := strings.Repeat(character, nodegroup.MaxNameBytes)
			for _, group := range []string{name, "~"} {
				_, _, err := s.GroupPut(t.Context(), key, group, 0, nodegroup.Context{}, &nodegroup.KeyPut{APISecret: pair.APISecret, ManifestKey: pair.ManifestKey})
				if err != nil {
					t.Fatal(err)
				}
			}
			first, err := s.GroupList(t.Context(), key, "", 1)
			if err != nil || len(first.Groups) != 1 || first.Groups[0].Group != name || len(first.NextCursor) <= 2*(nodegroup.MaxNameBytes+128) {
				t.Fatalf("escaped maximum-name page: groups=%d cursorBytes=%d err=%v", len(first.Groups), len(first.NextCursor), err)
			}
			second, err := s.GroupList(t.Context(), key, first.NextCursor, 1)
			if err != nil || len(second.Groups) != 1 || second.Groups[0].Group != "~" || second.NextCursor != "" {
				t.Fatalf("returned cursor cannot advance the page: %+v %v", second, err)
			}
			if _, err := s.GroupList(t.Context(), key, strings.Repeat("a", 16*nodegroup.MaxNameBytes), 1); !errors.Is(err, nodegroup.ErrInvalid) {
				t.Fatalf("unbounded cursor accepted: %v", err)
			}
		})
	}
}
