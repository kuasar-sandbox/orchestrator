package metastore

import (
	"errors"
	"sync"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
)

func TestPublicListCursorKeyConformance(t *testing.T) {
	for name, factory := range backendCases {
		t.Run(name, func(t *testing.T) {
			t.Run("initialize_once_and_reopen", func(t *testing.T) {
				s := newBackendStore(t, factory(t))
				commits := 0
				s.afterNativeCommit = func(int) error { commits++; return nil }
				first, err := s.GetOrCreatePublicListCursorKey(t.Context())
				check(t, err)
				if first == ([32]byte{}) || commits != 1 {
					t.Fatalf("initialization: zero=%v commits=%d", first == ([32]byte{}), commits)
				}
				second, err := s.GetOrCreatePublicListCursorKey(t.Context())
				check(t, err)
				fresh := companion(t, s)
				reopened, err := fresh.GetOrCreatePublicListCursorKey(t.Context())
				check(t, err)
				if first != second || first != reopened || commits != 1 {
					t.Fatal("cursor key changed or existing key was rewritten")
				}
			})
			t.Run("concurrent_initializers_share_winner", func(t *testing.T) {
				s := newBackendStore(t, factory(t))
				peers := make([]*testStore, 8)
				for i := range peers {
					peers[i] = companion(t, s)
				}
				var wg sync.WaitGroup
				keys := make([][32]byte, len(peers))
				errs := make([]error, len(peers))
				start := make(chan struct{})
				for i, peer := range peers {
					wg.Add(1)
					go func() {
						defer wg.Done()
						<-start
						keys[i], errs[i] = peer.GetOrCreatePublicListCursorKey(t.Context())
					}()
				}
				close(start)
				wg.Wait()
				for i, key := range keys {
					check(t, errs[i])
					if key == ([32]byte{}) || key != keys[0] {
						t.Fatal("concurrent Registry initialized different cursor keys")
					}
				}
			})
			t.Run("pinned_conflict_reads_winner_without_replay", func(t *testing.T) {
				s := newBackendStore(t, factory(t))
				peer := companion(t, s)
				var winner [32]byte
				s.beforeRetry = func() error {
					var err error
					winner, err = peer.GetOrCreatePublicListCursorKey(t.Context())
					return err
				}
				commits := 0
				s.afterNativeCommit = func(int) error { commits++; return nil }
				key, err := s.GetOrCreatePublicListCursorKey(t.Context())
				check(t, err)
				if key != winner || winner == ([32]byte{}) || commits != 0 {
					t.Fatalf("initialization conflict did not read winner: commits=%d", commits)
				}
			})
			t.Run("unknown_commit_is_not_replayed_or_returned", func(t *testing.T) {
				s := newBackendStore(t, factory(t))
				commits := 0
				s.afterNativeCommit = func(int) error { commits++; return metadata.ErrUnknown }
				key, err := s.GetOrCreatePublicListCursorKey(t.Context())
				if !errors.Is(err, metadata.ErrUnknown) || commits != 1 || key != ([32]byte{}) {
					t.Fatalf("unknown initialization: zero=%v commits=%d err=%v", key == ([32]byte{}), commits, err)
				}
				// An independent request observes the committed record, with no
				// mutation even while the after-commit failure is still armed.
				key, err = s.GetOrCreatePublicListCursorKey(t.Context())
				check(t, err)
				if key == ([32]byte{}) || commits != 1 {
					t.Fatal("unknown outcome recovery rewrote cursor key")
				}
			})
			t.Run("removed_key_is_not_recreated", func(t *testing.T) {
				s := newBackendStore(t, factory(t))
				_, err := s.GetOrCreatePublicListCursorKey(t.Context())
				check(t, err)
				_, err = s.mutate(t.Context(), func(m *mutation) (bool, error) {
					clearRecord(m.tr, s.publicListCursorKeyRecord())
					return true, nil
				})
				check(t, err)
				commits := 0
				s.afterNativeCommit = func(int) error { commits++; return nil }
				key, err := s.GetOrCreatePublicListCursorKey(t.Context())
				if !errors.Is(err, metadata.ErrInvalidRecord) || key != ([32]byte{}) || commits != 0 {
					t.Fatalf("removed key was silently recreated: commits=%d err=%v", commits, err)
				}
			})
			t.Run("corrupt_key_fails_closed", func(t *testing.T) {
				s := newBackendStore(t, factory(t))
				_, err := s.mutate(t.Context(), func(m *mutation) (bool, error) {
					return true, putJSON(m.tr, s.publicListCursorKeyRecord(), publicListCursorKey{Key: make([]byte, 31)})
				})
				check(t, err)
				commits := 0
				s.afterNativeCommit = func(int) error { commits++; return nil }
				key, err := s.GetOrCreatePublicListCursorKey(t.Context())
				if !errors.Is(err, metadata.ErrInvalidRecord) || key != ([32]byte{}) || commits != 0 {
					t.Fatalf("corrupt key replaced: commits=%d err=%v", commits, err)
				}
			})
		})
	}
}
