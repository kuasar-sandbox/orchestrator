package metastore

import (
	"errors"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/storage"
	records "github.com/kuasar-sandbox/orchestrator/internal/storage/record"
)

func TestCurrentGuardPreservesRecordConditions(t *testing.T) {
	for name, factory := range backendCases {
		t.Run(name, func(t *testing.T) {
			for _, condition := range []string{"Get", "Expect"} {
				t.Run(condition, func(t *testing.T) {
					s := newBackendStore(t, factory(t))
					node := registerNode(t, s, "node")
					other := companion(t, s)
					s.beforeRetry = func() error {
						value := node.Value
						value.LastHeartbeatUnix++
						_, err := other.UpdateProfile(t.Context(), node.Registration, node.Version, value)
						return err
					}
					attempts := 0
					_, err := s.records.Update(t.Context(), func(tr *records.Tx) (bool, error) {
						attempts++
						if attempts == 1 {
							var err error
							if condition == "Get" {
								_, err = tr.Get(s.profileKey(node.Value.NodeID))
							} else {
								_, err = tr.Expect(s.profileKey(node.Value.NodeID), node.Version)
							}
							if err != nil {
								return false, err
							}
						}
						// A current guard on the second attempt must not erase a
						// condition established by the first physical attempt.
						if err := tr.CheckCurrent(s.profileKey(node.Value.NodeID), func(records.Value) error { return nil }); err != nil {
							return false, err
						}
						return true, tr.Put(s.key("guard-test"), []byte("committed"))
					})
					if !errors.Is(err, metadata.ErrConflict) || attempts != 2 {
						t.Fatalf("current guard weakened %s: attempts=%d error=%v", condition, attempts, err)
					}
					check(t, s.read(t.Context(), func(tr *records.Tx) error {
						row, err := tr.Get(s.key("guard-test"))
						if row.Exists {
							t.Fatal("rejected condition left a committed write")
						}
						return err
					}))
				})
			}
			t.Run("staged_record_is_rejected", func(t *testing.T) {
				s := newBackendStore(t, factory(t))
				key := s.key("guard-test")
				called := false
				_, err := s.records.Update(t.Context(), func(tr *records.Tx) (bool, error) {
					if err := tr.Put(key, []byte("staged")); err != nil {
						return false, err
					}
					return true, tr.CheckCurrent(key, func(records.Value) error { called = true; return nil })
				})
				if !errors.Is(err, storage.ErrInvalid) || called {
					t.Fatalf("guard read staged state: called=%v error=%v", called, err)
				}
				check(t, s.read(t.Context(), func(tr *records.Tx) error {
					row, err := tr.Get(key)
					if row.Exists {
						t.Fatal("rejected staged guard left a write")
					}
					return err
				}))
			})
		})
	}
}

// A normal FoundationDB read must still create a physical read conflict. Unlike
// the deterministic abort hook above, this interleaves a real committed profile
// update between the guard read and the transaction's commit.
func TestCurrentGuardFoundationDBReadConflict(t *testing.T) {
	factory, enabled := backendCases["fdb"]
	if !enabled {
		t.Skip("requires the real FoundationDB backend")
	}
	for _, draining := range []bool{false, true} {
		name := "heartbeat_remains_eligible"
		if draining {
			name = "draining_rejects_retry"
		}
		t.Run(name, func(t *testing.T) {
			s := newBackendStore(t, factory(t))
			node := registerNode(t, s, "node")
			other := companion(t, s)
			attempts := 0
			_, err := s.records.Update(t.Context(), func(tr *records.Tx) (bool, error) {
				attempts++
				err := tr.CheckCurrent(s.profileKey(node.Value.NodeID), func(raw records.Value) error {
					profile, err := typed[metadata.NodeProfileRecord](wrapRecord(raw))
					if err != nil {
						return err
					}
					if !profile.Exists || profile.Value.Draining {
						return metadata.ErrConflict
					}
					if attempts == 1 {
						value := node.Value
						value.LastHeartbeatUnix++
						value.Draining = draining
						_, err = other.UpdateProfile(t.Context(), node.Registration, node.Version, value)
						return err
					}
					return nil
				})
				if err != nil {
					return false, err
				}
				return true, tr.Put(s.key("guard-test"), []byte("committed"))
			})
			if attempts < 2 {
				t.Fatalf("profile read did not conflict with concurrent write: attempts=%d error=%v", attempts, err)
			}
			if draining {
				if !errors.Is(err, metadata.ErrConflict) {
					t.Fatalf("draining update was not rechecked: %v", err)
				}
			} else {
				check(t, err)
			}
			check(t, s.read(t.Context(), func(tr *records.Tx) error {
				row, err := tr.Get(s.key("guard-test"))
				if row.Exists == draining {
					t.Fatalf("unexpected committed guard result: exists=%v draining=%v", row.Exists, draining)
				}
				return err
			}))
		})
	}
}
