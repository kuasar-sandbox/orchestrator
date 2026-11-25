package metastore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/storage"
	records "github.com/kuasar-sandbox/orchestrator/internal/storage/record"
)

type authCandidateReads struct {
	storage.Engine
	scans, rows int
}

func (e *authCandidateReads) View(ctx context.Context, fn func(storage.ReadTx) error) error {
	return e.Engine.View(ctx, func(tr storage.ReadTx) error { return fn(authCandidateReadTx{ReadTx: tr, owner: e}) })
}

type authCandidateReadTx struct {
	storage.ReadTx
	owner *authCandidateReads
}

func (t authCandidateReadTx) Scan(r storage.Range, limit storage.Limit) (storage.Page, error) {
	page, err := t.ReadTx.Scan(r, limit)
	t.owner.scans++
	t.owner.rows += len(page.Rows)
	return page, err
}

func authCandidateFixture(i int) metadata.GroupAuth {
	secret := sha256.Sum256([]byte(fmt.Sprintf("auth-root-%d", i)))
	fp := sha256.Sum256(secret[:])
	return metadata.GroupAuth{Group: "g", AuthRef: hex.EncodeToString(fp[:]), APISecret: hex.EncodeToString(secret[:])}
}

func seedAuthCandidates(t testing.TB, s *Store, count int) []metadata.GroupAuth {
	t.Helper()
	roots := make([]metadata.GroupAuth, count)
	for i := range roots {
		roots[i] = authCandidateFixture(i)
	}
	// Write the pre-existing root layout directly, without a new index or
	// migration, in bounded transactions. Lifecycle reference tests are separate.
	for start := 0; start < count; start += 128 {
		_, err := s.mutate(t.Context(), func(m *mutation) (bool, error) {
			for _, root := range roots[start:min(start+128, count)] {
				if err := putJSON(m.tr, s.groupAuthKey(root.Group, root.AuthRef), root); err != nil {
					return false, err
				}
			}
			return true, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	slices.SortFunc(roots, func(a, b metadata.GroupAuth) int { return strings.Compare(a.AuthRef, b.AuthRef) })
	return roots
}

func TestGroupAuthCandidates(t *testing.T) {
	for name, factory := range backendCases {
		t.Run(name, func(t *testing.T) {
			s := newBackendStore(t, factory(t))
			roots := seedAuthCandidates(t, s.Store, 1024)
			reads := &authCandidateReads{Engine: s.backend}
			reader := New(reads, s.backend)
			for _, index := range []int{0, len(roots) / 2, len(roots) - 1} {
				root := roots[index]
				reads.scans, reads.rows = 0, 0
				page, err := reader.ListGroupAuthCandidates(t.Context(), "g", root.AuthRef[:24], "", 128)
				check(t, err)
				if len(page.Items) != 1 || page.Items[0].Value != root || page.Next != "" {
					t.Fatalf("candidate %d = %+v", index, page)
				}
				if reads.scans != 1 || reads.rows > 128 {
					t.Fatalf("candidate scan grew with directory: scans=%d rows=%d", reads.scans, reads.rows)
				}
			}
			reads.scans, reads.rows = 0, 0
			page, err := reader.ListGroupAuthCandidates(t.Context(), "g", strings.Repeat("0", 24), "", 128)
			check(t, err)
			if len(page.Items) != 0 || page.Next != "" || reads.scans != 1 || reads.rows > 128 {
				t.Fatalf("absent candidate escaped bounded page: page=%+v scans=%d rows=%d", page, reads.scans, reads.rows)
			}

			root := roots[0]
			scope := s.key("group-auth", "g")
			for name, cursor := range map[string]metadata.Cursor{
				"malformed":    "not-a-cursor",
				"other group":  encodeCursor(s.key("group-auth", "h"), s.groupAuthKey("h", root.AuthRef)),
				"other prefix": encodeCursor(scope, s.groupAuthKey("g", roots[1].AuthRef)),
				"child":        encodeCursor(scope, s.groupAuthKey("g", root.AuthRef).Child("extra")),
				"short ref":    encodeCursor(scope, scope.Child(root.AuthRef[:24])),
			} {
				t.Run(name, func(t *testing.T) {
					_, err := s.ListGroupAuthCandidates(t.Context(), "g", root.AuthRef[:24], cursor, 128)
					if !errors.Is(err, metadata.ErrInvalidRecord) {
						t.Fatalf("invalid candidate cursor accepted: %v", err)
					}
				})
			}
			for _, candidate := range []string{"", "a", strings.Repeat("A", 24), strings.Repeat("z", 24), strings.Repeat("0", 25)} {
				if _, err := s.ListGroupAuthCandidates(t.Context(), "g", candidate, "", 128); !errors.Is(err, metadata.ErrInvalidRecord) {
					t.Fatalf("candidate %q accepted: %v", candidate, err)
				}
			}
			for _, limit := range []int{-1, 0, 129} {
				if _, err := s.ListGroupAuthCandidates(t.Context(), "g", root.AuthRef[:24], "", limit); !errors.Is(err, metadata.ErrInvalidRecord) {
					t.Fatalf("limit %d accepted: %v", limit, err)
				}
			}
		})
	}
}

// Real SHA-256 prefix collisions are not practical fixtures. Exercise the range
// boundary separately with synthetic fixed-width fingerprints; the public
// repository's material/identity validation is covered with genuine hashes.
func TestGroupAuthCandidateRangeCollisions(t *testing.T) {
	for name, factory := range backendCases {
		t.Run(name, func(t *testing.T) {
			for _, count := range []int{0, 1, 128, 129, 256, 257} {
				t.Run(fmt.Sprint(count), func(t *testing.T) {
					s := newBackendStore(t, factory(t))
					scope := s.key("group-auth", "g")
					candidate := strings.Repeat("8", 24)
					_, err := s.mutate(t.Context(), func(m *mutation) (bool, error) {
						for i := range 256 {
							for _, prefix := range []string{strings.Repeat("7", 24), strings.Repeat("9", 24)} {
								if err := putJSON(m.tr, scope.Child(prefix+fmt.Sprintf("%040x", i)), struct{}{}); err != nil {
									return false, err
								}
							}
						}
						for i := range count {
							if err := putJSON(m.tr, scope.Child(candidate+fmt.Sprintf("%040x", i)), struct{}{}); err != nil {
								return false, err
							}
						}
						return true, nil
					})
					check(t, err)
					reads := &authCandidateReads{Engine: s.backend}
					reader := New(reads, s.backend)
					after := scope.Child(candidate + strings.Repeat("\x00", 40))
					got := 0
					for {
						var page records.Page
						check(t, reader.read(t.Context(), func(tr *records.Tx) error {
							var err error
							page, err = scanGroupAuthCandidates(tr, scope, after, candidate, 128)
							return err
						}))
						for _, item := range page.Items {
							want := candidate + fmt.Sprintf("%040x", got)
							ref, err := groupAuthKeyFingerprint(scope, item.Key)
							check(t, err)
							if ref != want {
								t.Fatalf("candidate %d = %q, want %q", got, ref, want)
							}
							got++
						}
						if !page.More {
							break
						}
						if len(page.Items) == 0 {
							t.Fatal("candidate scan made no progress")
						}
						after = page.Items[len(page.Items)-1].Key
					}
					if got != count || reads.scans != count/128+1 || reads.rows > count+128 {
						t.Fatalf("candidate range: got=%d want=%d scans=%d rows=%d", got, count, reads.scans, reads.rows)
					}
				})
			}
		})
	}
}

func TestGroupAuthCandidatesValidateFullIdentity(t *testing.T) {
	for name, factory := range backendCases {
		t.Run(name, func(t *testing.T) {
			for _, corrupt := range []string{"material", "key", "group"} {
				t.Run(corrupt, func(t *testing.T) {
					s := newBackendStore(t, factory(t))
					root := authCandidateFixture(0)
					stored := root
					switch corrupt {
					case "material":
						stored.APISecret = strings.Repeat("0", 64)
					case "key":
						stored = authCandidateFixture(1)
					case "group":
						stored.Group = "h"
					}
					_, err := s.mutate(t.Context(), func(m *mutation) (bool, error) {
						return true, putJSON(m.tr, s.groupAuthKey("g", root.AuthRef), stored)
					})
					check(t, err)
					if _, err = s.ListGroupAuthCandidates(t.Context(), "g", root.AuthRef[:24], "", 128); !errors.Is(err, metadata.ErrInvalidRecord) {
						t.Fatalf("invalid %s accepted: %v", corrupt, err)
					}
				})
			}
		})
	}
}
