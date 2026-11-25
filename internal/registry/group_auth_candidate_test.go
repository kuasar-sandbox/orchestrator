package registry

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
)

type candidateAuthPages struct {
	metadata.GroupSupportRepository
	t         *testing.T
	candidate string
	items     []metadata.Versioned[metadata.GroupAuth]
	calls     int
}

func (p *candidateAuthPages) ListGroupAuthCandidates(_ context.Context, group, candidate string, cursor metadata.Cursor, limit int) (metadata.Page[metadata.Versioned[metadata.GroupAuth]], error) {
	p.t.Helper()
	if group != "g" || candidate != p.candidate || limit != 128 || string(cursor) != map[bool]string{true: "", false: fmt.Sprint(p.calls)}[p.calls == 0] {
		p.t.Fatalf("unexpected candidate lookup: group=%q candidate=%q cursor=%q limit=%d", group, candidate, cursor, limit)
	}
	start := p.calls * limit
	p.calls++
	end := min(start+limit, len(p.items))
	page := metadata.Page[metadata.Versioned[metadata.GroupAuth]]{Items: p.items[start:end]}
	if end < len(p.items) {
		page.Next = metadata.Cursor(fmt.Sprint(p.calls))
	}
	return page, nil
}

func TestVerifyGroupAPIKeyChecksEveryCandidateMAC(t *testing.T) {
	raw, _ := hex.DecodeString(testAPISecret)
	key, err := apikey.Mint(raw)
	if err != nil {
		t.Fatal(err)
	}
	prefix := testAPIFingerprint[:24]
	for _, mode := range []string{"no matching secret", "valid MAC", "invalid MAC"} {
		t.Run(mode, func(t *testing.T) {
			pages := &candidateAuthPages{t: t, candidate: prefix}
			// Simulate colliding directory candidates. The repository separately
			// validates full fingerprints; the Registry must still check the MAC
			// and continue beyond the first candidate page.
			for i := range 129 {
				pages.items = append(pages.items, metadata.Versioned[metadata.GroupAuth]{Exists: true, Value: metadata.GroupAuth{
					Group: "g", AuthRef: prefix + fmt.Sprintf("%040x", i), APISecret: strings.Repeat("2", 64),
				}})
			}
			if mode != "no matching secret" {
				pages.items[128].Value.AuthRef = testAPIFingerprint
				pages.items[128].Value.APISecret = testAPISecret
			}
			token := key
			if mode == "invalid MAC" {
				payload, err := hex.DecodeString(strings.TrimPrefix(key, "e2b_"))
				if err != nil {
					t.Fatal(err)
				}
				payload[len(payload)-1] ^= 1
				token = "e2b_" + hex.EncodeToString(payload)
			}
			r := &Registry{stores: &Stores{groups: pages}}
			ok, err := r.VerifyGroupAPIKey(t.Context(), "g", token)
			if err != nil || ok != (mode == "valid MAC") || pages.calls != 2 {
				t.Fatalf("verification: ok=%v err=%v calls=%d", ok, err, pages.calls)
			}
		})
	}
}
