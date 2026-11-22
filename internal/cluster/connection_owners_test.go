package cluster

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
)

func TestConnectionOwnersPreserveBusinessCandidates(t *testing.T) {
	views := []MemberView{{Version: 2, Members: []string{"b", "c", "d"}}, {Version: 1, Members: []string{"a", "b", "c"}}}
	o, err := NewConnectionOwners("b", views, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"node-a", "node-b", "node-c"} {
		var want []string
		seen := map[string]bool{}
		for _, v := range []MemberView{views[1], views[0]} {
			ids, err := v.Owners(key, 2)
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range ids {
				if !seen[id] {
					seen[id] = true
					want = append(want, id)
				}
			}
		}
		got, err := o.Candidates(t.Context(), key)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("key=%q got=%v want=%v err=%v", key, got, want, err)
		}
	}
	before, _ := o.Candidates(t.Context(), "node-a")
	if err := o.Update([]MemberView{{Version: 1, Members: []string{"b", "b"}}}, 2); err == nil {
		t.Fatal("duplicate members accepted")
	}
	after, _ := o.Candidates(t.Context(), "node-a")
	if !reflect.DeepEqual(before, after) {
		t.Fatal("failed update changed routing")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := o.Candidates(ctx, "node-a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
}

func TestConnectionOwnersConcurrentViewReplacement(t *testing.T) {
	first := []MemberView{{Version: 1, Members: []string{"a"}}}
	second := []MemberView{{Version: 2, Members: []string{"b"}}}
	o, err := NewConnectionOwners("a", first, 2)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				ids, err := o.Candidates(t.Context(), "node")
				if err != nil || len(ids) != 1 || (ids[0] != "a" && ids[0] != "b") {
					t.Errorf("partial routing view: %v %v", ids, err)
					return
				}
			}
		})
	}
	for range 10 {
		if err := o.Update(second, 2); err != nil {
			t.Fatal(err)
		}
		if err := o.Update(first, 2); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
}
