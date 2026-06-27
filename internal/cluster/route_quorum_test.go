package cluster

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

func routeQuorumForTest(writer string, rs []*MemoryRouteReplica) *RouteQuorum {
	reps := make([]RouteReplica, len(rs))
	for i, r := range rs {
		reps[i] = r
	}
	return NewRouteQuorum(writer, reps...)
}

func TestRouteQuorumCASUsesUniqueBallots(t *testing.T) {
	ctx := context.Background()
	rs := []*MemoryRouteReplica{NewMemoryRouteReplica(), NewMemoryRouteReplica(), NewMemoryRouteReplica()}
	qa := routeQuorumForTest("a", rs)
	qb := routeQuorumForTest("b", rs)

	a, err := qa.CAS(ctx, "/g", "rk", 0, func(RouteRecord, bool) (RouteRecord, bool, error) {
		return RouteRecord{Group: "/g", RouteKey: "rk", SandboxID: "sb-a", State: RouteReserved}, true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := qb.CAS(ctx, "/g", "rk", a.Meta.Rev, func(cur RouteRecord, found bool) (RouteRecord, bool, error) {
		if !found || cur.SandboxID != "sb-a" {
			t.Fatalf("proposal saw current=%+v found=%v", cur, found)
		}
		cur.State = RouteReady
		return cur, true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if a.Meta.Ballot == b.Meta.Ballot || !a.Meta.Ballot.Less(b.Meta.Ballot) {
		t.Fatalf("ballots not uniquely ordered: a=%+v b=%+v", a.Meta.Ballot, b.Meta.Ballot)
	}
	if b.Meta.Rev != 2 || b.State != RouteReady {
		t.Fatalf("second write = %+v", b)
	}
}

func TestRouteQuorumCASConflict(t *testing.T) {
	ctx := context.Background()
	rs := []*MemoryRouteReplica{NewMemoryRouteReplica(), NewMemoryRouteReplica(), NewMemoryRouteReplica()}
	q := routeQuorumForTest("a", rs)
	rec, err := q.CAS(ctx, "/g", "rk", 0, func(RouteRecord, bool) (RouteRecord, bool, error) {
		return RouteRecord{Group: "/g", RouteKey: "rk", SandboxID: "sb", State: RouteReserved}, true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = q.CAS(ctx, "/g", "rk", rec.Meta.Rev-1, func(RouteRecord, bool) (RouteRecord, bool, error) {
		return RouteRecord{Group: "/g", RouteKey: "rk", State: RouteReady}, true, nil
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("CAS stale rev err=%v, want ErrConflict", err)
	}
}

type flakyRouteReplica struct {
	RouteReplica
	down bool
}

func (r *flakyRouteReplica) Read(ctx context.Context, key string) (RouteRecord, bool, error) {
	if r.down {
		return RouteRecord{}, false, context.Canceled
	}
	return r.RouteReplica.Read(ctx, key)
}

func (r *flakyRouteReplica) Prepare(ctx context.Context, key string, ballot Ballot) (RouteRecord, bool, bool, error) {
	if r.down {
		return RouteRecord{}, false, false, context.Canceled
	}
	return r.RouteReplica.Prepare(ctx, key, ballot)
}

func (r *flakyRouteReplica) Accept(ctx context.Context, key string, rec RouteRecord, ballot Ballot) (bool, error) {
	if r.down {
		return false, context.Canceled
	}
	return r.RouteReplica.Accept(ctx, key, rec, ballot)
}

func (r *flakyRouteReplica) Repair(ctx context.Context, key string, rec RouteRecord) error {
	if r.down {
		return context.Canceled
	}
	return r.RouteReplica.Repair(ctx, key, rec)
}

func (r *flakyRouteReplica) MaxBallot(ctx context.Context, key string) (Ballot, error) {
	if r.down {
		return Ballot{}, context.Canceled
	}
	return r.RouteReplica.MaxBallot(ctx, key)
}

func (r *flakyRouteReplica) Keys(ctx context.Context) []string {
	if r.down {
		return nil
	}
	return r.RouteReplica.Keys(ctx)
}

func TestRouteQuorumMemberFailurePolicy(t *testing.T) {
	ctx := context.Background()
	a := &flakyRouteReplica{RouteReplica: NewMemoryRouteReplica()}
	b := &flakyRouteReplica{RouteReplica: NewMemoryRouteReplica()}
	c := &flakyRouteReplica{RouteReplica: NewMemoryRouteReplica()}
	q := NewRouteQuorum("writer", a, b, c)

	a.down = true
	if _, err := q.CAS(ctx, "/g", "rk", 0, func(RouteRecord, bool) (RouteRecord, bool, error) {
		return RouteRecord{Group: "/g", RouteKey: "rk", SandboxID: "sb", State: RouteReady}, true, nil
	}); err != nil {
		t.Fatalf("single member down should keep quorum: %v", err)
	}
	b.down = true
	if _, err := q.CAS(ctx, "/g", "rk2", 0, func(RouteRecord, bool) (RouteRecord, bool, error) {
		return RouteRecord{Group: "/g", RouteKey: "rk2", SandboxID: "sb2", State: RouteReady}, true, nil
	}); !errors.Is(err, ErrQuorum) {
		t.Fatalf("two members down err=%v, want ErrQuorum", err)
	}
}

func TestRouteQuorumReadRepair(t *testing.T) {
	ctx := context.Background()
	rs := []*MemoryRouteReplica{NewMemoryRouteReplica(), NewMemoryRouteReplica(), NewMemoryRouteReplica()}
	partial := RouteRecord{
		Meta:  RecordMeta{Ballot: Ballot{Round: 7, Writer: "old"}, Rev: 3},
		Group: "/g", RouteKey: "rk", SandboxID: "sb", State: RouteReady,
	}
	if ok, err := rs[0].Accept(ctx, RouteKey("/g", "rk"), partial, partial.Meta.Ballot); err != nil || !ok {
		t.Fatalf("partial accept ok=%v err=%v", ok, err)
	}

	q := routeQuorumForTest("reader", rs)
	got, found, err := q.Get(ctx, "/g", "rk")
	if err != nil || !found {
		t.Fatalf("Get found=%v err=%v", found, err)
	}
	if got.SandboxID != "sb" || got.Meta.Rev != 3 || got.Meta.Ballot.Writer != "reader" {
		t.Fatalf("Get = %+v", got)
	}
	for i, r := range rs {
		rr, found, err := r.Read(ctx, RouteKey("/g", "rk"))
		if err != nil || !found || rr.SandboxID != "sb" || rr.Meta.Ballot != got.Meta.Ballot {
			t.Fatalf("replica %d after repair found=%v err=%v rec=%+v", i, found, err, rr)
		}
	}
}

func TestRouteQuorumDeleteTombstonePreventsResurrection(t *testing.T) {
	ctx := context.Background()
	rs := []*MemoryRouteReplica{NewMemoryRouteReplica(), NewMemoryRouteReplica(), NewMemoryRouteReplica()}
	key := RouteKey("/g", "rk")
	old := RouteRecord{
		Meta:  RecordMeta{Ballot: Ballot{Round: 1, Writer: "old"}, Rev: 1},
		Group: "/g", RouteKey: "rk", SandboxID: "sb-old", State: RouteReady,
	}
	for _, r := range rs {
		if ok, err := r.Accept(ctx, key, old, old.Meta.Ballot); err != nil || !ok {
			t.Fatalf("seed accept ok=%v err=%v", ok, err)
		}
	}
	delBallot := Ballot{Round: 2, Writer: "deleter"}
	for _, r := range rs[:2] {
		if ok, err := r.AcceptDelete(ctx, key, delBallot); err != nil || !ok {
			t.Fatalf("delete accept ok=%v err=%v", ok, err)
		}
	}

	q := routeQuorumForTest("reader", rs)
	got, found, err := q.Get(ctx, "/g", "rk")
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatalf("deleted route resurrected as %+v", got)
	}
	for i, r := range rs {
		rec, found, err := r.Read(ctx, key)
		if err != nil || !found || rec.State != RouteDead {
			t.Fatalf("replica %d after repair found=%v err=%v rec=%+v, want tombstone", i, found, err, rec)
		}
	}
}

func TestRouteQuorumCASDeleteWritesUniformTombstone(t *testing.T) {
	ctx := context.Background()
	rs := []*MemoryRouteReplica{NewMemoryRouteReplica(), NewMemoryRouteReplica(), NewMemoryRouteReplica()}
	q := routeQuorumForTest("writer", rs)
	rec, err := q.CAS(ctx, "/g", "rk", 0, func(RouteRecord, bool) (RouteRecord, bool, error) {
		return RouteRecord{Group: "/g", RouteKey: "rk", SandboxID: "sb", State: RouteReady}, true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.CAS(ctx, "/g", "rk", rec.Meta.Rev, func(RouteRecord, bool) (RouteRecord, bool, error) {
		return RouteRecord{}, false, nil
	}); err != nil {
		t.Fatal(err)
	}
	var want RecordMeta
	for i, r := range rs {
		got, found, err := r.Read(ctx, RouteKey("/g", "rk"))
		if err != nil || !found || got.State != RouteDead {
			t.Fatalf("replica %d found=%v err=%v got=%+v, want tombstone", i, found, err, got)
		}
		if i == 0 {
			want = got.Meta
			continue
		}
		if got.Meta != want {
			t.Fatalf("replica %d tombstone meta=%+v, want %+v", i, got.Meta, want)
		}
	}
}

func TestRouteQuorumRandomizedCASConflicts(t *testing.T) {
	ctx := context.Background()
	rs := []*MemoryRouteReplica{NewMemoryRouteReplica(), NewMemoryRouteReplica(), NewMemoryRouteReplica()}
	writers := []*RouteQuorum{
		routeQuorumForTest("a", rs),
		routeQuorumForTest("b", rs),
		routeQuorumForTest("c", rs),
	}
	rng := rand.New(rand.NewSource(42))
	var rev uint64
	for i := 0; i < 200; i++ {
		q := writers[rng.Intn(len(writers))]
		expect := rev
		wantConflict := false
		if rev > 0 && rng.Intn(4) == 0 {
			expect = uint64(rng.Intn(int(rev)))
			wantConflict = true
		}
		got, err := q.CAS(ctx, "/g", "rk", expect, func(cur RouteRecord, found bool) (RouteRecord, bool, error) {
			next := RouteRecord{Group: "/g", RouteKey: "rk", SandboxID: "sb-rand", State: RouteReady}
			if found {
				next = cur
			}
			next.TemplateID = fmt.Sprintf("step-%03d", i)
			return next, true, nil
		})
		if wantConflict {
			if !errors.Is(err, ErrConflict) {
				t.Fatalf("step %d stale expect=%d rev=%d err=%v, want ErrConflict", i, expect, rev, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("step %d CAS err=%v", i, err)
		}
		rev++
		if got.Meta.Rev != rev {
			t.Fatalf("step %d got rev=%d, want %d", i, got.Meta.Rev, rev)
		}
	}
	got, found, err := writers[0].Get(ctx, "/g", "rk")
	if err != nil || !found {
		t.Fatalf("final get found=%v err=%v", found, err)
	}
	if got.Meta.Rev != rev {
		t.Fatalf("final rev=%d, want %d", got.Meta.Rev, rev)
	}
}

func TestRouteQuorumConcurrentSameWriterNextBallotsAreUnique(t *testing.T) {
	ctx := context.Background()
	rs := []*MemoryRouteReplica{NewMemoryRouteReplica(), NewMemoryRouteReplica(), NewMemoryRouteReplica()}
	q := routeQuorumForTest("same-writer", rs)
	if _, err := q.CAS(ctx, "/g", "rk", 0, func(RouteRecord, bool) (RouteRecord, bool, error) {
		return RouteRecord{Group: "/g", RouteKey: "rk", SandboxID: "seed", State: RouteReady}, true, nil
	}); err != nil {
		t.Fatal(err)
	}

	const workers = 128
	ballots := make(chan Ballot, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ballots <- q.nextBallot(ctx, RouteKey("/g", "rk"))
		}()
	}
	wg.Wait()
	close(ballots)
	seen := map[Ballot]bool{}
	for b := range ballots {
		if seen[b] {
			t.Fatalf("duplicate ballot %+v", b)
		}
		seen[b] = true
	}
	if len(seen) != workers {
		t.Fatalf("committed ballots=%d, want %d", len(seen), workers)
	}
}
