package orch

import "sync"

// flightGroup collapses concurrent calls keyed by the same string into one
// execution; the others wait and observe its result. It is the resume dedupe so a
// burst of data-plane requests (or wakes) for one paused sandbox triggers a single
// launch. (A local stand-in for golang.org/x/sync/singleflight — no new dep.)
type flightGroup struct {
	mu sync.Mutex
	m  map[string]*flight
}

type flight struct {
	done chan struct{}
	err  error
}

// Do runs fn unless another Do with the same key is in flight, in which case it
// waits for that one and returns its error.
func (g *flightGroup) Do(key string, fn func() error) error {
	g.mu.Lock()
	if g.m == nil {
		g.m = map[string]*flight{}
	}
	if f, ok := g.m[key]; ok {
		g.mu.Unlock()
		<-f.done
		return f.err
	}
	f := &flight{done: make(chan struct{})}
	g.m[key] = f
	g.mu.Unlock()

	f.err = fn()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
	close(f.done)
	return f.err
}
