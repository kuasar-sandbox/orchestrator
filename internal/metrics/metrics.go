// Package metrics is a tiny dependency-free counter registry with a Prometheus
// text exposition handler. It is intentionally minimal (no histograms, no client
// library) so both node-ctl conductor serve and the proxy worker can surface
// data-plane / route-sync counters without pulling in a metrics dependency.
//
// Counter names may carry Prometheus labels inline, e.g. Inc(`data_requests_total{result="ok"}`).
// All methods are nil-safe so callers can hold a *M that may be nil (metrics off).
package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// M is a set of monotonically increasing counters.
type M struct {
	mu sync.Mutex
	c  map[string]*atomic.Int64
	g  map[string]*atomic.Int64
}

// New returns an empty registry.
func New() *M { return &M{c: map[string]*atomic.Int64{}, g: map[string]*atomic.Int64{}} }

// Inc adds 1 to the named counter (no-op on a nil registry).
func (m *M) Inc(name string) { m.Add(name, 1) }

// Add adds n to the named counter (no-op on a nil registry).
func (m *M) Add(name string, n int64) {
	if m == nil {
		return
	}
	m.counter(name).Add(n)
}

// Set publishes an absolute integer gauge (no-op on a nil registry).
func (m *M) Set(name string, value int64) {
	if m == nil {
		return
	}
	m.gauge(name).Store(value)
}

func (m *M) counter(name string) *atomic.Int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.c[name]
	if !ok {
		c = &atomic.Int64{}
		m.c[name] = c
	}
	return c
}

func (m *M) gauge(name string) *atomic.Int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.g[name]
	if !ok {
		g = &atomic.Int64{}
		m.g[name] = g
	}
	return g
}

// Handler renders the registry as Prometheus text exposition.
func (m *M) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		if m == nil {
			return
		}
		m.mu.Lock()
		names := make([]string, 0, len(m.c)+len(m.g))
		vals := make(map[string]int64, len(m.c)+len(m.g))
		kinds := make(map[string]string, len(m.c)+len(m.g))
		for n, c := range m.c {
			names = append(names, n)
			vals[n] = c.Load()
			kinds[n] = "counter"
		}
		for n, g := range m.g {
			names = append(names, n)
			vals[n] = g.Load()
			kinds[n] = "gauge"
		}
		m.mu.Unlock()
		sort.Strings(names)
		seen := map[string]bool{}
		for _, n := range names {
			base := n
			if i := strings.IndexByte(n, '{'); i >= 0 {
				base = n[:i]
			}
			if !seen[base] {
				fmt.Fprintf(w, "# TYPE %s %s\n", base, kinds[n])
				seen[base] = true
			}
			fmt.Fprintf(w, "%s %d\n", n, vals[n])
		}
	}
}
