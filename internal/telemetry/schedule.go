package telemetry

import (
	"container/heap"
	"hash/fnv"
	"sync"
	"time"
)

// Each receiver instance owns its cadence and in-flight jobs. Entries are
// pointers into the one RouteEntry view, never another lifecycle authority.
type scrapeSchedule struct {
	mu       sync.Mutex
	view     *View
	interval time.Duration
	entries  map[string]*scheduledTarget
	due      targetHeap
	changed  chan struct{}
}

type scheduledTarget struct {
	entry *target
	next  time.Time
	index int
	busy  bool
}

func newScrapeSchedule(view *View, interval time.Duration) *scrapeSchedule {
	s := &scrapeSchedule{view: view, interval: interval, entries: make(map[string]*scheduledTarget), changed: make(chan struct{}, 1)}
	view.mu.Lock()
	view.schedules[s] = struct{}{}
	if view.synced {
		for _, entry := range view.byID {
			s.update(entry.route.SandboxID, entry)
		}
	}
	view.mu.Unlock()
	return s
}

func (s *scrapeSchedule) detach() {
	s.view.mu.Lock()
	delete(s.view.schedules, s)
	s.view.mu.Unlock()
}

// View calls update while holding its authority lock. A schedule retains only
// derived jobs pointing into that view; it never decides lifecycle or binding.
func (s *scrapeSchedule) update(id string, entry *target) {
	s.mu.Lock()
	if old := s.entries[id]; old != nil {
		heap.Remove(&s.due, old.index)
		delete(s.entries, id)
	}
	if entry != nil && entry.ctx.Err() == nil && scrapeEligible(entry.route) {
		hash := fnv.New64a()
		_, _ = hash.Write([]byte(id))
		job := &scheduledTarget{entry: entry, next: time.Now().Add(time.Duration(hash.Sum64() % uint64(s.interval)))}
		s.entries[id] = job
		heap.Push(&s.due, job)
	}
	s.mu.Unlock()
	s.signal()
}

func (s *scrapeSchedule) clear() {
	s.mu.Lock()
	clear(s.entries)
	s.due = nil
	s.mu.Unlock()
	s.signal()
}

func (s *scrapeSchedule) signal() {
	select {
	case s.changed <- struct{}{}:
	default:
	}
}

func (s *scrapeSchedule) takeDue(now time.Time, limit int) []*target {
	s.mu.Lock()
	defer s.mu.Unlock()
	var jobs []*target
	for len(jobs) < limit && len(s.due) != 0 && !s.due[0].next.After(now) {
		job := heap.Pop(&s.due).(*scheduledTarget)
		if !job.busy {
			job.busy = true
			jobs = append(jobs, job.entry)
		}
		job.next = now.Add(s.interval)
		heap.Push(&s.due, job)
	}
	return jobs
}

func (s *scrapeSchedule) finished(entry *target) {
	s.mu.Lock()
	if job := s.entries[entry.route.SandboxID]; job != nil && job.entry == entry {
		job.busy = false
	}
	s.mu.Unlock()
	s.signal()
}

func (s *scrapeSchedule) nextDelay(now time.Time) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.due) == 0 {
		return time.Hour
	}
	return max(time.Millisecond, s.due[0].next.Sub(now))
}

type targetHeap []*scheduledTarget

func (h targetHeap) Len() int           { return len(h) }
func (h targetHeap) Less(i, j int) bool { return h[i].next.Before(h[j].next) }
func (h targetHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *targetHeap) Push(value any) {
	job := value.(*scheduledTarget)
	job.index = len(*h)
	*h = append(*h, job)
}
func (h *targetHeap) Pop() any {
	old := *h
	job := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	job.index = -1
	return job
}
