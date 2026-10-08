package sandboxsdk

import (
	"io"
	"log"
	"sync"

	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
)

// Readiness owns the connection until ready or failure; consumer disconnects
// affect delivery only, never the sandbox lifetime.
type Readiness struct {
	mu              sync.Mutex
	w               io.WriteCloser
	control, closed bool
}

func NewReadiness(w io.WriteCloser) *Readiness { return &Readiness{w: w} }
func (r *Readiness) Notify(e sandbox.ReadinessEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	switch e {
	case sandbox.ReadinessRuntimeReady:
		return
	case sandbox.ReadinessControlReady:
		if r.control {
			return
		}
		r.control = true
	case sandbox.ReadinessReady:
		if !r.control {
			log.Printf("[sandbox-ctl] readiness invariant violation: ready before control_ready")
			r.close()
			return
		}
	default:
		log.Printf("[sandbox-ctl] readiness invariant violation: unknown event %q", e)
		r.close()
		return
	}
	if r.w != nil {
		s := string(e) + "\n"
		for len(s) > 0 {
			n, err := io.WriteString(r.w, s)
			if n < 0 || n > len(s) {
				n = 0
				err = io.ErrShortWrite
			}
			if n == 0 && err == nil {
				err = io.ErrShortWrite
			}
			if err != nil {
				log.Printf("[sandbox-ctl] readiness fd write %s: %v", e, err)
				_ = r.close()
				return
			}
			s = s[n:]
		}
	}
	if e == sandbox.ReadinessReady {
		r.close()
	}
}
func (r *Readiness) close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.w != nil {
		return r.w.Close()
	}
	return nil
}
func (r *Readiness) Close() error { r.mu.Lock(); defer r.mu.Unlock(); return r.close() }
