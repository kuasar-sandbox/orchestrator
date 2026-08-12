package proxyshm

import (
	"os"
	"sync"
	"testing"
)

func TestBroadcasterNotifyAndRemoveAreSynchronized(t *testing.T) {
	for range 100 {
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		b := NewBroadcaster()
		remove := b.Add(w)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			remove()
		}()
		go func() {
			defer wg.Done()
			<-start
			for range 10 {
				b.Notify()
			}
		}()
		close(start)
		wg.Wait()
		_ = r.Close()
	}
}
