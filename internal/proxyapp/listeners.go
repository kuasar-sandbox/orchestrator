package proxyapp

import (
	"fmt"
	"maps"
	"net"
	"os"

	"github.com/kuasar-sandbox/orchestrator/internal/listenresource"
)

// listenerSet borrows the App-owned input files. Each Open creates a distinct
// instance-owned listener copy. Worker files are further explicit duplicates;
// releasing any copy must not shutdown the shared listening socket.
type listenerSet struct {
	files map[string]*os.File
	used  map[string]bool
}

func newListenerSet(files map[string]*os.File) (*listenerSet, error) {
	frozen := maps.Clone(files)
	if err := listenresource.Validate(frozen); err != nil {
		return nil, err
	}
	return &listenerSet{files: frozen, used: make(map[string]bool)}, nil
}

func (s *listenerSet) open(expression string, bind func(string) (net.Listener, error)) (net.Listener, error) {
	name, err := listenresource.Parse(expression)
	if err != nil {
		return nil, err
	}
	if name == "" {
		return bind(expression)
	}
	file, found := s.files[name]
	if !found {
		return nil, fmt.Errorf("listener %q was not provided", name)
	}
	if s.used[name] {
		return nil, fmt.Errorf("listener %q is selected by multiple endpoints", name)
	}
	listener, err := listenresource.Listener(file)
	if err != nil {
		return nil, fmt.Errorf("listener %q: %w", name, err)
	}
	s.used[name] = true
	return listener, nil
}

func (s *listenerSet) complete() error {
	for name := range s.files {
		if !s.used[name] {
			return fmt.Errorf("unused listener resource %q", name)
		}
	}
	return nil
}
