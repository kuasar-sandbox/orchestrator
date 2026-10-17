package proxyapp

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// RunEntry is native entry-point signal assembly. Service actions themselves
// have no signal dependency. RunContext callers leave Signals false and may
// invoke the same action from any application-specific trigger.
func RunEntry(ctx context.Context, service *Service, input ServiceInput, continuation *Continuation, prepare PrepareService, runner MasterRunner) error {
	done := make(chan struct{})
	defer close(done)
	var once sync.Once
	var signals chan os.Signal
	arm := func(enabled bool) {
		once.Do(func() {
			if !input.Signals || !enabled || (continuation != nil && continuation.envelope.Mode == "candidate") {
				return
			}
			signals = make(chan os.Signal, 1)
			signal.Notify(signals, syscall.SIGUSR2)
			go func() {
				for {
					select {
					case <-signals:
						_, _ = service.InplaceRestart(context.Background())
					case <-done:
						return
					}
				}
			}()
		})
	}
	defer func() {
		if signals != nil {
			signal.Stop(signals)
		}
	}()
	if continuation != nil {
		var config struct {
			InplaceRestart bool `json:"inplace_restart"`
		}
		if err := json.Unmarshal(continuation.envelope.Baseline, &config); err != nil {
			return err
		}
		arm(config.InplaceRestart)
	}
	wrapped := func(ctx context.Context, input ServiceInput) (*Startup, error) {
		startup, err := prepare(ctx, input)
		if err == nil && startup != nil && startup.Effective != nil {
			arm(startup.Effective.config.InplaceRestart)
		}
		return startup, err
	}
	return service.Run(ctx, input, continuation, wrapped, runner)
}
