package orch

import (
	"context"
	"errors"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/vswitch"
)

type generationTestVS struct {
	bits      uint8
	bitsErr   error
	bitsCalls int
	reqs      []vswitch.AttachReq
}

func (v *generationTestVS) GenerationBits(context.Context) (uint8, error) {
	v.bitsCalls++
	err := v.bitsErr
	v.bitsErr = nil
	return v.bits, err
}
func (v *generationTestVS) Attach(_ context.Context, req vswitch.AttachReq) (*vswitch.Port, error) {
	v.reqs = append(v.reqs, req)
	return &vswitch.Port{Port: "1", Generation: req.Generation}, nil
}
func (*generationTestVS) Detach(context.Context, string) error { return nil }
func (*generationTestVS) TapFD(string) vswitch.TapFD           { return vswitch.TapFD{} }

type legacyGenerationTestVS struct{ reqs []vswitch.AttachReq }

func (v *legacyGenerationTestVS) Attach(_ context.Context, req vswitch.AttachReq) (*vswitch.Port, error) {
	v.reqs = append(v.reqs, req)
	return &vswitch.Port{Port: "1"}, nil
}
func (*legacyGenerationTestVS) Detach(context.Context, string) error { return nil }
func (*legacyGenerationTestVS) TapFD(string) vswitch.TapFD           { return vswitch.TapFD{} }

func TestNetworkAttachmentGenerationUsesSwitchBitsAndGlobalSequence(t *testing.T) {
	vs := &generationTestVS{bits: 4}
	o := &Orchestrator{vs: vs, detachedPortsPending: map[string]struct{}{}}
	for i := 0; i < 18; i++ {
		port, err := o.attachNetwork(context.Background(), sandboxcfg.NetworkSpec{InnerIP: "169.254.1.1/32"})
		if err != nil {
			t.Fatal(err)
		}
		if got, want := port.Generation, uint32(i%16); got != want {
			t.Fatalf("attach %d generation=%d want=%d", i, got, want)
		}
	}
	if vs.bitsCalls != 1 {
		t.Fatalf("generation bits queried %d times, want once", vs.bitsCalls)
	}
}

func TestNetworkAttachmentGenerationRetriesStatusFailure(t *testing.T) {
	vs := &generationTestVS{bits: 4, bitsErr: errors.New("temporary status failure")}
	o := &Orchestrator{vs: vs, detachedPortsPending: map[string]struct{}{}}
	if _, err := o.attachNetwork(context.Background(), sandboxcfg.NetworkSpec{InnerIP: "169.254.1.1/32"}); err == nil {
		t.Fatal("status failure did not block Attach")
	}
	port, err := o.attachNetwork(context.Background(), sandboxcfg.NetworkSpec{InnerIP: "169.254.1.1/32"})
	if err != nil {
		t.Fatal(err)
	}
	if port.Generation != 0 || vs.bitsCalls != 2 || len(vs.reqs) != 1 {
		t.Fatalf("retry generation=%d bitsCalls=%d attaches=%d", port.Generation, vs.bitsCalls, len(vs.reqs))
	}
}

func TestNetworkAttachmentGenerationLegacyProviderIsZero(t *testing.T) {
	vs := &legacyGenerationTestVS{}
	o := &Orchestrator{vs: vs, detachedPortsPending: map[string]struct{}{}}
	for i := 0; i < 2; i++ {
		if _, err := o.attachNetwork(context.Background(), sandboxcfg.NetworkSpec{InnerIP: "169.254.1.1/32"}); err != nil {
			t.Fatal(err)
		}
	}
	for i, req := range vs.reqs {
		if req.Generation != 0 {
			t.Fatalf("legacy attach %d generation=%d", i, req.Generation)
		}
	}
}
