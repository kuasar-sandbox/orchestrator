package proxystats

import (
	"net"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyadmission"
)

func TestPauseActivityIncludesUnsentUnlimitedParkingAndConnected(t *testing.T) {
	stats := NewWorkerStats()
	parking, err := stats.TryBeginParking("sandbox", proxy.ConnectServiceForward, proxyadmission.Binding{})
	if err != nil {
		t.Fatal(err)
	}
	defer parking.Close()
	flow := stats.BeginParking("sandbox", proxy.ConnectServiceExec)
	a, b := net.Pipe()
	defer b.Close()
	conn := flow.AttachBackend(a)
	defer conn.Close()
	sample, err := stats.PauseActivity("sandbox", func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	if sample[string(proxy.ConnectServiceForward)].Parking != 1 || sample[string(proxy.ConnectServiceExec)].Connected != 1 {
		t.Fatalf("synchronous sample missed unsent traffic: %+v", sample)
	}
	sample, err = stats.PauseActivity("sandbox", func() bool { return false })
	if err == nil || sample[string(proxy.ConnectServiceExec)].Connected != 1 {
		t.Fatalf("ungated ACK=%+v, %v", sample, err)
	}
}
