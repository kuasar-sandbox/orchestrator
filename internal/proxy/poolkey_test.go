package proxy

import "testing"

// TestPoolKeyConfinesReuse guards the data-plane connection-pool fix: the
// ReverseProxy Transport keys idle keep-alive connections by req.URL.Host =
// poolKey(route), so a connection is reused ONLY for the same backend — never
// across sandboxes, ports, or a TCP port-forward vs an envd-control UDS. The
// last collision is the bug this fixes: a port-forward request reused a pooled
// envd-control connection and got envd's "404 page not found".
func TestPoolKeyConfinesReuse(t *testing.T) {
	tcpA := Route{Kind: KindTCP, Addr: "100.100.96.0:8000"}
	tcpAagain := Route{Kind: KindTCP, Addr: "100.100.96.0:8000"}
	tcpPort := Route{Kind: KindTCP, Addr: "100.100.96.0:8001"}
	tcpIP := Route{Kind: KindTCP, Addr: "100.100.96.1:8000"}
	udsA := Route{Kind: KindUDS, UDS: "/run/sandbox/aaa/envd.sock"}
	udsB := Route{Kind: KindUDS, UDS: "/run/sandbox/bbb/envd.sock"}
	udsCi := Route{Kind: KindUDS, UDS: "/run/sandbox/aaa/ci.sock"}

	// Identical backend → same key (keep-alive reuse is correct and wanted).
	if poolKey(tcpA) != poolKey(tcpAagain) {
		t.Errorf("same backend must share a pool key: %q vs %q", poolKey(tcpA), poolKey(tcpAagain))
	}

	// Distinct backends → distinct keys (no cross-route reuse).
	diff := [][2]Route{
		{tcpA, tcpPort}, // same ip, different port
		{tcpA, tcpIP},   // different floating ip
		{udsA, udsB},    // different sandbox's control socket
		{udsA, udsCi},   // envd vs ci on the same sandbox
		{tcpA, udsA},    // the bug: a TCP port-forward must not share a key with an envd UDS
	}
	for _, c := range diff {
		ka, kb := poolKey(c[0]), poolKey(c[1])
		if ka == kb {
			t.Errorf("distinct backends must NOT share a pool key: %+v and %+v both → %q", c[0], c[1], ka)
		}
		if ka == "" || kb == "" {
			t.Errorf("empty pool key for %+v / %+v", c[0], c[1])
		}
	}
}
