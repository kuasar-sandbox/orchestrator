package main

import (
	"net"
	"os/exec"

	"github.com/kuasar-sandbox/orchestrator/internal/appnet"
	"github.com/kuasar-sandbox/orchestrator/internal/netns"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
)

func openProxyNetNS(name string) (*netns.NetNS, error) { return appnet.OpenProxyNetNS(name) }

func listenTCPInNetNS(ns *netns.NetNS, addr string) (net.Listener, error) {
	return appnet.ListenTCPInNetNS(ns, addr)
}

func startCommandInNetNS(ns *netns.NetNS, cmd *exec.Cmd) error {
	return appnet.StartCommandInNetNS(ns, cmd)
}

func routeDialerInNetNS(ns *netns.NetNS) proxy.RouteDialer {
	return appnet.RouteDialerInNetNS(ns)
}
