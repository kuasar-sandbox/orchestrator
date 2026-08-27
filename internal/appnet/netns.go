package appnet

import (
	"context"
	"fmt"
	"net"
	"os/exec"

	"github.com/kuasar-sandbox/orchestrator/internal/netns"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
)

func OpenProxyNetNS(name string) (*netns.NetNS, error) {
	if name == "" {
		return nil, nil
	}
	ns, err := netns.Open(name)
	if err != nil {
		return nil, fmt.Errorf("proxy_netns %s: %w", name, err)
	}
	return ns, nil
}

func ListenTCPInNetNS(ns *netns.NetNS, addr string) (net.Listener, error) {
	if ns == nil {
		return net.Listen("tcp", addr)
	}
	var ln net.Listener
	err := ns.Do(func() error {
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			return err
		}
		ln = listener
		return nil
	})
	return ln, err
}

func StartCommandInNetNS(ns *netns.NetNS, cmd *exec.Cmd) error {
	if ns == nil {
		return cmd.Start()
	}
	return ns.Do(cmd.Start)
}

func RouteDialerInNetNS(ns *netns.NetNS) proxy.RouteDialer {
	if ns == nil {
		return nil
	}
	return func(ctx context.Context, route proxy.Route) (net.Conn, error) {
		dialer := net.Dialer{}
		switch route.Kind {
		case proxy.KindUDS:
			return dialer.DialContext(ctx, "unix", route.UDS)
		case proxy.KindTCP:
			var connection net.Conn
			err := ns.Do(func() error {
				conn, err := dialer.DialContext(ctx, "tcp", route.Addr)
				if err != nil {
					return err
				}
				connection = conn
				return nil
			})
			return connection, err
		default:
			return nil, fmt.Errorf("proxy: no dialable route in context")
		}
	}
}
