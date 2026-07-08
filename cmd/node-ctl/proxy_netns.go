package main

import (
	"context"
	"fmt"
	"net"
	"os/exec"

	"github.com/kuasar-sandbox/orchestrator/internal/netns"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
)

func openProxyNetNS(name string) (*netns.NetNS, error) {
	if name == "" {
		return nil, nil
	}
	ns, err := netns.Open(name)
	if err != nil {
		return nil, fmt.Errorf("proxy_netns %s: %w", name, err)
	}
	return ns, nil
}

func listenTCPInNetNS(ns *netns.NetNS, addr string) (net.Listener, error) {
	if ns == nil {
		return net.Listen("tcp", addr)
	}
	var ln net.Listener
	err := ns.Do(func() error {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			return err
		}
		ln = l
		return nil
	})
	return ln, err
}

func startCommandInNetNS(ns *netns.NetNS, cmd *exec.Cmd) error {
	if ns == nil {
		return cmd.Start()
	}
	return ns.Do(cmd.Start)
}

func routeDialerInNetNS(ns *netns.NetNS) proxy.RouteDialer {
	if ns == nil {
		return nil
	}
	return func(ctx context.Context, r proxy.Route) (net.Conn, error) {
		d := net.Dialer{}
		switch r.Kind {
		case proxy.KindUDS:
			return d.DialContext(ctx, "unix", r.UDS)
		case proxy.KindTCP:
			var conn net.Conn
			err := ns.Do(func() error {
				c, err := d.DialContext(ctx, "tcp", r.Addr)
				if err != nil {
					return err
				}
				conn = c
				return nil
			})
			return conn, err
		default:
			return nil, fmt.Errorf("proxy: no dialable route in context")
		}
	}
}
