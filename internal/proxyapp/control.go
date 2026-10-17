package proxyapp

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"
)

// controlIdentity binds a private connection to one instance and worker epoch.
// An operation ID may be empty only for the initial, ordinary instance.
type controlIdentity struct {
	BootID      string    `json:"boot_id"`
	RestartID   RestartID `json:"restart_id"`
	WorkerID    string    `json:"worker_id,omitempty"`
	WorkerEpoch uint64    `json:"worker_epoch,omitempty"`
}

type controlMessage struct {
	controlIdentity
	Kind        string    `json:"kind"`
	Deadline    time.Time `json:"deadline,omitempty"`
	CanShutdown bool      `json:"can_shutdown,omitempty"`
	Error       string    `json:"error,omitempty"`
	Forced      bool      `json:"forced,omitempty"`
}

type controlPeer struct {
	net.Conn
	identity controlIdentity
	reader   *bufio.Reader
	writeMu  sync.Mutex
}

func newControlPeer(conn net.Conn, identity controlIdentity) *controlPeer {
	return &controlPeer{Conn: conn, identity: identity, reader: bufio.NewReaderSize(conn, 64<<10)}
}

func (p *controlPeer) send(message controlMessage) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	message.controlIdentity = p.identity
	deadline := time.Now().Add(2 * time.Second)
	if !message.Deadline.IsZero() && message.Deadline.Before(deadline) {
		deadline = message.Deadline
	}
	if err := p.SetWriteDeadline(deadline); err != nil {
		return err
	}
	defer p.SetWriteDeadline(time.Time{})
	return json.NewEncoder(p.Conn).Encode(message)
}

func (p *controlPeer) receive() (controlMessage, error) {
	raw, err := p.reader.ReadSlice('\n')
	if err != nil {
		return controlMessage{}, err
	}
	var message controlMessage
	if err := json.Unmarshal(raw, &message); err != nil {
		return message, err
	}
	if message.controlIdentity != p.identity {
		return message, fmt.Errorf("proxy control: stale or foreign instance/worker identity")
	}
	return message, nil
}
