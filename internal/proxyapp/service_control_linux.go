package proxyapp

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/listenresource"
	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
	"golang.org/x/sys/unix"
)

const maxServiceMessage = 1 << 20

type serviceMessage struct {
	Version     int             `json:"version"`
	BootID      string          `json:"boot_id"`
	Operation   RestartID       `json:"operation"`
	Kind        string          `json:"kind"`
	Status      *InstanceStatus `json:"status,omitempty"`
	Service     *Status         `json:"service,omitempty"`
	CanShutdown bool            `json:"can_shutdown,omitempty"`
	Deadline    time.Time       `json:"deadline,omitempty"`
	Forced      bool            `json:"forced,omitempty"`
	Error       string          `json:"error,omitempty"`
	Request     uint64          `json:"request,omitempty"`
	file        *os.File
}

// Packet framing leaves no userspace read-ahead to lose across exec. The
// detach acknowledgement is the final packet before B pauses notifications.
type serviceWire struct {
	conn      *net.UnixConn
	mu        sync.Mutex
	boot      string
	operation RestartID
}

func newServicePair() (*os.File, *os.File, error) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK, 0)
	if err != nil {
		return nil, nil, err
	}
	return os.NewFile(uintptr(fds[0]), "proxy-service-parent"), os.NewFile(uintptr(fds[1]), "proxy-service-child"), nil
}

func serviceConnection(file *os.File, boot string, operation RestartID) (*serviceWire, error) {
	copy, err := listenresource.Dup(file, "proxy-service-control")
	if err != nil {
		return nil, err
	}
	defer copy.Close()
	conn, err := net.FileConn(copy)
	if err != nil {
		return nil, err
	}
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		conn.Close()
		return nil, fmt.Errorf("proxy service control is not a local connection")
	}
	raw, err := unixConn.SyscallConn()
	if err != nil {
		conn.Close()
		return nil, err
	}
	valid := false
	err = raw.Control(func(fd uintptr) {
		typ, e := unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_TYPE)
		valid = e == nil && typ == unix.SOCK_SEQPACKET
	})
	if err != nil || !valid {
		conn.Close()
		return nil, fmt.Errorf("proxy service control has invalid framing")
	}
	return &serviceWire{conn: unixConn, boot: boot, operation: operation}, nil
}

func (w *serviceWire) send(message serviceMessage) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	message.Version = continuationVersion
	message.BootID = w.boot
	message.Operation = w.operation
	payload, err := json.Marshal(message)
	if err != nil {
		return err
	}
	if len(payload) > maxServiceMessage {
		return fmt.Errorf("proxy service message exceeds limit")
	}
	deadline := time.Now().Add(2 * time.Second)
	// A whole-service stop must reach the coordinator even if restoration has
	// already consumed its business budget. Preserve the expired deadline in
	// the packet while allowing this bounded control write to be delivered.
	if message.Kind != "shutdown" && !message.Deadline.IsZero() && message.Deadline.Before(deadline) {
		deadline = message.Deadline
	}
	_ = w.conn.SetWriteDeadline(deadline)
	var rights []byte
	if message.file != nil {
		rights = unix.UnixRights(int(message.file.Fd()))
	}
	_, _, err = w.conn.WriteMsgUnix(payload, rights, nil)
	return err
}

func (w *serviceWire) read() (serviceMessage, error) {
	buffer := make([]byte, maxServiceMessage)
	oob := make([]byte, unix.CmsgSpace(4))
	n, oobn, flags, _, err := w.conn.ReadMsgUnix(buffer, oob)
	if err != nil {
		return serviceMessage{}, err
	}
	var files []*os.File
	messages, parseErr := unix.ParseSocketControlMessage(oob[:oobn])
	for _, control := range messages {
		fds, e := unix.ParseUnixRights(&control)
		if e != nil {
			parseErr = e
			continue
		}
		for _, fd := range fds {
			unix.CloseOnExec(fd)
			files = append(files, os.NewFile(uintptr(fd), "proxy-candidate-config"))
		}
	}
	transferred := false
	defer func() {
		if !transferred {
			closeFiles(files)
		}
	}()
	if parseErr != nil {
		return serviceMessage{}, parseErr
	}
	if flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 {
		return serviceMessage{}, fmt.Errorf("proxy service message truncated")
	}
	var message serviceMessage
	if err := strictjson.Decode(buffer[:n], &message); err != nil {
		return message, err
	}
	if message.Version != continuationVersion || message.BootID != w.boot || message.Operation != w.operation {
		return message, fmt.Errorf("%w: service control identity", ErrIncompatible)
	}
	if len(files) != 0 {
		if len(files) != 1 || message.Kind != "config_connected" || message.Error != "" {
			return serviceMessage{}, fmt.Errorf("unexpected descriptor in service control")
		}
		message.file = files[0]
		transferred = true
	}
	return message, nil
}
