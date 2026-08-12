package proxystats

import (
	"context"
	"errors"
	"io"
	"net"
	"time"
)

const streamWriteTimeout = 5 * time.Second

func StreamSender(conn net.Conn) func(Frame) error {
	return func(frame Frame) error {
		if err := conn.SetWriteDeadline(time.Now().Add(streamWriteTimeout)); err != nil {
			return err
		}
		err := WriteFrame(conn, frame)
		_ = conn.SetWriteDeadline(time.Time{})
		return err
	}
}

func (m *MasterStats) ReadWorkerStream(ctx context.Context, reader io.Reader, workerID string, epoch uint64) error {
	for {
		if err := ctx.Err(); err != nil {
			m.StreamFault(workerID, epoch)
			return err
		}
		frame, err := ReadFrame(reader)
		if err != nil {
			m.StreamFault(workerID, epoch)
			if errors.Is(err, io.EOF) && ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		if err := m.Receive(workerID, epoch, frame); err != nil {
			m.StreamFault(workerID, epoch)
			return err
		}
		if frame.Type == TypeGoodbye {
			return nil
		}
	}
}
