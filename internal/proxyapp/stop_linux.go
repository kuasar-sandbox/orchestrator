package proxyapp

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// stopRecord persists the first full-service stop deadline across exec. Only
// the main coordinator writes it; B forwards public stop requests over control.
// WriteAt completes before taking the launch/exec mutex, so an accepted stop cannot
// disappear with a Go goroutine during the image transition.
type stopRecord struct {
	mu   sync.Mutex
	file *os.File
}

// The state mutex covers publication and a stop racing the initial record
// allocation or continuation restore. Preserve its first deadline rather than
// starting another budget when the descriptor becomes available.
func (s *Service) installStopRecordLocked(record *stopRecord) error {
	if s.stopRequested {
		deadline, err := record.request(s.stopDeadline)
		if err != nil {
			return err
		}
		s.stopDeadline = deadline
	}
	s.stopRecord = record
	return nil
}

func newStopRecord() (*stopRecord, error) {
	fd, err := unix.MemfdCreate("kuasar-proxy-service-stop", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), "kuasar-proxy-service-stop")
	if err := file.Truncate(8); err != nil {
		file.Close()
		return nil, err
	}
	if _, err := unix.FcntlInt(file.Fd(), unix.F_ADD_SEALS, unix.F_SEAL_GROW|unix.F_SEAL_SHRINK|unix.F_SEAL_SEAL); err != nil {
		file.Close()
		return nil, err
	}
	return &stopRecord{file: file}, nil
}

// read distinguishes no request from an explicit stop without a deadline.
func (r *stopRecord) read() (time.Time, bool, error) {
	var value [8]byte
	if _, err := r.file.ReadAt(value[:], 0); err != nil {
		return time.Time{}, false, err
	}
	stamp := int64(binary.LittleEndian.Uint64(value[:]))
	if stamp == 0 {
		return time.Time{}, false, nil
	}
	if stamp == -1 {
		return time.Time{}, true, nil
	}
	if stamp < 0 {
		return time.Time{}, false, fmt.Errorf("proxy stop deadline is invalid")
	}
	return time.Unix(0, stamp), true, nil
}

func (r *stopRecord) request(deadline time.Time) (time.Time, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	prior, requested, err := r.read()
	if err != nil || requested {
		return prior, err
	}
	var value [8]byte
	stamp := int64(-1)
	if !deadline.IsZero() {
		stamp = deadline.UnixNano()
	}
	binary.LittleEndian.PutUint64(value[:], uint64(stamp))
	if _, err := r.file.WriteAt(value[:], 0); err != nil {
		return time.Time{}, err
	}
	return deadline, nil
}

func receiveStopRecord(file *os.File) (*stopRecord, error) {
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !stat.Mode().IsRegular() || stat.Size() != 8 {
		return nil, fmt.Errorf("proxy stop record has invalid type or size")
	}
	flags, err := unix.FcntlInt(file.Fd(), unix.F_GET_SEALS, 0)
	want := unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL
	if err != nil || flags&want != want || flags&unix.F_SEAL_WRITE != 0 {
		return nil, fmt.Errorf("proxy stop record has invalid seals")
	}
	record := &stopRecord{file: file}
	if _, _, err := record.read(); err != nil {
		return nil, err
	}
	return record, nil
}
