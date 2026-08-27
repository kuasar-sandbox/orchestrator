package main

import (
	"os"
	"syscall"
)

func newSocketpair() (master, worker *os.File, err error) {
	descriptors, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, nil, err
	}
	return os.NewFile(uintptr(descriptors[0]), "test-master"), os.NewFile(uintptr(descriptors[1]), "test-worker"), nil
}
