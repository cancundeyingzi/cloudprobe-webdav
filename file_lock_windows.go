//go:build windows

package main

import (
	"os"
	"syscall"
	"time"
)

const lockRetryDelay = 20 * time.Millisecond

func tryLockFile(path string) (*os.File, error) {
	pointer, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	handle, err := syscall.CreateFile(pointer, syscall.GENERIC_READ|syscall.GENERIC_WRITE, 0, nil, syscall.OPEN_ALWAYS, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(handle), path), nil
}
