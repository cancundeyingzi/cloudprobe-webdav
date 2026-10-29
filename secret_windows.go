//go:build windows

package main

import (
	"fmt"
	"syscall"
	"unsafe"
)

const localCredentialsEncrypted = true

type localBlob struct {
	Size uint32
	Data *byte
}

func localCrypt(data []byte, protect bool) ([]byte, error) {
	crypt := syscall.NewLazyDLL("crypt32.dll")
	name := "CryptProtectData"
	if !protect {
		name = "CryptUnprotectData"
	}
	proc := crypt.NewProc(name)
	var in, out localBlob
	if len(data) > 0 {
		in = localBlob{Size: uint32(len(data)), Data: &data[0]}
	}
	ok, _, e := proc.Call(uintptr(unsafe.Pointer(&in)), 0, 0, 0, 0, 1, uintptr(unsafe.Pointer(&out)))
	if ok == 0 {
		return nil, fmt.Errorf("Windows 凭据保护失败：%w", e)
	}
	defer syscall.NewLazyDLL("kernel32.dll").NewProc("LocalFree").Call(uintptr(unsafe.Pointer(out.Data)))
	return append([]byte(nil), unsafe.Slice(out.Data, int(out.Size))...), nil
}
func protectLocal(data []byte) ([]byte, error)   { return localCrypt(data, true) }
func unprotectLocal(data []byte) ([]byte, error) { return localCrypt(data, false) }
