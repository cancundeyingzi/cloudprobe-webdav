//go:build !windows

package main

func protectLocal(data []byte) ([]byte, error)   { return append([]byte(nil), data...), nil }
func unprotectLocal(data []byte) ([]byte, error) { return append([]byte(nil), data...), nil }
