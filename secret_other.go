//go:build !windows

package main

const localCredentialsEncrypted = false

func protectLocal(data []byte) ([]byte, error)   { return append([]byte(nil), data...), nil }
func unprotectLocal(data []byte) ([]byte, error) { return append([]byte(nil), data...), nil }
