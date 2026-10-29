//go:build !windows

package main

import (
	"os/exec"
	"runtime"
)

func hideChildWindow(cmd *exec.Cmd) {}
func openLocalBrowser(address string) error {
	if runtime.GOOS == "darwin" {
		return exec.Command("open", address).Start()
	}
	return exec.Command("xdg-open", address).Start()
}
