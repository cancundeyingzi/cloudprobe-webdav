//go:build windows

package main

import (
	"os/exec"
	"syscall"
)

func hideChildWindow(cmd *exec.Cmd) { cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true} }
func openLocalBrowser(address string) error {
	cmd := exec.Command("rundll32.exe", "url.dll,FileProtocolHandler", address)
	hideChildWindow(cmd)
	return cmd.Start()
}
