//go:build !windows

package main

import (
	"errors"
	"os"
	"syscall"
)

// processAlive 用 signal 0 探測；EPERM 代表進程存在但不是我們的。
func processAlive(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = process.Signal(syscall.Signal(0))

	return err == nil || errors.Is(err, syscall.EPERM)
}
