//go:build windows

package main

import "os"

// processAlive 在 Windows 上靠 OpenProcess 是否成功判斷。
func processAlive(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	_ = process.Release()

	return true
}
