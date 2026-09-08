//go:build !windows

package process

import (
	"os/exec"
	"syscall"
)

// configureProcAttr puts the agent in its own process group.
//
// This matters more than it looks. An agent typically spawns children — a
// browser spawns renderer processes — and killing only the parent leaves those
// children running, holding memory and network connections. Since the whole
// point of this system is that nothing outlives its deadline, we need to signal
// the entire tree, and a process group is how you address one on Unix.
func configureProcAttr(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// killTree terminates the agent and every process it spawned.
//
// A negative PID signals the process group. SIGKILL rather than SIGTERM: this
// is called when a deadline has already passed or an operator asked for the
// environment to be destroyed, and at that point a process that ignores SIGTERM
// is precisely the process we most need to stop.
func killTree(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	pid := cmd.Process.Pid
	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil {
		// The group may already be gone, or setpgid may have failed. Fall back
		// to the single process rather than give up on the kill entirely.
		_ = cmd.Process.Kill()
	}
}

// killPID terminates a process group by PID. Used for orphans inherited from a
// previous control-plane process, where we have the PID but no exec.Cmd.
func killPID(pid int) {
	if pid <= 0 {
		return
	}
	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil {
		_ = syscall.Kill(pid, syscall.SIGKILL)
	}
}

// processAlive reports whether a PID is still running.
//
// Signal 0 performs the permission and existence checks without delivering
// anything. ESRCH means no such process; EPERM means it exists but belongs to
// someone else, which still counts as alive.
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	if err == nil {
		return true
	}
	return err == syscall.EPERM
}
