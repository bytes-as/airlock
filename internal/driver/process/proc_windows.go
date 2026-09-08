//go:build windows

package process

import (
	"os"
	"os/exec"
	"strconv"
	"syscall"
)

// stillActive is the exit code Windows reports for a process that has not
// exited yet (STILL_ACTIVE).
const stillActive = 259

// configureProcAttr puts the agent in its own process group.
//
// CREATE_NEW_PROCESS_GROUP is the closest Windows analogue to setpgid. It is
// not as strong: unlike a Unix process group, it does not by itself guarantee
// that killing the parent reaches every descendant. That is why killTree shells
// out to taskkill /T rather than relying on the group alone.
//
// The rigorous answer on Windows is a Job Object, which the kernel tears down
// as a unit. That needs golang.org/x/sys/windows, and this driver is the
// zero-isolation rung of the ladder — anyone who needs guaranteed containment
// should be on the docker driver, which gets it from the container runtime.
func configureProcAttr(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
}

// killTree terminates the agent and every process it spawned.
func killTree(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	killPID(cmd.Process.Pid)
}

// killPID terminates a process tree by PID.
//
// taskkill /T walks the child list and /F forces termination. We ignore its
// error: the usual cause is that the process already exited, which is the
// outcome we wanted. The fallback covers taskkill being unavailable.
func killPID(pid int) {
	if pid <= 0 {
		return
	}
	cmd := exec.Command("taskkill", "/F", "/T", "/PID", strconv.Itoa(pid))
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	if err := cmd.Run(); err != nil {
		if p, ferr := os.FindProcess(pid); ferr == nil {
			_ = p.Kill()
		}
	}
}

// processAlive reports whether a PID is still running.
//
// Opening the process and reading its exit code is more reliable than merely
// checking that the handle opens: a terminated process can remain openable
// while any handle to it is held, so an open handle alone would report a dead
// process as alive and leave a job hanging forever.
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	handle, err := syscall.OpenProcess(syscall.PROCESS_QUERY_INFORMATION, false, uint32(pid))
	if err != nil {
		return false
	}
	defer syscall.CloseHandle(handle)

	var code uint32
	if err := syscall.GetExitCodeProcess(handle, &code); err != nil {
		return false
	}
	return code == stillActive
}
