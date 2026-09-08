//go:build windows

package docker

import "os"

// defaultHost returns the daemon address to use when none is configured.
//
// The Windows default is a named pipe, which NewClient rejects with an
// explanation rather than pretending to support. That is deliberate: this
// project targets Linux and macOS for container work, and a half-working
// Windows path would be worse than a clear message pointing at WSL2 or a TCP
// listener.
func defaultHost() string {
	if v := os.Getenv("DOCKER_HOST"); v != "" {
		return v
	}
	return "npipe:////./pipe/docker_engine"
}
