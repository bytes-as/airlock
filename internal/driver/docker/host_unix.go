//go:build !windows

package docker

import "os"

// defaultHost returns the daemon address to use when none is configured.
func defaultHost() string {
	if v := os.Getenv("DOCKER_HOST"); v != "" {
		return v
	}
	return "unix:///var/run/docker.sock"
}
