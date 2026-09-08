// Package docker implements a Driver backed by the Docker Engine.
//
// It speaks the Engine HTTP API directly rather than depending on
// github.com/docker/docker. That SDK pulls in a very large dependency tree for
// the dozen endpoints we use, and the endpoints themselves are stable and
// simple. The cost is that we own the wire details — stream framing, tar
// extraction — so those are written as pure functions with their own tests,
// which is where the subtle bugs would otherwise hide.
package docker

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// APIVersion is pinned rather than negotiated. An unpinned client silently
// changes behaviour when the daemon is upgraded; a pinned one fails loudly if
// the daemon is too old, which is the failure we would rather have.
const APIVersion = "v1.43"

// Client is a minimal Docker Engine API client.
type Client struct {
	http *http.Client
	// base is the URL prefix for requests. For socket transports the host is a
	// placeholder, since the dialer ignores it.
	base string
}

// NewClient connects to the Docker daemon.
//
// host accepts the same forms as DOCKER_HOST: unix:///var/run/docker.sock,
// tcp://host:port, or empty for the platform default.
func NewClient(host string) (*Client, error) {
	if host == "" {
		host = defaultHost()
	}

	scheme, address, found := strings.Cut(host, "://")
	if !found {
		return nil, fmt.Errorf("docker: cannot parse host %q; expected unix:///path or tcp://host:port", host)
	}

	switch scheme {
	case "unix":
		return &Client{
			base: "http://docker",
			http: &http.Client{
				Transport: &http.Transport{
					DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
						var d net.Dialer
						return d.DialContext(ctx, "unix", address)
					},
				},
			},
		}, nil

	case "tcp", "http":
		return &Client{
			base: "http://" + address,
			http: &http.Client{},
		}, nil

	case "npipe":
		// Named pipes need golang.org/x/sys/windows or github.com/Microsoft/
		// go-winio to dial. Rather than add a Windows-only dependency for a
		// path this project does not target, say exactly what to do instead.
		return nil, errors.New("docker: named pipes are not supported; " +
			"set DOCKER_HOST to tcp://localhost:2375 (enable the daemon's TCP listener) " +
			"or run the control plane inside WSL2 where the unix socket is available")

	default:
		return nil, fmt.Errorf("docker: unsupported transport %q in host %q", scheme, host)
	}
}

// Ping verifies the daemon is reachable and reports its version.
func (c *Client) Ping(ctx context.Context) (string, error) {
	var info struct {
		Version    string `json:"Version"`
		APIVersion string `json:"ApiVersion"`
	}
	if err := c.getJSON(ctx, "/version", &info); err != nil {
		return "", fmt.Errorf("docker: daemon not reachable: %w", err)
	}
	return info.Version, nil
}

// --- containers ---

// ContainerConfig is the create-container payload.
//
// Only the fields this driver sets are modelled. A full mirror of the Engine
// schema would be mostly dead weight, and every field present here is one we
// deliberately chose.
type ContainerConfig struct {
	Image      string            `json:"Image"`
	Cmd        []string          `json:"Cmd,omitempty"`
	Env        []string          `json:"Env,omitempty"`
	Labels     map[string]string `json:"Labels,omitempty"`
	WorkingDir string            `json:"WorkingDir,omitempty"`
	// User runs the agent as a non-root account inside the container. Untrusted
	// code should not be uid 0 even inside a namespace: a container escape is
	// far more useful to an attacker who is already root.
	User            string     `json:"User,omitempty"`
	AttachStdout    bool       `json:"AttachStdout"`
	AttachStderr    bool       `json:"AttachStderr"`
	NetworkDisabled bool       `json:"NetworkDisabled,omitempty"`
	HostConfig      HostConfig `json:"HostConfig"`
}

// HostConfig carries the isolation and resource controls.
type HostConfig struct {
	// Memory is the hard limit in bytes. Exceeding it triggers an OOM kill,
	// which the driver reports distinctly from an agent crash.
	Memory int64 `json:"Memory,omitempty"`
	// NanoCPUs is the CPU limit: 1e9 equals one core.
	NanoCPUs int64 `json:"NanoCpus,omitempty"`
	// PidsLimit caps process count, which is what stops a fork bomb in one job
	// taking down the host and every other job on it.
	PidsLimit int64 `json:"PidsLimit,omitempty"`
	// NetworkMode selects the network: "none", "bridge", or a named network.
	NetworkMode string `json:"NetworkMode,omitempty"`
	// ReadonlyRootfs makes the image read-only; the agent writes only to the
	// tmpfs and volumes we grant it.
	ReadonlyRootfs bool `json:"ReadonlyRootfs,omitempty"`
	// Tmpfs are in-memory writable mounts. Using tmpfs rather than a host bind
	// means the job's filesystem vanishes with the container by construction,
	// which is a stronger guarantee than remembering to delete a directory.
	Tmpfs map[string]string `json:"Tmpfs,omitempty"`
	// CapDrop removes Linux capabilities. Dropping ALL and adding nothing back
	// is the right default for code we did not write.
	CapDrop []string `json:"CapDrop,omitempty"`
	// SecurityOpt carries no-new-privileges, which prevents a setuid binary
	// inside the container from regaining privileges we just dropped.
	SecurityOpt []string `json:"SecurityOpt,omitempty"`
	// AutoRemove is deliberately false. Letting the daemon delete containers
	// would race the driver's own collection of logs and artifacts, and would
	// hide exit codes from the reaper.
	AutoRemove bool `json:"AutoRemove"`
	// ExtraHosts pins names in /etc/hosts.
	ExtraHosts []string `json:"ExtraHosts,omitempty"`
}

// ContainerCreate creates a container and returns its ID.
func (c *Client) ContainerCreate(ctx context.Context, name string, cfg ContainerConfig) (string, error) {
	query := url.Values{}
	if name != "" {
		query.Set("name", name)
	}

	var created struct {
		ID       string   `json:"Id"`
		Warnings []string `json:"Warnings"`
	}
	if err := c.postJSON(ctx, "/containers/create?"+query.Encode(), cfg, &created); err != nil {
		return "", err
	}
	return created.ID, nil
}

func (c *Client) ContainerStart(ctx context.Context, id string) error {
	return c.postJSON(ctx, "/containers/"+id+"/start", nil, nil)
}

// ContainerState is the subset of an inspect response the driver uses.
type ContainerState struct {
	Status     string `json:"Status"` // created, running, paused, exited, dead
	Running    bool   `json:"Running"`
	ExitCode   int    `json:"ExitCode"`
	OOMKilled  bool   `json:"OOMKilled"`
	Error      string `json:"Error"`
	StartedAt  string `json:"StartedAt"`
	FinishedAt string `json:"FinishedAt"`
}

// ContainerInspect returns a container's state.
func (c *Client) ContainerInspect(ctx context.Context, id string) (ContainerState, error) {
	var inspect struct {
		State ContainerState `json:"State"`
	}
	if err := c.getJSON(ctx, "/containers/"+id+"/json", &inspect); err != nil {
		return ContainerState{}, err
	}
	return inspect.State, nil
}

// ContainerWait blocks until the container exits and returns its exit code.
func (c *Client) ContainerWait(ctx context.Context, id string) (int, error) {
	var result struct {
		StatusCode int `json:"StatusCode"`
		Error      *struct {
			Message string `json:"Message"`
		} `json:"Error"`
	}
	if err := c.postJSON(ctx, "/containers/"+id+"/wait?condition=not-running", nil, &result); err != nil {
		return 0, err
	}
	if result.Error != nil && result.Error.Message != "" {
		return result.StatusCode, fmt.Errorf("docker: wait reported %s", result.Error.Message)
	}
	return result.StatusCode, nil
}

// ContainerLogs opens the container's combined output stream.
func (c *Client) ContainerLogs(ctx context.Context, id string, follow bool) (io.ReadCloser, error) {
	query := url.Values{}
	query.Set("stdout", "1")
	query.Set("stderr", "1")
	query.Set("timestamps", "0")
	if follow {
		query.Set("follow", "1")
	}
	return c.getStream(ctx, "/containers/"+id+"/logs?"+query.Encode())
}

// ContainerRemove deletes a container and its anonymous volumes.
func (c *Client) ContainerRemove(ctx context.Context, id string, force bool) error {
	query := url.Values{}
	query.Set("v", "1") // remove anonymous volumes, or disk usage grows silently
	if force {
		query.Set("force", "1")
	}
	err := c.do(ctx, http.MethodDelete, "/containers/"+id+"?"+query.Encode(), nil, nil)
	if IsNotFound(err) {
		return nil // Already gone is the outcome we wanted.
	}
	return err
}

// ContainerSummary is one entry from a container listing.
type ContainerSummary struct {
	ID      string            `json:"Id"`
	Names   []string          `json:"Names"`
	Image   string            `json:"Image"`
	State   string            `json:"State"`
	Status  string            `json:"Status"`
	Created int64             `json:"Created"`
	Labels  map[string]string `json:"Labels"`
}

// ContainerList returns containers carrying the given labels, including stopped
// ones. Listing by label is what lets the reaper find environments created by a
// control plane that has since died.
func (c *Client) ContainerList(ctx context.Context, labels map[string]string) ([]ContainerSummary, error) {
	filters := map[string][]string{}
	var labelFilters []string
	for key, value := range labels {
		if value == "" {
			labelFilters = append(labelFilters, key)
			continue
		}
		labelFilters = append(labelFilters, key+"="+value)
	}
	if len(labelFilters) > 0 {
		filters["label"] = labelFilters
	}

	encoded, err := json.Marshal(filters)
	if err != nil {
		return nil, err
	}
	query := url.Values{}
	query.Set("all", "1")
	query.Set("filters", string(encoded))

	var summaries []ContainerSummary
	if err := c.getJSON(ctx, "/containers/json?"+query.Encode(), &summaries); err != nil {
		return nil, err
	}
	return summaries, nil
}

// CopyFromContainer returns a tar stream of a path inside the container.
func (c *Client) CopyFromContainer(ctx context.Context, id, path string) (io.ReadCloser, error) {
	query := url.Values{}
	query.Set("path", path)
	return c.getStream(ctx, "/containers/"+id+"/archive?"+query.Encode())
}

// --- images ---

// ImagePull pulls an image, blocking until it is complete.
func (c *Client) ImagePull(ctx context.Context, image string) error {
	query := url.Values{}
	query.Set("fromImage", imageRef(image))

	stream, err := c.postStream(ctx, "/images/create?"+query.Encode())
	if err != nil {
		return err
	}
	defer stream.Close()

	// The pull endpoint returns 200 immediately and reports failures inside the
	// stream. Draining it and checking for an error object is the only way to
	// know whether the pull actually worked; ignoring the body means a failed
	// pull looks like a success until the container fails to start.
	decoder := json.NewDecoder(stream)
	for {
		var progress struct {
			Error string `json:"error"`
		}
		if err := decoder.Decode(&progress); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("docker: reading pull progress: %w", err)
		}
		if progress.Error != "" {
			return fmt.Errorf("docker: pulling %s: %s", image, progress.Error)
		}
	}
}

// ImageExists reports whether an image is present locally.
func (c *Client) ImageExists(ctx context.Context, image string) (bool, error) {
	err := c.getJSON(ctx, "/images/"+url.PathEscape(image)+"/json", nil)
	if IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// imageRef ensures a tag is present, since the pull endpoint defaults to
// pulling every tag when none is given.
func imageRef(image string) string {
	if strings.Contains(image, "@") {
		return image // digest reference
	}
	// A colon after the last slash means there is already a tag.
	lastSlash := strings.LastIndex(image, "/")
	if strings.Contains(image[lastSlash+1:], ":") {
		return image
	}
	return image + ":latest"
}

// --- networks ---

// NetworkCreate creates a network, ignoring the error if it already exists.
type NetworkSpec struct {
	Name string `json:"Name"`
	// Driver is normally "bridge".
	Driver string `json:"Driver,omitempty"`
	// Internal removes the network's route to the outside world.
	//
	// This is the strongest egress control available without touching host
	// firewall rules, and it is structural rather than a rule that could be
	// mis-specified: a container on an internal network has no route to
	// 169.254.169.254 because it has no route anywhere off the network.
	Internal   bool              `json:"Internal,omitempty"`
	Attachable bool              `json:"Attachable,omitempty"`
	Labels     map[string]string `json:"Labels,omitempty"`
}

func (c *Client) NetworkCreate(ctx context.Context, spec NetworkSpec) error {
	err := c.postJSON(ctx, "/networks/create", spec, nil)
	if err != nil && strings.Contains(err.Error(), "already exists") {
		return nil
	}
	return err
}

// NetworkExists reports whether a network is present.
func (c *Client) NetworkExists(ctx context.Context, name string) (bool, error) {
	err := c.getJSON(ctx, "/networks/"+url.PathEscape(name), nil)
	if IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// --- log stream framing ---

// Stream identifies which output a framed chunk came from.
type Stream uint8

const (
	StreamStdin  Stream = 0
	StreamStdout Stream = 1
	StreamStderr Stream = 2
)

// Frame is one chunk of a multiplexed Docker log stream.
type Frame struct {
	Stream Stream
	Data   []byte
}

// ReadFrame reads one frame from a multiplexed Docker stream.
//
// The framing is an 8-byte header — one byte of stream type, three reserved
// bytes, then a big-endian uint32 length — followed by that many payload bytes.
// Getting this wrong does not fail loudly; it produces log output with binary
// garbage sprinkled through it, which is why it is a separate, tested function.
//
// Returns io.EOF at the clean end of the stream.
func ReadFrame(r *bufio.Reader) (Frame, error) {
	var header [8]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return Frame{}, err
	}

	stream := Stream(header[0])
	size := binary.BigEndian.Uint32(header[4:8])

	// A hostile or corrupt stream must not be able to make us allocate
	// gigabytes on the strength of a length field.
	const maxFrame = 16 << 20
	if size > maxFrame {
		return Frame{}, fmt.Errorf("docker: log frame of %d bytes exceeds the %d byte limit", size, maxFrame)
	}
	if size == 0 {
		return Frame{Stream: stream}, nil
	}

	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil {
		if errors.Is(err, io.EOF) {
			// A truncated frame is a broken stream, not a clean end. Say so,
			// or a partial log looks like a complete one.
			return Frame{}, io.ErrUnexpectedEOF
		}
		return Frame{}, err
	}
	return Frame{Stream: stream, Data: data}, nil
}

// --- HTTP plumbing ---

// APIError is a non-2xx response from the daemon.
type APIError struct {
	Status  int
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("docker: api returned %d: %s", e.Status, e.Message)
}

// IsNotFound reports whether an error is a 404 from the daemon.
func IsNotFound(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound
}

// IsConflict reports whether an error is a 409, which usually means the
// resource is in a state that forbids the operation.
func IsConflict(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.Status == http.StatusConflict
}

func (c *Client) url(path string) string {
	return c.base + "/" + APIVersion + path
}

func (c *Client) do(ctx context.Context, method, path string, body any, out any) error {
	stream, err := c.doStream(ctx, method, path, body)
	if err != nil {
		return err
	}
	defer stream.Close()

	if out == nil {
		_, _ = io.Copy(io.Discard, stream)
		return nil
	}
	return json.NewDecoder(stream).Decode(out)
}

func (c *Client) doStream(ctx context.Context, method, path string, body any) (io.ReadCloser, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("docker: encode request: %w", err)
		}
		reader = strings.NewReader(string(encoded))
	}

	req, err := http.NewRequestWithContext(ctx, method, c.url(path), reader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("docker: %s %s: %w", method, path, err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))

		message := strings.TrimSpace(string(raw))
		var apiMsg struct {
			Message string `json:"message"`
		}
		if json.Unmarshal(raw, &apiMsg) == nil && apiMsg.Message != "" {
			message = apiMsg.Message
		}
		return nil, &APIError{Status: resp.StatusCode, Message: message}
	}
	return resp.Body, nil
}

func (c *Client) getJSON(ctx context.Context, path string, out any) error {
	return c.do(ctx, http.MethodGet, path, nil, out)
}

func (c *Client) postJSON(ctx context.Context, path string, body, out any) error {
	return c.do(ctx, http.MethodPost, path, body, out)
}

func (c *Client) getStream(ctx context.Context, path string) (io.ReadCloser, error) {
	return c.doStream(ctx, http.MethodGet, path, nil)
}

func (c *Client) postStream(ctx context.Context, path string) (io.ReadCloser, error) {
	return c.doStream(ctx, http.MethodPost, path, nil)
}

// parseDockerTime parses the RFC3339Nano timestamps the Engine returns.
// A zero container timestamp is reported as a zero time rather than an error.
func parseDockerTime(value string) time.Time {
	if value == "" || strings.HasPrefix(value, "0001-01-01") {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}
	}
	return t
}
