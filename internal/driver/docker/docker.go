package docker

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bytes-as/ephemera/internal/driver"
	"github.com/bytes-as/ephemera/internal/driver/archive"
	"github.com/bytes-as/ephemera/internal/job"
)

// Label keys applied to every container, so environments remain identifiable as
// ours after the control plane that created them is gone.
const (
	LabelManaged = "ephemera.managed"
	LabelJob     = "ephemera.job"
	LabelTenant  = "ephemera.tenant"
	LabelExpires = "ephemera.expires-at"
	LabelCreated = "ephemera.created-at"
	// LabelArtifactDir records the host directory bound at artifactMount.
	//
	// Written on the container so Collect and Destroy can find the artifacts of
	// an environment this process did not create - after a control-plane
	// restart the reaper still needs to clean the directory up.
	LabelArtifactDir = "ephemera.artifact-dir"
)

// artifactMount is where agents write their outputs inside the container.
const artifactMount = "/artifacts"

// Config tunes the driver.
type Config struct {
	// Host is the daemon address. Empty uses DOCKER_HOST or the platform default.
	Host string

	// Network is the network job containers join. Empty means the default bridge.
	//
	// For egress control this should name an internal network — see
	// EgressModel in the package docs.
	Network string

	// ProxyURL, when set, is injected as HTTP_PROXY/HTTPS_PROXY so an agent on
	// an internal network can still reach the internet through a controlled hop.
	ProxyURL string

	// NoProxyHosts are exempted from the proxy, e.g. an in-cluster service.
	NoProxyHosts string

	// User is the account the agent runs as inside the container. Defaults to a
	// non-root uid: untrusted code should not be root even inside a namespace,
	// because a container escape is far more useful to an attacker who already is.
	User string

	// PullPolicy is "always", "if-missing" or "never".
	PullPolicy string

	// DefaultMemoryMiB and DefaultCPUMillis apply when a job requests none. A
	// job with no limits can take the whole host, so there is always a limit.
	DefaultMemoryMiB int64
	DefaultCPUMillis int64

	// PidsLimit caps processes per container, which is what stops a fork bomb
	// in one job taking down every other job on the host.
	PidsLimit int64

	// TmpfsSizeMiB is the size of the writable in-memory scratch filesystem
	// granted to each job at /tmp. The root filesystem is read-only, so this is
	// where the agent scribbles — and it vanishes with the container.
	TmpfsSizeMiB int64

	// ArtifactRoot is the host directory under which each job gets its own
	// artifact directory, bind-mounted at /artifacts.
	//
	// It cannot be a tmpfs: tmpfs is unmounted when the container exits, so
	// artifacts written there are destroyed before Collect can read them.
	// Empty means a subdirectory of the OS temp directory.
	ArtifactRoot string
}

// DefaultConfig returns settings suitable for running untrusted agents.
func DefaultConfig() Config {
	return Config{
		User:             "65534:65534", // nobody:nogroup on most images
		PullPolicy:       "if-missing",
		DefaultMemoryMiB: 1024,
		DefaultCPUMillis: 1000,
		PidsLimit:        256,
		TmpfsSizeMiB:     512,
	}
}

// Driver runs each job in its own container.
type Driver struct {
	cfg    Config
	client *Client
	now    func() time.Time

	mu        sync.Mutex
	pulled    map[string]bool
	networkOK bool
}

// Option configures a Driver.
type Option func(*Driver)

// WithClock overrides the time source.
func WithClock(now func() time.Time) Option {
	return func(d *Driver) { d.now = now }
}

// New connects to the daemon and returns a driver.
//
// It pings the daemon rather than deferring the failure to the first job. A
// control plane that starts happily and then fails every job is harder to
// diagnose than one that refuses to start with "daemon not reachable".
func New(cfg Config, opts ...Option) (*Driver, error) {
	client, err := NewClient(cfg.Host)
	if err != nil {
		return nil, err
	}

	d := &Driver{
		cfg:    cfg,
		client: client,
		now:    time.Now,
		pulled: map[string]bool{},
	}
	for _, opt := range opts {
		opt(d)
	}

	if d.cfg.ArtifactRoot == "" {
		d.cfg.ArtifactRoot = filepath.Join(os.TempDir(), "ephemera-artifacts")
	}
	root, err := filepath.Abs(d.cfg.ArtifactRoot)
	if err != nil {
		return nil, fmt.Errorf("docker driver: artifact root: %w", err)
	}
	d.cfg.ArtifactRoot = root
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, fmt.Errorf("docker driver: create artifact root: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := client.Ping(ctx); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *Driver) Name() string { return "docker" }

// Capabilities reports what this driver enforces.
//
// EnforcesNetworkPolicy is true, but Create is strict about which policies it
// will accept — see the comment there. Claiming the capability and then
// silently ignoring a deny list would be exactly the dishonesty this design
// exists to prevent.
func (d *Driver) Capabilities() driver.Capabilities {
	return driver.Capabilities{
		Isolation:              driver.IsolationContainer,
		EnforcesResourceLimits: true,
		EnforcesNetworkPolicy:  true,
		// Containers are labelled, and the daemon outlives the control plane,
		// so a fresh process can enumerate and reap what an old one left.
		SurvivesControlPlaneRestart: true,
	}
}

// Create provisions a container for the job without starting it.
func (d *Driver) Create(ctx context.Context, spec driver.EnvSpec) (driver.Env, error) {
	if spec.Image == "" {
		return driver.Env{}, errors.New("docker driver: spec has no image")
	}

	if err := d.ensureNetwork(ctx); err != nil {
		return driver.Env{}, err
	}
	if err := d.ensureImage(ctx, spec.Image); err != nil {
		return driver.Env{}, fmt.Errorf("docker driver: %w", err)
	}

	networkMode, err := d.networkModeFor(spec.Network)
	if err != nil {
		return driver.Env{}, err
	}

	now := d.now()
	var expires time.Time
	if spec.Deadline > 0 {
		expires = now.Add(spec.Deadline)
	}

	name := "ephemera-" + sanitiseName(spec.JobID) + "-" + strconv.FormatInt(now.UnixNano()%1e6, 36)

	// The artifact directory is created on the host before the container, and
	// bound in below. 0o777 because the agent runs as an unprivileged uid that
	// this process cannot chown to; the directory is per-job and removed by
	// Destroy, so the exposure is one job's own output.
	hostArtifacts := filepath.Join(d.cfg.ArtifactRoot, name)
	if err := os.MkdirAll(hostArtifacts, 0o777); err != nil {
		return driver.Env{}, fmt.Errorf("docker driver: create artifact dir: %w", err)
	}
	if err := os.Chmod(hostArtifacts, 0o777); err != nil {
		return driver.Env{}, fmt.Errorf("docker driver: chmod artifact dir: %w", err)
	}

	labels := map[string]string{
		LabelManaged:     "true",
		LabelJob:         spec.JobID,
		LabelTenant:      spec.TenantID,
		LabelCreated:     now.UTC().Format(time.RFC3339),
		LabelArtifactDir: hostArtifacts,
	}
	if !expires.IsZero() {
		// The expiry is written on the container itself, so the reaper can
		// enforce a deadline for a job whose record it cannot find.
		labels[LabelExpires] = expires.UTC().Format(time.RFC3339)
	}
	for key, value := range spec.Labels {
		labels[key] = value
	}

	cfg := ContainerConfig{
		Image:        spec.Image,
		Cmd:          spec.Command,
		Env:          d.buildEnv(spec),
		Labels:       labels,
		User:         d.cfg.User,
		WorkingDir:   artifactMount,
		AttachStdout: true,
		AttachStderr: true,
		HostConfig: HostConfig{
			Memory:      memoryBytes(spec.Resources.MemoryMiB, d.cfg.DefaultMemoryMiB),
			NanoCPUs:    nanoCPUs(spec.Resources.CPUMillis, d.cfg.DefaultCPUMillis),
			PidsLimit:   d.cfg.PidsLimit,
			NetworkMode: networkMode,
			// The image is read-only. Scratch space is tmpfs and vanishes with
			// the container; the artifact directory is a host bind, because it
			// has to outlive the container to be collectable at all.
			ReadonlyRootfs: true,
			Tmpfs: map[string]string{
				"/tmp": fmt.Sprintf("rw,size=%dm,mode=1777", d.cfg.TmpfsSizeMiB),
			},
			Binds:       []string{hostArtifacts + ":" + artifactMount + ":rw"},
			CapDrop:     []string{"ALL"},
			SecurityOpt: []string{"no-new-privileges"},
			AutoRemove:  false,
		},
	}
	if spec.Network.Mode == driver.NetworkNone {
		cfg.NetworkDisabled = true
	}

	id, err := d.client.ContainerCreate(ctx, name, cfg)
	if err != nil {
		// No container means nothing will ever call Destroy for this directory.
		os.RemoveAll(hostArtifacts)
		return driver.Env{}, fmt.Errorf("docker driver: create container: %w", err)
	}

	return driver.Env{
		ID:        id,
		Driver:    d.Name(),
		JobID:     spec.JobID,
		TenantID:  spec.TenantID,
		CreatedAt: now,
		ExpiresAt: expires,
	}, nil
}

// networkModeFor maps a policy to a Docker network, refusing what it cannot
// genuinely enforce.
//
// The honesty rule in practice. Docker can enforce two of the three postures
// structurally:
//
//   - NetworkNone: no interface at all. Nothing to reach, nothing to police.
//   - NetworkProxied: an *internal* network, which has no route off itself.
//     169.254.169.254 is unreachable because everything is unreachable except
//     the proxy, which is the only container bridging both networks. The deny
//     is structural rather than a rule that could be mis-specified.
//
// It cannot enforce a CIDR deny list on an ordinary bridge network: that needs
// rules in the host's DOCKER-USER iptables chain, which is host configuration
// this driver does not own and could not apply portably (Docker Desktop runs
// the daemon inside a VM). So a spec asking for one is refused, with the
// message naming the mode that does provide it.
func (d *Driver) networkModeFor(policy driver.NetworkPolicy) (string, error) {
	switch policy.Mode {
	case driver.NetworkNone:
		return "none", nil

	case driver.NetworkProxied:
		if d.cfg.Network == "" {
			return "", &driver.UnsupportedError{
				Driver:  d.Name(),
				Feature: "proxied networking without a configured internal network (set --docker-network)",
			}
		}
		return d.cfg.Network, nil

	case "", driver.NetworkEgress:
		if len(policy.DenyCIDRs) > 0 {
			return "", &driver.UnsupportedError{
				Driver: d.Name(),
				Feature: "CIDR deny lists on a bridge network; these require host firewall rules " +
					"this driver does not own. Use network mode \"proxied\" with an internal " +
					"network, where the deny is structural",
			}
		}
		if d.cfg.Network != "" {
			return d.cfg.Network, nil
		}
		return "bridge", nil

	default:
		return "", &driver.UnsupportedError{
			Driver:  d.Name(),
			Feature: fmt.Sprintf("network mode %q", policy.Mode),
		}
	}
}

// ensureNetwork creates the configured network if it does not exist.
func (d *Driver) ensureNetwork(ctx context.Context) error {
	if d.cfg.Network == "" {
		return nil
	}
	d.mu.Lock()
	done := d.networkOK
	d.mu.Unlock()
	if done {
		return nil
	}

	exists, err := d.client.NetworkExists(ctx, d.cfg.Network)
	if err != nil {
		return fmt.Errorf("docker driver: check network: %w", err)
	}
	if !exists {
		// Created internal: a container on it has no route off the network, so
		// cloud metadata and the host's other services are unreachable by
		// construction rather than by rule.
		err := d.client.NetworkCreate(ctx, NetworkSpec{
			Name:     d.cfg.Network,
			Driver:   "bridge",
			Internal: true,
			Labels:   map[string]string{LabelManaged: "true"},
		})
		if err != nil {
			return fmt.Errorf("docker driver: create network %s: %w", d.cfg.Network, err)
		}
	}

	d.mu.Lock()
	d.networkOK = true
	d.mu.Unlock()
	return nil
}

// ensureImage applies the pull policy.
func (d *Driver) ensureImage(ctx context.Context, image string) error {
	switch strings.ToLower(d.cfg.PullPolicy) {
	case "never":
		return nil

	case "always":
		return d.client.ImagePull(ctx, image)

	default: // if-missing
		d.mu.Lock()
		known := d.pulled[image]
		d.mu.Unlock()
		if known {
			return nil
		}

		exists, err := d.client.ImageExists(ctx, image)
		if err != nil {
			return err
		}
		if !exists {
			if err := d.client.ImagePull(ctx, image); err != nil {
				return err
			}
		}
		d.mu.Lock()
		d.pulled[image] = true
		d.mu.Unlock()
		return nil
	}
}

// buildEnv assembles the container environment. Secrets are merged here and
// nowhere else, so this is the whole surface on which a credential can reach
// the agent — and none of it is written to a label, a name, or a log.
func (d *Driver) buildEnv(spec driver.EnvSpec) []string {
	env := make([]string, 0, len(spec.Env)+len(spec.Secrets)+6)

	env = append(env,
		"EPHEMERA_JOB_ID="+spec.JobID,
		"EPHEMERA_TENANT_ID="+spec.TenantID,
		"EPHEMERA_ARTIFACT_DIR="+artifactMount,
	)

	if spec.Network.Mode == driver.NetworkProxied && d.cfg.ProxyURL != "" {
		// Both cases, because different tools read different spellings.
		env = append(env,
			"HTTP_PROXY="+d.cfg.ProxyURL,
			"HTTPS_PROXY="+d.cfg.ProxyURL,
			"http_proxy="+d.cfg.ProxyURL,
			"https_proxy="+d.cfg.ProxyURL,
		)
		if d.cfg.NoProxyHosts != "" {
			env = append(env, "NO_PROXY="+d.cfg.NoProxyHosts, "no_proxy="+d.cfg.NoProxyHosts)
		}
	}

	for key, value := range spec.Env {
		env = append(env, key+"="+value)
	}
	for key, value := range spec.Secrets {
		env = append(env, key+"="+value.Reveal())
	}
	return env
}

// Start begins the agent.
func (d *Driver) Start(ctx context.Context, env driver.Env, spec driver.EnvSpec) error {
	if err := d.client.ContainerStart(ctx, env.ID); err != nil {
		return fmt.Errorf("docker driver: start container: %w", err)
	}
	return nil
}

// Logs streams container output, demultiplexing Docker's framing.
func (d *Driver) Logs(ctx context.Context, env driver.Env) (<-chan driver.LogLine, error) {
	stream, err := d.client.ContainerLogs(ctx, env.ID, true)
	if err != nil {
		if IsNotFound(err) {
			return nil, driver.ErrNotFound
		}
		return nil, fmt.Errorf("docker driver: open logs: %w", err)
	}

	out := make(chan driver.LogLine, 64)
	go func() {
		defer close(out)
		defer stream.Close()

		reader := bufio.NewReaderSize(stream, 64*1024)
		// Partial lines accumulate per stream: a frame boundary does not
		// respect line boundaries, so a log line can arrive split across two
		// frames and must not be emitted as two lines.
		partial := map[Stream][]byte{}

		emit := func(s Stream, text string) bool {
			line := driver.LogLine{At: d.now(), Stream: streamName(s), Text: text}
			select {
			case out <- line:
				return true
			case <-ctx.Done():
				return false
			}
		}

		for {
			if ctx.Err() != nil {
				return
			}
			frame, err := ReadFrame(reader)
			if err != nil {
				// Flush whatever was buffered: the last line before a crash is
				// usually the one that explains it, and it often lacks a
				// trailing newline.
				for s, buf := range partial {
					if len(buf) > 0 {
						emit(s, string(buf))
					}
				}
				return
			}

			buf := append(partial[frame.Stream], frame.Data...)
			for {
				index := indexByte(buf, '\n')
				if index < 0 {
					break
				}
				text := strings.TrimRight(string(buf[:index]), "\r")
				if !emit(frame.Stream, text) {
					return
				}
				buf = buf[index+1:]
			}
			partial[frame.Stream] = buf
		}
	}()
	return out, nil
}

func streamName(s Stream) string {
	if s == StreamStderr {
		return driver.StreamStderr
	}
	return driver.StreamStdout
}

func indexByte(b []byte, c byte) int {
	for i, v := range b {
		if v == c {
			return i
		}
	}
	return -1
}

// Wait blocks until the container exits.
func (d *Driver) Wait(ctx context.Context, env driver.Env) (driver.Status, error) {
	if _, err := d.client.ContainerWait(ctx, env.ID); err != nil {
		if IsNotFound(err) {
			return driver.Status{Phase: driver.PhaseGone}, nil
		}
		return driver.Status{}, fmt.Errorf("docker driver: wait: %w", err)
	}
	// Inspect rather than trusting the wait result alone: only inspect reports
	// OOMKilled, and "the agent failed" versus "we gave it too little memory"
	// are different bugs with different owners.
	return d.Describe(ctx, env)
}

// Describe reports a container's current state.
func (d *Driver) Describe(ctx context.Context, env driver.Env) (driver.Status, error) {
	state, err := d.client.ContainerInspect(ctx, env.ID)
	if err != nil {
		if IsNotFound(err) {
			return driver.Status{Phase: driver.PhaseGone}, nil
		}
		return driver.Status{}, fmt.Errorf("docker driver: inspect: %w", err)
	}

	status := driver.Status{
		ExitCode:  state.ExitCode,
		OOMKilled: state.OOMKilled,
		Reason:    state.Error,
		StartedAt: parseDockerTime(state.StartedAt),
		ExitedAt:  parseDockerTime(state.FinishedAt),
	}

	switch state.Status {
	case "created":
		status.Phase = driver.PhaseReady
	case "running", "paused", "restarting":
		status.Phase = driver.PhaseRunning
	case "exited", "dead":
		status.Phase = driver.PhaseExited
	case "removing":
		status.Phase = driver.PhaseGone
	default:
		status.Phase = driver.PhaseRunning
	}

	// Docker reports a SIGKILL as exit 137 (128 + 9). When the environment had
	// a deadline that has passed, that is overwhelmingly our own deadline
	// enforcement rather than a coincidence, and reporting it as a crash would
	// send an operator looking for an agent bug that does not exist.
	if status.Phase == driver.PhaseExited && state.ExitCode == 137 &&
		!env.ExpiresAt.IsZero() && d.now().After(env.ExpiresAt) {
		status.DeadlineExceeded = true
		status.Reason = "container killed after exceeding its deadline"
	}
	return status, nil
}

// List returns every container this driver owns, including those created by a
// previous control-plane process.
func (d *Driver) List(ctx context.Context) ([]driver.Env, error) {
	summaries, err := d.client.ContainerList(ctx, map[string]string{LabelManaged: "true"})
	if err != nil {
		return nil, fmt.Errorf("docker driver: list containers: %w", err)
	}

	envs := make([]driver.Env, 0, len(summaries))
	for _, summary := range summaries {
		created := time.Unix(summary.Created, 0)
		if label := summary.Labels[LabelCreated]; label != "" {
			if parsed, err := time.Parse(time.RFC3339, label); err == nil {
				created = parsed
			}
		}
		var expires time.Time
		if label := summary.Labels[LabelExpires]; label != "" {
			expires, _ = time.Parse(time.RFC3339, label)
		}

		envs = append(envs, driver.Env{
			ID:        summary.ID,
			Driver:    d.Name(),
			JobID:     summary.Labels[LabelJob],
			TenantID:  summary.Labels[LabelTenant],
			CreatedAt: created,
			ExpiresAt: expires,
		})
	}
	sort.Slice(envs, func(i, j int) bool { return envs[i].CreatedAt.Before(envs[j].CreatedAt) })
	return envs, nil
}

// Collect copies the environment's artifacts into dest.
//
// Artifacts are read from the host directory bound at artifactMount, not out of
// the container. Copying from the container only works while it is running: the
// mount is gone once it exits, and the interesting case - an agent that crashed
// - is precisely the one where the container is no longer running.
func (d *Driver) Collect(ctx context.Context, env driver.Env, dest string) ([]driver.Artifact, error) {
	src, err := d.artifactDirFor(ctx, env)
	if err != nil {
		return nil, err
	}
	if src == "" {
		return nil, nil // Environment is gone; nothing to collect is not a failure.
	}
	if _, err := os.Stat(src); err != nil {
		if os.IsNotExist(err) {
			return nil, nil // Nothing written is not a failure.
		}
		return nil, fmt.Errorf("docker driver: stat artifact dir: %w", err)
	}

	if err := os.MkdirAll(dest, 0o750); err != nil {
		return nil, fmt.Errorf("docker driver: create artifact destination: %w", err)
	}
	return archive.CollectDir(ctx, src, dest)
}

// artifactDirFor resolves the host artifact directory for an environment,
// preferring the label on the container so that an environment created by a
// previous control-plane process still resolves. Returns "" if the container is
// gone and no directory can be inferred.
func (d *Driver) artifactDirFor(ctx context.Context, env driver.Env) (string, error) {
	labels, err := d.client.ContainerLabels(ctx, env.ID)
	if err != nil {
		if IsNotFound(err) {
			return "", nil
		}
		return "", fmt.Errorf("docker driver: inspect for artifact dir: %w", err)
	}
	return labels[LabelArtifactDir], nil
}

// Destroy removes the container. Idempotent, and removing something already
// gone is success — the reaper calls this speculatively.
func (d *Driver) Destroy(ctx context.Context, env driver.Env) error {
	// Resolve the directory before removing the container: the label is the
	// only record of it, and it disappears with the container.
	artifactDir, dirErr := d.artifactDirFor(ctx, env)

	if err := d.client.ContainerRemove(ctx, env.ID, true); err != nil && !IsNotFound(err) {
		return fmt.Errorf("docker driver: remove container: %w", err)
	}

	// A container removed without its artifact directory is a disk leak, which
	// is the same class of bug as a leaked container - just slower to notice.
	if dirErr == nil && artifactDir != "" {
		if err := os.RemoveAll(artifactDir); err != nil {
			return fmt.Errorf("docker driver: remove artifact dir: %w", err)
		}
	}
	return nil
}

// ExtractTar unpacks a Docker archive stream into dest.
//
// The implementation lives in the archive package, shared with every other
// driver: a tar stream out of a container is untrusted input, and that defence
// should exist once rather than once per driver. Kept here as the name callers
// and tests already use.
//
// Docker prefixes entries with the copied directory's name, so that prefix is
// stripped to leave names relative to the artifact directory itself.
func ExtractTar(ctx context.Context, r io.Reader, dest string) ([]driver.Artifact, error) {
	return archive.Extract(ctx, r, dest, path.Base(artifactMount)+"/")
}

// memoryBytes resolves the memory limit, always returning one.
func memoryBytes(requestedMiB, defaultMiB int64) int64 {
	mib := requestedMiB
	if mib <= 0 {
		mib = defaultMiB
	}
	if mib <= 0 {
		return 0
	}
	return mib * 1024 * 1024
}

// nanoCPUs resolves the CPU limit. 1000 millicores equals one core.
func nanoCPUs(requestedMillis, defaultMillis int64) int64 {
	millis := requestedMillis
	if millis <= 0 {
		millis = defaultMillis
	}
	if millis <= 0 {
		return 0
	}
	return millis * 1_000_000
}

// sanitiseName strips anything Docker will not accept in a container name.
//
// Docker requires [a-zA-Z0-9][a-zA-Z0-9_.-]*: the first character must be
// alphanumeric, so a name that survives filtering as "_foo" or ".bar" would be
// rejected by the daemon. Dropping leading punctuation here turns that into a
// non-event rather than a create failure on an oddly-named job.
//
// Note this produces a container *name*, never a path, so sequences like ".."
// are harmless here - the traversal defences that matter are in safeJoin.
func sanitiseName(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-', r == '.':
			out = append(out, r)
		}
	}
	for len(out) > 0 && !isAlphanumeric(out[0]) {
		out = out[1:]
	}
	if len(out) == 0 {
		return "job"
	}
	return string(out)
}

func isAlphanumeric(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}

var (
	_ driver.Driver = (*Driver)(nil)
	_               = job.Secret("")
)
