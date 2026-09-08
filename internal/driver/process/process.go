// Package process implements a Driver that runs each job as an ordinary OS
// process in a private working directory.
//
// This is a real driver, not a stand-in. It is the right choice when the
// payload is code you control and you want zero infrastructure: no daemon, no
// images, no cloud account. It runs on Linux, macOS and Windows.
//
// What it does NOT provide is a security boundary. The agent shares the host
// kernel, the host filesystem outside its working directory, and the host
// network. Capabilities() says so, and Create refuses any spec asking for
// guarantees this driver cannot deliver, rather than running the job without
// them. For untrusted payloads, use the docker or fargate driver.
package process

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"ephemera/internal/driver"
	"ephemera/internal/job"
)

// stateFile is the per-environment record on disk. It exists so List and
// Describe can report on environments this process did not create, which is
// what lets the reaper clean up after a control-plane crash.
//
// It deliberately contains no secrets. Everything here is safe to read, log,
// and hand to an operator.
type stateFile struct {
	EnvID     string       `json:"env_id"`
	JobID     string       `json:"job_id"`
	TenantID  string       `json:"tenant_id"`
	PID       int          `json:"pid"`
	Phase     driver.Phase `json:"phase"`
	ExitCode  int          `json:"exit_code"`
	Deadline  time.Time    `json:"deadline"`
	CreatedAt time.Time    `json:"created_at"`
	StartedAt time.Time    `json:"started_at,omitempty"`
	ExitedAt  time.Time    `json:"exited_at,omitempty"`

	DeadlineExceeded bool   `json:"deadline_exceeded,omitempty"`
	Reason           string `json:"reason,omitempty"`

	Command []string `json:"command"`
}

const (
	stateFileName    = "env.json"
	logFileName      = "logs.jsonl"
	artifactDirName  = "artifacts"
	tailPollInterval = 50 * time.Millisecond
)

// Driver runs jobs as host processes rooted under a working directory.
type Driver struct {
	root string
	now  func() time.Time

	mu      sync.Mutex
	running map[string]*execution
}

// execution is the in-memory handle for a process this instance started.
// Environments started by a previous process have no execution; they are
// observed through their state file and PID instead.
type execution struct {
	cmd  *exec.Cmd
	done chan struct{}

	mu     sync.Mutex
	status driver.Status
}

// Option configures a Driver.
type Option func(*Driver)

// WithClock overrides the time source. Used by tests to drive deadline
// behaviour without sleeping.
func WithClock(now func() time.Time) Option {
	return func(d *Driver) { d.now = now }
}

// New creates a process driver rooted at dir. Every environment gets a
// subdirectory there, and that subdirectory is the unit of cleanup.
func New(dir string, opts ...Option) (*Driver, error) {
	if dir == "" {
		return nil, errors.New("process driver: root directory is required")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("process driver: create root %s: %w", dir, err)
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("process driver: resolve root %s: %w", dir, err)
	}
	d := &Driver{
		root:    abs,
		now:     time.Now,
		running: make(map[string]*execution),
	}
	for _, opt := range opts {
		opt(d)
	}
	return d, nil
}

func (d *Driver) Name() string { return "process" }

// Capabilities reports what this driver genuinely enforces. Resource limits and
// network policy are both false: honouring them portably would require cgroups
// on Linux, Job Objects on Windows and sandbox-exec on macOS, and claiming them
// while implementing one platform would be worse than declining all three.
func (d *Driver) Capabilities() driver.Capabilities {
	return driver.Capabilities{
		Isolation:              driver.IsolationProcess,
		EnforcesResourceLimits: false,
		EnforcesNetworkPolicy:  false,
		// State lives on disk keyed by environment ID, so a fresh control plane
		// can enumerate and reap what an old one left behind.
		SurvivesControlPlaneRestart: true,
	}
}

func (d *Driver) envDir(envID string) string { return filepath.Join(d.root, envID) }

// Create provisions a working directory for the job and records its state.
//
// It refuses specs demanding guarantees this driver cannot enforce. A caller
// who asked for network egress policy and silently got none is in a worse
// position than one whose job was rejected, because they believe the control is
// in place.
func (d *Driver) Create(ctx context.Context, spec driver.EnvSpec) (driver.Env, error) {
	if spec.Network.Mode != "" && spec.Network.Mode != driver.NetworkEgress {
		// NetworkEgress is the ambient behaviour of a host process, so accepting
		// it is truthful. NetworkNone and NetworkProxied would both be lies.
		return driver.Env{}, &driver.UnsupportedError{Driver: d.Name(), Feature: fmt.Sprintf("network mode %q", spec.Network.Mode)}
	}
	if len(spec.Network.DenyCIDRs) > 0 {
		return driver.Env{}, &driver.UnsupportedError{Driver: d.Name(), Feature: "network deny lists"}
	}
	if spec.Resources.CPUMillis > 0 || spec.Resources.MemoryMiB > 0 {
		return driver.Env{}, &driver.UnsupportedError{Driver: d.Name(), Feature: "resource limits"}
	}
	if len(spec.Command) == 0 {
		return driver.Env{}, fmt.Errorf("process driver: spec has no command to run")
	}

	now := d.now()
	envID := "env_" + strings.TrimPrefix(job.NewID(now), "job_")
	dir := d.envDir(envID)

	if err := os.MkdirAll(filepath.Join(dir, artifactDirName), 0o750); err != nil {
		return driver.Env{}, fmt.Errorf("process driver: create env dir: %w", err)
	}

	var expires time.Time
	if spec.Deadline > 0 {
		expires = now.Add(spec.Deadline)
	}

	st := stateFile{
		EnvID:     envID,
		JobID:     spec.JobID,
		TenantID:  spec.TenantID,
		Phase:     driver.PhaseReady,
		Deadline:  expires,
		CreatedAt: now,
		Command:   spec.Command,
	}
	if err := d.writeState(envID, st); err != nil {
		// Best-effort cleanup: an env dir with no state file is invisible to
		// List, and therefore un-reapable. Better to remove it now.
		_ = os.RemoveAll(dir)
		return driver.Env{}, err
	}

	return driver.Env{
		ID:        envID,
		Driver:    d.Name(),
		JobID:     spec.JobID,
		TenantID:  spec.TenantID,
		CreatedAt: now,
		ExpiresAt: expires,
	}, nil
}

// Start launches the agent.
//
// The spec's secret values are used here and dropped when this returns: they
// are injected into the child process environment and written to no file, no
// log and no field of the driver.
func (d *Driver) Start(ctx context.Context, env driver.Env, spec driver.EnvSpec) error {
	if len(spec.Command) == 0 {
		return fmt.Errorf("process driver: spec has no command to run")
	}

	st, err := d.readState(env.ID)
	if err != nil {
		return err
	}
	if st.Phase != driver.PhaseReady {
		return fmt.Errorf("process driver: environment %s is %s, not ready to start", env.ID, st.Phase)
	}

	dir := d.envDir(env.ID)
	logPath := filepath.Join(dir, logFileName)
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return fmt.Errorf("process driver: open log file: %w", err)
	}

	// The process gets its own context, deliberately detached from ctx. ctx
	// governs the *start call*; the agent's lifetime is governed by its deadline
	// and by Destroy. Tying the process to the caller's context would kill jobs
	// whenever an HTTP handler returned.
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))

	cmd := exec.CommandContext(runCtx, spec.Command[0], spec.Command[1:]...)
	cmd.Dir = dir
	cmd.Env = buildEnv(spec, dir)
	configureProcAttr(cmd)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		logFile.Close()
		return fmt.Errorf("process driver: stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		logFile.Close()
		return fmt.Errorf("process driver: stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		cancel()
		logFile.Close()
		st.Phase = driver.PhaseExited
		st.ExitCode = -1
		st.Reason = "failed to start: " + err.Error()
		st.ExitedAt = d.now()
		_ = d.writeState(env.ID, st)
		return fmt.Errorf("process driver: start %s: %w", spec.Command[0], err)
	}

	started := d.now()
	st.Phase = driver.PhaseRunning
	st.PID = cmd.Process.Pid
	st.StartedAt = started
	if err := d.writeState(env.ID, st); err != nil {
		// The process is already running and we cannot record it. Kill it rather
		// than leak an environment nothing can find.
		cancel()
		_ = cmd.Wait()
		logFile.Close()
		return fmt.Errorf("process driver: record running state: %w", err)
	}

	ex := &execution{
		cmd:    cmd,
		done:   make(chan struct{}),
		status: driver.Status{Phase: driver.PhaseRunning, StartedAt: started},
	}
	d.mu.Lock()
	d.running[env.ID] = ex
	d.mu.Unlock()

	// Pump both streams into the log file. A WaitGroup here matters: cmd.Wait
	// closes the pipes, so it must not run until both readers are finished, or
	// the tail of the agent's output is lost exactly when it is most wanted —
	// the last thing a crashing process prints.
	var pumps sync.WaitGroup
	writes := &sync.Mutex{}
	pumps.Add(2)
	go func() { defer pumps.Done(); pumpStream(stdout, driver.StreamStdout, logFile, writes, d.now) }()
	go func() { defer pumps.Done(); pumpStream(stderr, driver.StreamStderr, logFile, writes, d.now) }()

	go d.supervise(env, ex, cancel, logFile, &pumps, spec.Deadline)

	return nil
}

// supervise waits for the process, enforces the deadline, and records the
// outcome. It owns the cleanup of everything Start opened.
func (d *Driver) supervise(env driver.Env, ex *execution, cancel context.CancelFunc, logFile *os.File, pumps *sync.WaitGroup, deadline time.Duration) {
	defer close(ex.done)
	defer cancel()

	var timedOut atomic4Bool
	if deadline > 0 {
		// The innermost reaper layer: the environment ends itself on time even
		// if every other layer of the system has stopped paying attention.
		timer := time.AfterFunc(deadline, func() {
			timedOut.Set(true)
			d.mu.Lock()
			e := d.running[env.ID]
			d.mu.Unlock()
			if e != nil {
				killTree(e.cmd)
			}
		})
		defer timer.Stop()
	}

	waitErr := ex.cmd.Wait()
	pumps.Wait()
	_ = logFile.Close()

	exited := d.now()
	status := driver.Status{
		Phase:            driver.PhaseExited,
		StartedAt:        ex.status.StartedAt,
		ExitedAt:         exited,
		DeadlineExceeded: timedOut.Get(),
	}
	if waitErr != nil {
		var exitErr *exec.ExitError
		if errors.As(waitErr, &exitErr) {
			status.ExitCode = exitErr.ExitCode()
		} else {
			status.ExitCode = -1
			status.Reason = waitErr.Error()
		}
	}
	if status.DeadlineExceeded {
		status.Reason = "killed after exceeding deadline"
	}

	ex.mu.Lock()
	ex.status = status
	ex.mu.Unlock()

	st, err := d.readState(env.ID)
	if err == nil {
		st.Phase = driver.PhaseExited
		st.ExitCode = status.ExitCode
		st.ExitedAt = exited
		st.DeadlineExceeded = status.DeadlineExceeded
		st.Reason = status.Reason
		_ = d.writeState(env.ID, st)
	}

	d.mu.Lock()
	delete(d.running, env.ID)
	d.mu.Unlock()
}

// buildEnv assembles the child process environment. Secrets are merged in here
// and nowhere else, so this function is the entire surface on which a
// credential can reach the agent.
func buildEnv(spec driver.EnvSpec, dir string) []string {
	env := make([]string, 0, len(spec.Env)+len(spec.Secrets)+8)

	// A minimal inherited environment. Passing the operator's full environment
	// through to an agent is how host credentials end up inside a job.
	for _, key := range []string{"PATH", "HOME", "SystemRoot", "TMP", "TEMP", "USERPROFILE", "LANG"} {
		if v, ok := os.LookupEnv(key); ok {
			env = append(env, key+"="+v)
		}
	}

	env = append(env,
		"EPHEMERA_JOB_ID="+spec.JobID,
		"EPHEMERA_TENANT_ID="+spec.TenantID,
		"EPHEMERA_ARTIFACT_DIR="+filepath.Join(dir, artifactDirName),
	)
	for k, v := range spec.Env {
		env = append(env, k+"="+v)
	}
	for k, v := range spec.Secrets {
		env = append(env, k+"="+v.Reveal())
	}
	return env
}

// pumpStream copies one output stream into the log file as JSON lines.
func pumpStream(r io.Reader, stream string, w io.Writer, mu *sync.Mutex, now func() time.Time) {
	scanner := bufio.NewScanner(r)
	// Agents print long lines — a base64 screenshot, a stack trace. The default
	// 64KiB limit would truncate them mid-line and corrupt the JSONL stream.
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := driver.LogLine{At: now(), Stream: stream, Text: scanner.Text()}
		encoded, err := json.Marshal(line)
		if err != nil {
			continue
		}
		mu.Lock()
		_, _ = w.Write(append(encoded, '\n'))
		mu.Unlock()
	}
}

// Logs tails the environment's log file. It works for a running environment and
// for one that has already exited, and it terminates when the environment does.
func (d *Driver) Logs(ctx context.Context, env driver.Env) (<-chan driver.LogLine, error) {
	path := filepath.Join(d.envDir(env.ID), logFileName)

	// The file appears when Start runs. Wait briefly for it rather than failing
	// a viewer who connected a moment early.
	f, err := openWhenReady(ctx, path, 2*time.Second)
	if err != nil {
		return nil, err
	}

	out := make(chan driver.LogLine, 64)
	go func() {
		defer close(out)
		defer f.Close()

		reader := bufio.NewReaderSize(f, 64*1024)
		var pending []byte
		for {
			if ctx.Err() != nil {
				return
			}
			chunk, err := reader.ReadBytes('\n')
			if len(chunk) > 0 {
				pending = append(pending, chunk...)
			}
			if err == nil && len(pending) > 0 {
				var line driver.LogLine
				if json.Unmarshal(pending[:len(pending)-1], &line) == nil {
					select {
					case out <- line:
					case <-ctx.Done():
						return
					}
				}
				pending = nil
				continue
			}
			if err != nil && !errors.Is(err, io.EOF) {
				return
			}
			// At EOF: stop only once the environment is terminal, so we do not
			// truncate the stream during a quiet moment in a running job.
			if st, serr := d.readState(env.ID); serr != nil || st.Phase.Terminal() {
				// One more pass to drain anything written between the read and
				// the state check, then finish.
				if rest, _ := io.ReadAll(reader); len(rest) > 0 {
					emitLines(ctx, append(pending, rest...), out)
				}
				return
			}
			select {
			case <-time.After(tailPollInterval):
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

func emitLines(ctx context.Context, buf []byte, out chan<- driver.LogLine) {
	for _, raw := range strings.Split(string(buf), "\n") {
		if strings.TrimSpace(raw) == "" {
			continue
		}
		var line driver.LogLine
		if json.Unmarshal([]byte(raw), &line) != nil {
			continue
		}
		select {
		case out <- line:
		case <-ctx.Done():
			return
		}
	}
}

func openWhenReady(ctx context.Context, path string, wait time.Duration) (*os.File, error) {
	deadline := time.Now().Add(wait)
	for {
		f, err := os.Open(path)
		if err == nil {
			return f, nil
		}
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("process driver: open logs: %w", err)
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("process driver: logs for environment not available: %w", driver.ErrNotFound)
		}
		select {
		case <-time.After(tailPollInterval):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Wait blocks until the agent exits. For an environment this process started it
// waits on the process directly; otherwise it polls the state file, which is how
// a restarted control plane can still await a job it inherited.
func (d *Driver) Wait(ctx context.Context, env driver.Env) (driver.Status, error) {
	d.mu.Lock()
	ex := d.running[env.ID]
	d.mu.Unlock()

	if ex != nil {
		select {
		case <-ex.done:
			ex.mu.Lock()
			defer ex.mu.Unlock()
			return ex.status, nil
		case <-ctx.Done():
			return driver.Status{}, ctx.Err()
		}
	}

	for {
		status, err := d.Describe(ctx, env)
		if err != nil {
			return driver.Status{}, err
		}
		if status.Phase.Terminal() {
			return status, nil
		}
		select {
		case <-time.After(tailPollInterval):
		case <-ctx.Done():
			return driver.Status{}, ctx.Err()
		}
	}
}

// Describe reports the current state of an environment, including one created
// by a previous control-plane process.
func (d *Driver) Describe(ctx context.Context, env driver.Env) (driver.Status, error) {
	st, err := d.readState(env.ID)
	if errors.Is(err, driver.ErrNotFound) {
		// Gone is a state, not an error. An environment that no longer exists is
		// one that costs nothing, which is the outcome the reaper wants.
		return driver.Status{Phase: driver.PhaseGone}, nil
	}
	if err != nil {
		return driver.Status{}, err
	}

	status := driver.Status{
		Phase:            st.Phase,
		ExitCode:         st.ExitCode,
		DeadlineExceeded: st.DeadlineExceeded,
		StartedAt:        st.StartedAt,
		ExitedAt:         st.ExitedAt,
		Reason:           st.Reason,
	}

	// A state file claiming "running" is only believable if the process is
	// actually there. After a host reboot or a SIGKILL the file outlives the
	// process, and trusting it would leave the job hanging forever.
	if st.Phase == driver.PhaseRunning && st.PID > 0 && !processAlive(st.PID) {
		status.Phase = driver.PhaseExited
		status.ExitCode = -1
		status.Reason = "process disappeared without recording an exit status"
		st.Phase = driver.PhaseExited
		st.ExitCode = -1
		st.Reason = status.Reason
		st.ExitedAt = d.now()
		_ = d.writeState(env.ID, st)
	}
	return status, nil
}

// List enumerates every environment on disk, including orphans from a previous
// process. Derived from the filesystem rather than memory, which is the whole
// point: in-memory bookkeeping cannot survive the crash it needs to recover from.
func (d *Driver) List(ctx context.Context) ([]driver.Env, error) {
	entries, err := os.ReadDir(d.root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("process driver: list environments: %w", err)
	}

	var envs []driver.Env
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		st, err := d.readState(entry.Name())
		if err != nil {
			// A directory without a readable state file is debris, not an
			// environment. Skip it rather than fail the whole sweep — one
			// corrupt entry must not blind the reaper to every other one.
			continue
		}
		envs = append(envs, driver.Env{
			ID:        st.EnvID,
			Driver:    d.Name(),
			JobID:     st.JobID,
			TenantID:  st.TenantID,
			CreatedAt: st.CreatedAt,
			ExpiresAt: st.Deadline,
		})
	}
	sort.Slice(envs, func(i, j int) bool { return envs[i].ID < envs[j].ID })
	return envs, nil
}

// Collect copies the agent's artifacts out of the environment into dest.
func (d *Driver) Collect(ctx context.Context, env driver.Env, dest string) ([]driver.Artifact, error) {
	src := filepath.Join(d.envDir(env.ID), artifactDirName)
	if _, err := os.Stat(src); err != nil {
		if os.IsNotExist(err) {
			return nil, nil // A job that produced nothing is not a failure.
		}
		return nil, fmt.Errorf("process driver: stat artifact dir: %w", err)
	}
	if err := os.MkdirAll(dest, 0o750); err != nil {
		return nil, fmt.Errorf("process driver: create artifact destination: %w", err)
	}

	var artifacts []driver.Artifact
	err := filepath.WalkDir(src, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dest, rel)
		if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
			return err
		}
		size, err := copyFile(path, target)
		if err != nil {
			return err
		}
		artifacts = append(artifacts, driver.Artifact{
			Name:        filepath.ToSlash(rel),
			Path:        target,
			Size:        size,
			ContentType: contentTypeOf(rel),
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("process driver: collect artifacts: %w", err)
	}
	sort.Slice(artifacts, func(i, j int) bool { return artifacts[i].Name < artifacts[j].Name })
	return artifacts, nil
}

// Destroy kills the process if it is still running and removes the environment
// directory. It is idempotent, and destroying something already gone is success.
func (d *Driver) Destroy(ctx context.Context, env driver.Env) error {
	d.mu.Lock()
	ex := d.running[env.ID]
	d.mu.Unlock()

	if ex != nil {
		killTree(ex.cmd)
		select {
		case <-ex.done:
		case <-time.After(5 * time.Second):
			// The supervisor did not finish. Proceed anyway: leaving the
			// directory behind is a worse outcome than a racing goroutine.
		case <-ctx.Done():
			return ctx.Err()
		}
	} else if st, err := d.readState(env.ID); err == nil && st.PID > 0 && st.Phase == driver.PhaseRunning {
		// An orphan from a previous control-plane process. This is the case the
		// reaper exists for.
		killPID(st.PID)
	}

	if err := os.RemoveAll(d.envDir(env.ID)); err != nil {
		return fmt.Errorf("process driver: remove env dir: %w", err)
	}
	return nil
}

func (d *Driver) writeState(envID string, st stateFile) error {
	dir := d.envDir(envID)
	encoded, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("process driver: encode state: %w", err)
	}
	// Write-then-rename: a crash mid-write must not leave a truncated state
	// file, because an unreadable state file makes the environment un-reapable.
	tmp := filepath.Join(dir, stateFileName+".tmp")
	if err := os.WriteFile(tmp, encoded, 0o640); err != nil {
		return fmt.Errorf("process driver: write state: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, stateFileName)); err != nil {
		return fmt.Errorf("process driver: commit state: %w", err)
	}
	return nil
}

func (d *Driver) readState(envID string) (stateFile, error) {
	raw, err := os.ReadFile(filepath.Join(d.envDir(envID), stateFileName))
	if err != nil {
		if os.IsNotExist(err) {
			return stateFile{}, driver.ErrNotFound
		}
		return stateFile{}, fmt.Errorf("process driver: read state: %w", err)
	}
	var st stateFile
	if err := json.Unmarshal(raw, &st); err != nil {
		return stateFile{}, fmt.Errorf("process driver: decode state for %s: %w", envID, err)
	}
	return st, nil
}

func copyFile(src, dst string) (int64, error) {
	in, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
	if err != nil {
		return 0, err
	}
	defer out.Close()
	return io.Copy(out, in)
}

func contentTypeOf(name string) string {
	if t := mime.TypeByExtension(filepath.Ext(name)); t != "" {
		return t
	}
	return "application/octet-stream"
}

// atomic4Bool is a tiny mutex-guarded bool. sync/atomic would do, but the
// deadline path is not hot and this keeps the intent obvious.
type atomic4Bool struct {
	mu sync.Mutex
	v  bool
}

func (b *atomic4Bool) Set(v bool) { b.mu.Lock(); b.v = v; b.mu.Unlock() }
func (b *atomic4Bool) Get() bool  { b.mu.Lock(); defer b.mu.Unlock(); return b.v }

var _ driver.Driver = (*Driver)(nil)
