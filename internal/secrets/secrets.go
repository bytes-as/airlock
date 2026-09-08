// Package secrets resolves the secret references in a job spec into values, at
// the moment of dispatch and no earlier.
//
// The whole design exists to keep one invariant: a secret value lives in
// process memory, briefly, on its way into an environment — and nowhere else.
// Job records store references. The queue stores references. Logs and API
// responses show references. Only the worker about to start a job ever holds
// the value, and it holds it for the length of one Start call.
//
// This is what makes "inject credentials without them being logged" a property
// of the architecture rather than a discipline everyone has to remember.
package secrets

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bytes-as/airlock/internal/job"
)

// Resolver turns secret references into values.
type Resolver interface {
	// Resolve returns a map of environment variable name to value. It must
	// fail rather than silently omit a secret the job asked for: an agent
	// starting without its credentials produces a confusing downstream failure,
	// while a clear resolution error names the problem immediately.
	Resolve(ctx context.Context, refs []job.SecretRef) (map[string]job.Secret, error)
}

// ErrNotFound reports that a referenced secret does not exist.
var ErrNotFound = errors.New("secrets: not found")

// ResolutionError names which reference failed, without revealing anything
// about the value.
type ResolutionError struct {
	Ref job.SecretRef
	Err error
}

func (e *ResolutionError) Error() string {
	return fmt.Sprintf("secrets: resolve %s from %s: %v", e.Ref.Key, e.Ref.Source, e.Err)
}

func (e *ResolutionError) Unwrap() error { return e.Err }

// Source resolves references for one backing store.
type Source interface {
	// Name is the value matched against SecretRef.Source.
	Name() string
	// Get returns the value for a key.
	Get(ctx context.Context, key string) (job.Secret, error)
}

// Chain routes each reference to the source named in it.
//
// Routing by name rather than trying sources in order is deliberate: a
// fallback chain would let a secret silently resolve from the wrong store,
// which is how a development credential ends up in a production job.
type Chain struct {
	sources map[string]Source
}

// NewChain builds a resolver over the given sources.
func NewChain(sources ...Source) *Chain {
	c := &Chain{sources: make(map[string]Source, len(sources))}
	for _, s := range sources {
		c.sources[s.Name()] = s
	}
	return c
}

func (c *Chain) Resolve(ctx context.Context, refs []job.SecretRef) (map[string]job.Secret, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	out := make(map[string]job.Secret, len(refs))
	for _, ref := range refs {
		if ref.Name == "" {
			return nil, &ResolutionError{Ref: ref, Err: errors.New("reference has no environment variable name")}
		}
		source, ok := c.sources[ref.Source]
		if !ok {
			return nil, &ResolutionError{Ref: ref, Err: fmt.Errorf("no source named %q is configured", ref.Source)}
		}
		value, err := source.Get(ctx, ref.Key)
		if err != nil {
			return nil, &ResolutionError{Ref: ref, Err: err}
		}
		out[ref.Name] = value
	}
	return out, nil
}

// EnvSource reads secrets from the control plane's own environment.
//
// Suitable for local runs and for container platforms that inject secrets as
// environment variables. The prefix guards against a job naming an arbitrary
// host variable and exfiltrating it: only variables the operator deliberately
// namespaced for this purpose are reachable.
type EnvSource struct {
	// Prefix is prepended to the key before lookup. Empty means no prefix,
	// which exposes the entire host environment and should be used only in
	// development.
	Prefix string
}

func (s *EnvSource) Name() string { return "env" }

func (s *EnvSource) Get(_ context.Context, key string) (job.Secret, error) {
	if key == "" {
		return "", errors.New("empty key")
	}
	value, ok := os.LookupEnv(s.Prefix + key)
	if !ok {
		return "", ErrNotFound
	}
	return job.Secret(value), nil
}

// FileSource reads secrets from files under a root directory.
//
// This is the shape Docker secrets, Kubernetes secret volumes and ECS
// sidecar-injected credentials all take, so a file-per-secret source is the one
// that ports furthest without changing the job spec.
type FileSource struct {
	Root string
}

func (s *FileSource) Name() string { return "file" }

func (s *FileSource) Get(_ context.Context, key string) (job.Secret, error) {
	if key == "" {
		return "", errors.New("empty key")
	}

	// Contain the lookup within Root. Without this, a key of "../../.ssh/id_rsa"
	// turns a secret reference into arbitrary host file read.
	clean := filepath.Clean(filepath.Join(s.Root, key))
	root := filepath.Clean(s.Root)
	if clean != root && !strings.HasPrefix(clean, root+string(os.PathSeparator)) {
		return "", fmt.Errorf("key %q escapes the secrets directory", key)
	}

	raw, err := os.ReadFile(clean)
	if err != nil {
		if os.IsNotExist(err) {
			return "", ErrNotFound
		}
		// Deliberately not wrapping err: an OS error can carry the full path,
		// and paths to secret material are themselves worth not logging.
		return "", errors.New("unreadable")
	}
	// Trailing newlines are near-universal in secret files and near-never part
	// of the credential.
	return job.Secret(strings.TrimRight(string(raw), "\r\n")), nil
}

// StaticSource holds secrets in memory. Intended for tests and for local runs
// where the operator passes credentials on the command line.
type StaticSource struct {
	Values map[string]job.Secret
}

func (s *StaticSource) Name() string { return "static" }

func (s *StaticSource) Get(_ context.Context, key string) (job.Secret, error) {
	value, ok := s.Values[key]
	if !ok {
		return "", ErrNotFound
	}
	return value, nil
}

// Redactor scrubs known secret values from text on its way to a log or an API
// response.
//
// This is the second line of defence, not the first. The first is that the
// platform never writes a secret anywhere — but the *agent* may print one, by
// accident or by echoing its own environment, and those lines flow through our
// log pipeline. Scrubbing them on the way out is what keeps an agent's mistake
// from becoming our data leak.
type Redactor struct {
	values []string
}

// NewRedactor builds a redactor for a set of resolved secrets.
func NewRedactor(resolved map[string]job.Secret) *Redactor {
	r := &Redactor{}
	for _, v := range resolved {
		value := v.Reveal()
		// Very short values would match constantly and turn every log line into
		// redaction noise. A credential short enough to collide with ordinary
		// text is not one this mechanism can usefully protect.
		if len(value) >= 6 {
			r.values = append(r.values, value)
		}
	}
	return r
}

// Redact replaces any known secret value found in s.
func (r *Redactor) Redact(s string) string {
	if r == nil || len(r.values) == 0 {
		return s
	}
	for _, value := range r.values {
		if strings.Contains(s, value) {
			s = strings.ReplaceAll(s, value, "[REDACTED]")
		}
	}
	return s
}

var (
	_ Resolver = (*Chain)(nil)
	_ Source   = (*EnvSource)(nil)
	_ Source   = (*FileSource)(nil)
	_ Source   = (*StaticSource)(nil)
)
