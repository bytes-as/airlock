// Package artifact stores what a job produced and hands out time-limited links
// to it.
//
// The local implementation deliberately copies S3's presigned-URL semantics
// rather than serving files behind a session cookie. That means the signing,
// expiry and verification code paths are exercised from day one, so moving to
// S3 is a change of backend rather than a change of design — and the security
// question ("who can read this screenshot, and for how long?") gets answered
// the same way in both.
package artifact

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"ephemera/internal/driver"
)

// Stored describes one artifact held by the store.
type Stored struct {
	JobID       string    `json:"job_id"`
	Name        string    `json:"name"`
	Size        int64     `json:"size"`
	ContentType string    `json:"content_type"`
	StoredAt    time.Time `json:"stored_at"`
}

// Store persists job artifacts and issues links to them.
type Store interface {
	// Put copies collected artifacts into the store.
	Put(ctx context.Context, jobID string, artifacts []driver.Artifact) ([]Stored, error)
	// List returns everything stored for a job.
	List(ctx context.Context, jobID string) ([]Stored, error)
	// Open returns a reader for one artifact.
	Open(ctx context.Context, jobID, name string) (io.ReadCloser, Stored, error)
	// Delete removes everything stored for a job.
	Delete(ctx context.Context, jobID string) error
}

var (
	// ErrNotFound reports that no such artifact exists.
	ErrNotFound = errors.New("artifact: not found")
	// ErrSignatureInvalid reports a bad or tampered signature.
	ErrSignatureInvalid = errors.New("artifact: invalid signature")
	// ErrLinkExpired reports a signature that was valid but has lapsed.
	// Distinct from ErrSignatureInvalid because the remedies differ: ask for a
	// fresh link, versus stop sending forged ones.
	ErrLinkExpired = errors.New("artifact: link expired")
)

// Local stores artifacts on the filesystem under Root.
type Local struct {
	root       string
	signingKey []byte
	now        func() time.Time
	// baseURL prefixes issued links, e.g. "http://localhost:8080".
	baseURL string
}

// Option configures a Local store.
type Option func(*Local)

// WithClock overrides the time source so expiry can be tested without waiting.
func WithClock(now func() time.Time) Option {
	return func(l *Local) { l.now = now }
}

// WithBaseURL sets the origin used when building links.
func WithBaseURL(base string) Option {
	return func(l *Local) { l.baseURL = strings.TrimRight(base, "/") }
}

// NewLocal opens a filesystem-backed store.
//
// signingKey secures the presigned links. Pass nil to have one generated,
// which is fine for a single-process run but means links do not survive a
// restart — set an explicit key when that matters.
func NewLocal(root string, signingKey []byte, opts ...Option) (*Local, error) {
	if root == "" {
		return nil, errors.New("artifact: root directory is required")
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, fmt.Errorf("artifact: create root: %w", err)
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("artifact: resolve root: %w", err)
	}

	if len(signingKey) == 0 {
		signingKey = make([]byte, 32)
		if _, err := rand.Read(signingKey); err != nil {
			return nil, fmt.Errorf("artifact: generate signing key: %w", err)
		}
	}

	l := &Local{root: abs, signingKey: signingKey, now: time.Now}
	for _, opt := range opts {
		opt(l)
	}
	return l, nil
}

// jobDir returns the directory for a job, refusing IDs that would escape root.
func (l *Local) jobDir(jobID string) (string, error) {
	if jobID == "" || strings.ContainsAny(jobID, `/\`) || strings.Contains(jobID, "..") {
		return "", fmt.Errorf("artifact: invalid job ID %q", jobID)
	}
	return filepath.Join(l.root, jobID), nil
}

// resolve maps a job ID and artifact name to a path inside the store,
// rejecting anything that would escape it.
//
// Artifact names come from the agent's own filesystem, which is to say from
// untrusted code. A name of "../../../etc/passwd" must not become a write
// outside the store, nor a read of something that was never a job's output.
func (l *Local) resolve(jobID, name string) (string, error) {
	dir, err := l.jobDir(jobID)
	if err != nil {
		return "", err
	}
	if name == "" {
		return "", fmt.Errorf("artifact: empty name")
	}
	clean := filepath.Clean(filepath.Join(dir, filepath.FromSlash(name)))
	if clean != dir && !strings.HasPrefix(clean, dir+string(os.PathSeparator)) {
		return "", fmt.Errorf("artifact: name %q escapes the job directory", name)
	}
	return clean, nil
}

// Put copies collected artifacts into the store.
func (l *Local) Put(ctx context.Context, jobID string, artifacts []driver.Artifact) ([]Stored, error) {
	if len(artifacts) == 0 {
		return nil, nil
	}
	dir, err := l.jobDir(jobID)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("artifact: create job dir: %w", err)
	}

	now := l.now()
	stored := make([]Stored, 0, len(artifacts))
	for _, a := range artifacts {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		target, err := l.resolve(jobID, a.Name)
		if err != nil {
			return nil, err
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
			return nil, fmt.Errorf("artifact: create nested dir: %w", err)
		}
		size, err := copyFile(a.Path, target)
		if err != nil {
			return nil, fmt.Errorf("artifact: store %s: %w", a.Name, err)
		}
		contentType := a.ContentType
		if contentType == "" {
			contentType = contentTypeOf(a.Name)
		}
		stored = append(stored, Stored{
			JobID:       jobID,
			Name:        a.Name,
			Size:        size,
			ContentType: contentType,
			StoredAt:    now,
		})
	}
	sort.Slice(stored, func(i, j int) bool { return stored[i].Name < stored[j].Name })
	return stored, nil
}

// List returns everything stored for a job.
func (l *Local) List(ctx context.Context, jobID string) ([]Stored, error) {
	dir, err := l.jobDir(jobID)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return nil, nil // A job with no artifacts is not an error.
	}

	var out []Stored
	err = filepath.WalkDir(dir, func(p string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		name := filepath.ToSlash(rel)
		out = append(out, Stored{
			JobID:       jobID,
			Name:        name,
			Size:        info.Size(),
			ContentType: contentTypeOf(name),
			StoredAt:    info.ModTime(),
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("artifact: list: %w", err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Open returns a reader for one artifact.
func (l *Local) Open(ctx context.Context, jobID, name string) (io.ReadCloser, Stored, error) {
	target, err := l.resolve(jobID, name)
	if err != nil {
		return nil, Stored{}, err
	}
	info, err := os.Stat(target)
	if err != nil || info.IsDir() {
		return nil, Stored{}, ErrNotFound
	}
	f, err := os.Open(target)
	if err != nil {
		return nil, Stored{}, ErrNotFound
	}
	return f, Stored{
		JobID:       jobID,
		Name:        name,
		Size:        info.Size(),
		ContentType: contentTypeOf(name),
		StoredAt:    info.ModTime(),
	}, nil
}

// Delete removes everything stored for a job.
func (l *Local) Delete(ctx context.Context, jobID string) error {
	dir, err := l.jobDir(jobID)
	if err != nil {
		return err
	}
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("artifact: delete: %w", err)
	}
	return nil
}

// SignedURL issues a time-limited link to one artifact.
//
// The signature covers the job ID, the artifact name and the expiry together.
// Signing them as one message is what stops an expiry being edited, or a
// signature for one artifact being replayed against another.
func (l *Local) SignedURL(jobID, name string, ttl time.Duration) (string, error) {
	if ttl <= 0 {
		return "", errors.New("artifact: link TTL must be positive")
	}
	if _, err := l.resolve(jobID, name); err != nil {
		return "", err
	}

	expires := l.now().Add(ttl).Unix()
	signature := l.sign(jobID, name, expires)

	query := url.Values{}
	query.Set("expires", strconv.FormatInt(expires, 10))
	query.Set("signature", signature)

	return fmt.Sprintf("%s/v1/jobs/%s/artifacts/%s?%s",
		l.baseURL, url.PathEscape(jobID), escapePath(name), query.Encode()), nil
}

// VerifySignedURL checks a presented signature and expiry.
func (l *Local) VerifySignedURL(jobID, name, signature string, expires int64) error {
	expected := l.sign(jobID, name, expires)

	// Constant-time comparison: a byte-by-byte compare that returns early
	// leaks, through timing, how much of a forged signature was correct.
	if !hmac.Equal([]byte(expected), []byte(signature)) {
		return ErrSignatureInvalid
	}

	// Expiry is checked only after the signature verifies. Reporting "expired"
	// for an unsigned request would confirm to an attacker that their forged
	// signature was otherwise well-formed.
	if l.now().Unix() > expires {
		return ErrLinkExpired
	}
	return nil
}

func (l *Local) sign(jobID, name string, expires int64) string {
	mac := hmac.New(sha256.New, l.signingKey)
	// Length-prefix each field so that ("ab", "c") and ("a", "bc") cannot
	// produce the same signed message.
	for _, field := range []string{jobID, name, strconv.FormatInt(expires, 10)} {
		fmt.Fprintf(mac, "%d:%s", len(field), field)
	}
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// SigningKeyFingerprint returns a short, non-secret identifier for the signing
// key, so an operator can confirm two processes share one without printing it.
func (l *Local) SigningKeyFingerprint() string {
	sum := sha256.Sum256(l.signingKey)
	return hex.EncodeToString(sum[:4])
}

func escapePath(name string) string {
	parts := strings.Split(name, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return path.Join(parts...)
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
	if t := mime.TypeByExtension(path.Ext(name)); t != "" {
		return t
	}
	return "application/octet-stream"
}

var _ Store = (*Local)(nil)
