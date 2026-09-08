package artifact

import (
	"context"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"ephemera/internal/driver"
)

var base = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: base} }

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newStore(t *testing.T) (*Local, *clock) {
	t.Helper()
	c := newClock()
	s, err := NewLocal(t.TempDir(), []byte("test-signing-key-32-bytes-long!!"),
		WithClock(c.Now), WithBaseURL("http://localhost:8080"))
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}
	return s, c
}

// sourceArtifacts writes files on disk and returns driver.Artifact values
// pointing at them, standing in for what a driver's Collect produced.
func sourceArtifacts(t *testing.T, files map[string]string) []driver.Artifact {
	t.Helper()
	dir := t.TempDir()
	var out []driver.Artifact
	for name, content := range files {
		p := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(p, []byte(content), 0o640); err != nil {
			t.Fatalf("write: %v", err)
		}
		out = append(out, driver.Artifact{Name: name, Path: p, Size: int64(len(content))})
	}
	return out
}

func TestPutAndOpenRoundTrip(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	stored, err := s.Put(ctx, "job_1", sourceArtifacts(t, map[string]string{
		"screenshot.png":     "png-bytes",
		"nested/result.json": `{"ok":true}`,
	}))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if len(stored) != 2 {
		t.Fatalf("stored %d, want 2", len(stored))
	}

	reader, meta, err := s.Open(ctx, "job_1", "screenshot.png")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer reader.Close()

	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(content) != "png-bytes" {
		t.Errorf("content = %q", content)
	}
	if !strings.HasPrefix(meta.ContentType, "image/png") {
		t.Errorf("ContentType = %q, want image/png", meta.ContentType)
	}
	if meta.Size != int64(len("png-bytes")) {
		t.Errorf("Size = %d", meta.Size)
	}
}

func TestListReturnsNestedNamesWithForwardSlashes(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	if _, err := s.Put(ctx, "job_1", sourceArtifacts(t, map[string]string{
		"a.txt":             "a",
		"deep/nested/b.txt": "b",
	})); err != nil {
		t.Fatalf("Put: %v", err)
	}

	listed, err := s.List(ctx, "job_1")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(listed) != 2 {
		t.Fatalf("listed %d, want 2: %+v", len(listed), listed)
	}

	names := []string{listed[0].Name, listed[1].Name}
	// Object keys must be OS-independent, so a Windows-produced artifact and a
	// Linux-produced one address identically.
	for _, n := range names {
		if strings.Contains(n, `\`) {
			t.Errorf("name %q contains a backslash", n)
		}
	}
	if names[0] != "a.txt" || names[1] != "deep/nested/b.txt" {
		t.Errorf("names = %v", names)
	}
}

func TestListOfJobWithNoArtifacts(t *testing.T) {
	s, _ := newStore(t)
	listed, err := s.List(context.Background(), "job_nothing")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(listed) != 0 {
		t.Errorf("listed = %+v, want empty", listed)
	}
}

func TestOpenUnknownArtifact(t *testing.T) {
	s, _ := newStore(t)
	if _, _, err := s.Open(context.Background(), "job_1", "absent.png"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Open = %v, want ErrNotFound", err)
	}
}

// TestPathTraversalIsRefused: artifact names come from the agent's filesystem,
// which is to say from untrusted code.
func TestPathTraversalIsRefused(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	hostile := []string{
		"../escaped.txt",
		"../../etc/passwd",
		"nested/../../escaped.txt",
	}
	for _, name := range hostile {
		artifacts := []driver.Artifact{{Name: name, Path: filepath.Join(t.TempDir(), "x")}}
		if _, err := s.Put(ctx, "job_1", artifacts); err == nil {
			t.Errorf("Put accepted hostile name %q", name)
		}
		if _, _, err := s.Open(ctx, "job_1", name); err == nil {
			t.Errorf("Open accepted hostile name %q", name)
		}
	}

	// A hostile job ID must not escape either.
	for _, jobID := range []string{"../elsewhere", "a/b", `a\b`, ".."} {
		if _, err := s.List(ctx, jobID); err == nil {
			t.Errorf("List accepted hostile job ID %q", jobID)
		}
	}
}

func TestSignedURLRoundTrip(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	if _, err := s.Put(ctx, "job_1", sourceArtifacts(t, map[string]string{"shot.png": "x"})); err != nil {
		t.Fatalf("Put: %v", err)
	}

	raw, err := s.SignedURL("job_1", "shot.png", 15*time.Minute)
	if err != nil {
		t.Fatalf("SignedURL: %v", err)
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed.Host != "localhost:8080" {
		t.Errorf("host = %q", parsed.Host)
	}
	if !strings.Contains(parsed.Path, "job_1") || !strings.Contains(parsed.Path, "shot.png") {
		t.Errorf("path = %q", parsed.Path)
	}

	expires, err := strconv.ParseInt(parsed.Query().Get("expires"), 10, 64)
	if err != nil {
		t.Fatalf("expires: %v", err)
	}
	signature := parsed.Query().Get("signature")
	if signature == "" {
		t.Fatal("no signature in URL")
	}

	if err := s.VerifySignedURL("job_1", "shot.png", signature, expires); err != nil {
		t.Errorf("VerifySignedURL on a fresh link = %v", err)
	}
}

func TestSignedURLExpires(t *testing.T) {
	s, c := newStore(t)
	raw, err := s.SignedURL("job_1", "shot.png", time.Minute)
	if err != nil {
		t.Fatalf("SignedURL: %v", err)
	}
	parsed, _ := url.Parse(raw)
	expires, _ := strconv.ParseInt(parsed.Query().Get("expires"), 10, 64)
	signature := parsed.Query().Get("signature")

	c.Advance(61 * time.Second)
	if err := s.VerifySignedURL("job_1", "shot.png", signature, expires); !errors.Is(err, ErrLinkExpired) {
		t.Errorf("Verify after expiry = %v, want ErrLinkExpired", err)
	}
}

// TestSignatureCoversEveryField: signing job, name and expiry as one message is
// what stops an expiry being edited or a signature replayed against a different
// artifact.
func TestSignatureCoversEveryField(t *testing.T) {
	s, _ := newStore(t)

	raw, err := s.SignedURL("job_1", "shot.png", time.Hour)
	if err != nil {
		t.Fatalf("SignedURL: %v", err)
	}
	parsed, _ := url.Parse(raw)
	expires, _ := strconv.ParseInt(parsed.Query().Get("expires"), 10, 64)
	signature := parsed.Query().Get("signature")

	cases := []struct {
		name            string
		jobID, artifact string
		expires         int64
	}{
		{"different job", "job_2", "shot.png", expires},
		{"different artifact", "job_1", "other.png", expires},
		{"extended expiry", "job_1", "shot.png", expires + 86400},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := s.VerifySignedURL(c.jobID, c.artifact, signature, c.expires); !errors.Is(err, ErrSignatureInvalid) {
				t.Errorf("Verify = %v, want ErrSignatureInvalid", err)
			}
		})
	}

	if err := s.VerifySignedURL("job_1", "shot.png", "forged-signature", expires); !errors.Is(err, ErrSignatureInvalid) {
		t.Errorf("Verify with a forged signature = %v, want ErrSignatureInvalid", err)
	}
}

// TestForgedSignatureReportsInvalidNotExpired: reporting "expired" for an
// unsigned request would confirm to an attacker that their forgery was
// otherwise well-formed.
func TestForgedSignatureReportsInvalidNotExpired(t *testing.T) {
	s, c := newStore(t)
	longExpired := c.Now().Add(-time.Hour).Unix()

	err := s.VerifySignedURL("job_1", "shot.png", "forged", longExpired)
	if !errors.Is(err, ErrSignatureInvalid) {
		t.Errorf("err = %v, want ErrSignatureInvalid even though the timestamp is stale", err)
	}
}

// TestFieldLengthPrefixingPreventsCollision: without length prefixes,
// ("ab","c") and ("a","bc") would sign the same message.
func TestFieldLengthPrefixingPreventsCollision(t *testing.T) {
	s, _ := newStore(t)
	expires := base.Add(time.Hour).Unix()

	if s.sign("ab", "c", expires) == s.sign("a", "bc", expires) {
		t.Error("signature collides across a field boundary")
	}
}

func TestDifferentKeysProduceDifferentSignatures(t *testing.T) {
	dir := t.TempDir()
	c := newClock()
	a, err := NewLocal(dir, []byte("key-one-aaaaaaaaaaaaaaaaaaaaaaaa"), WithClock(c.Now))
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}
	b, err := NewLocal(dir, []byte("key-two-bbbbbbbbbbbbbbbbbbbbbbbb"), WithClock(c.Now))
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	expires := base.Add(time.Hour).Unix()
	if a.sign("job_1", "x.png", expires) == b.sign("job_1", "x.png", expires) {
		t.Error("signatures match across different keys")
	}
	if a.SigningKeyFingerprint() == b.SigningKeyFingerprint() {
		t.Error("fingerprints match across different keys")
	}
	// The fingerprint must not be the key.
	if strings.Contains(a.SigningKeyFingerprint(), "key-one") {
		t.Error("fingerprint leaks the key")
	}
}

func TestSignedURLRejectsNonPositiveTTL(t *testing.T) {
	s, _ := newStore(t)
	if _, err := s.SignedURL("job_1", "x.png", 0); err == nil {
		t.Error("accepted a zero TTL")
	}
	if _, err := s.SignedURL("job_1", "x.png", -time.Hour); err == nil {
		t.Error("accepted a negative TTL")
	}
}

func TestGeneratedKeyWhenNoneProvided(t *testing.T) {
	s, err := NewLocal(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}
	if _, err := s.SignedURL("job_1", "x.png", time.Minute); err != nil {
		t.Errorf("SignedURL with a generated key: %v", err)
	}
}

func TestDelete(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	if _, err := s.Put(ctx, "job_1", sourceArtifacts(t, map[string]string{"a.txt": "a"})); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Delete(ctx, "job_1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	listed, err := s.List(ctx, "job_1")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(listed) != 0 {
		t.Errorf("artifacts survived delete: %+v", listed)
	}
	// Deleting again is fine.
	if err := s.Delete(ctx, "job_1"); err != nil {
		t.Errorf("second Delete = %v, want nil", err)
	}
}

func TestPutEmptyIsNoOp(t *testing.T) {
	s, _ := newStore(t)
	stored, err := s.Put(context.Background(), "job_1", nil)
	if err != nil {
		t.Fatalf("Put(nil) = %v", err)
	}
	if len(stored) != 0 {
		t.Errorf("stored = %+v, want empty", stored)
	}
}
