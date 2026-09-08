package secrets

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bytes-as/airlock/internal/job"
)

func TestChainRoutesBySourceName(t *testing.T) {
	ctx := context.Background()
	chain := NewChain(
		&StaticSource{Values: map[string]job.Secret{"token": "static-value"}},
		&EnvSource{Prefix: "AIRLOCK_TEST_SECRET_"},
	)

	t.Setenv("AIRLOCK_TEST_SECRET_API_KEY", "env-value")

	resolved, err := chain.Resolve(ctx, []job.SecretRef{
		{Name: "TOKEN", Source: "static", Key: "token"},
		{Name: "API_KEY", Source: "env", Key: "API_KEY"},
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if resolved["TOKEN"].Reveal() != "static-value" {
		t.Errorf("TOKEN = %q", resolved["TOKEN"].Reveal())
	}
	if resolved["API_KEY"].Reveal() != "env-value" {
		t.Errorf("API_KEY = %q", resolved["API_KEY"].Reveal())
	}
}

// TestChainDoesNotFallBack: a fallback chain would let a secret resolve from
// the wrong store, which is how a development credential reaches a production
// job. An unknown source is an error, not a reason to go looking elsewhere.
func TestChainDoesNotFallBack(t *testing.T) {
	chain := NewChain(&StaticSource{Values: map[string]job.Secret{"token": "value"}})

	_, err := chain.Resolve(context.Background(), []job.SecretRef{
		{Name: "TOKEN", Source: "vault", Key: "token"},
	})
	if err == nil {
		t.Fatal("resolving from an unconfigured source succeeded")
	}
	var resErr *ResolutionError
	if !errors.As(err, &resErr) {
		t.Fatalf("error = %T, want *ResolutionError", err)
	}
	if !strings.Contains(err.Error(), "vault") {
		t.Errorf("error does not name the source: %v", err)
	}
}

// TestMissingSecretFailsLoudly: an agent that starts without its credentials
// fails confusingly downstream. Naming the problem here is far cheaper.
func TestMissingSecretFailsLoudly(t *testing.T) {
	chain := NewChain(&StaticSource{Values: map[string]job.Secret{}})

	resolved, err := chain.Resolve(context.Background(), []job.SecretRef{
		{Name: "TOKEN", Source: "static", Key: "absent"},
	})
	if err == nil {
		t.Fatal("missing secret resolved without error")
	}
	if resolved != nil {
		t.Error("a failed resolution must not return a partial map")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("error = %v, want it to wrap ErrNotFound", err)
	}
}

func TestResolveRejectsReferenceWithoutName(t *testing.T) {
	chain := NewChain(&StaticSource{Values: map[string]job.Secret{"k": "v"}})
	_, err := chain.Resolve(context.Background(), []job.SecretRef{{Source: "static", Key: "k"}})
	if err == nil {
		t.Fatal("reference with no environment variable name was accepted")
	}
}

func TestResolveEmptyIsNotAnError(t *testing.T) {
	chain := NewChain()
	resolved, err := chain.Resolve(context.Background(), nil)
	if err != nil {
		t.Fatalf("Resolve(nil) = %v, want no error", err)
	}
	if len(resolved) != 0 {
		t.Errorf("resolved = %v, want empty", resolved)
	}
}

// TestEnvSourcePrefixContainsTheBlastRadius: without a prefix, a job could name
// any host variable and exfiltrate it.
func TestEnvSourcePrefixContainsTheBlastRadius(t *testing.T) {
	t.Setenv("AWS_SECRET_ACCESS_KEY", "host-credential-do-not-leak")
	t.Setenv("AIRLOCK_SECRET_SAFE", "intended-value")

	source := &EnvSource{Prefix: "AIRLOCK_SECRET_"}

	if _, err := source.Get(context.Background(), "AWS_SECRET_ACCESS_KEY"); !errors.Is(err, ErrNotFound) {
		t.Error("prefix did not prevent reading an arbitrary host variable")
	}
	value, err := source.Get(context.Background(), "SAFE")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if value.Reveal() != "intended-value" {
		t.Errorf("value = %q", value.Reveal())
	}
}

func TestFileSourceReadsAndTrims(t *testing.T) {
	dir := t.TempDir()
	// Trailing newlines are near-universal in secret files and near-never part
	// of the credential.
	if err := os.WriteFile(filepath.Join(dir, "api-token"), []byte("s3cr3t-value\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	source := &FileSource{Root: dir}
	value, err := source.Get(context.Background(), "api-token")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if value.Reveal() != "s3cr3t-value" {
		t.Errorf("value = %q, want the newline trimmed", value.Reveal())
	}
}

// TestFileSourceRefusesPathTraversal: without containment, a secret reference
// becomes an arbitrary host file read.
func TestFileSourceRefusesPathTraversal(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(root, "..", "outside-secret")
	if err := os.WriteFile(outside, []byte("should-be-unreachable"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	t.Cleanup(func() { os.Remove(outside) })

	source := &FileSource{Root: root}
	for _, key := range []string{
		"../outside-secret",
		"../../etc/passwd",
		filepath.Join("nested", "..", "..", "outside-secret"),
	} {
		if _, err := source.Get(context.Background(), key); err == nil {
			t.Errorf("key %q escaped the secrets directory", key)
		}
	}
}

// TestFileSourceErrorsDoNotCarryPaths: paths to secret material are themselves
// worth not logging.
func TestFileSourceErrorsDoNotCarryPaths(t *testing.T) {
	dir := t.TempDir()
	source := &FileSource{Root: dir}

	_, err := source.Get(context.Background(), "absent")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	if strings.Contains(err.Error(), dir) {
		t.Errorf("error leaks the secrets directory path: %v", err)
	}
}

// TestRedactorScrubsAgentOutput is the second line of defence: the platform
// never writes a secret, but the agent might print one, and those lines flow
// through our log pipeline.
func TestRedactorScrubsAgentOutput(t *testing.T) {
	resolved := map[string]job.Secret{
		"API_TOKEN": job.Secret("sk-live-abcdef123456"),
		"DB_PASS":   job.Secret("correct-horse-battery"),
	}
	r := NewRedactor(resolved)

	line := "connecting with token sk-live-abcdef123456 and password correct-horse-battery"
	got := r.Redact(line)

	if strings.Contains(got, "sk-live-abcdef123456") {
		t.Errorf("token survived redaction: %q", got)
	}
	if strings.Contains(got, "correct-horse-battery") {
		t.Errorf("password survived redaction: %q", got)
	}
	if !strings.Contains(got, "[REDACTED]") {
		t.Errorf("no redaction marker in %q", got)
	}
	// The surrounding text must survive, or the log becomes useless.
	if !strings.Contains(got, "connecting with token") {
		t.Errorf("redaction destroyed the log line: %q", got)
	}
}

func TestRedactorIgnoresShortValues(t *testing.T) {
	// A credential short enough to collide with ordinary text is not one this
	// mechanism can usefully protect, and matching it would redact everything.
	r := NewRedactor(map[string]job.Secret{"SHORT": job.Secret("ab")})
	line := "about to abandon the abacus"
	if got := r.Redact(line); got != line {
		t.Errorf("short value caused spurious redaction: %q", got)
	}
}

func TestRedactorHandlesNilAndEmpty(t *testing.T) {
	var nilRedactor *Redactor
	if got := nilRedactor.Redact("unchanged"); got != "unchanged" {
		t.Errorf("nil redactor mangled input: %q", got)
	}
	empty := NewRedactor(nil)
	if got := empty.Redact("unchanged"); got != "unchanged" {
		t.Errorf("empty redactor mangled input: %q", got)
	}
}

// TestResolvedSecretsStillRedactWhenPrinted ties this package back to the
// job.Secret guarantee: even a resolved value cannot be printed by accident.
func TestResolvedSecretsStillRedactWhenPrinted(t *testing.T) {
	chain := NewChain(&StaticSource{Values: map[string]job.Secret{"k": "very-secret-value"}})
	resolved, err := chain.Resolve(context.Background(), []job.SecretRef{
		{Name: "TOKEN", Source: "static", Key: "k"},
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	// The %v path is the one a careless log line takes.
	if printed := fmt.Sprintf("%v", resolved); strings.Contains(printed, "very-secret-value") {
		t.Errorf("resolved secret leaked when the map was formatted: %s", printed)
	}
}
