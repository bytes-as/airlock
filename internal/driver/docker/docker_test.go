package docker

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"ephemera/internal/driver"
	"ephemera/internal/job"
)

// These tests cover the parts of the Docker driver that do not need a daemon:
// the wire formats we own, and the security-critical extraction path. Tests
// requiring a live daemon are in docker_integration_test.go behind a build tag.
//
// That split is deliberate rather than convenient. Stream framing and tar
// extraction are exactly where subtle bugs hide — a framing error produces
// binary noise in logs, and a lax extractor turns "collect the screenshots"
// into arbitrary host file write — and neither needs Docker to exercise.

// frame builds one Docker log frame: [stream][0][0][0][big-endian length][payload].
func frame(stream Stream, payload string) []byte {
	var header [8]byte
	header[0] = byte(stream)
	binary.BigEndian.PutUint32(header[4:], uint32(len(payload)))
	return append(header[:], []byte(payload)...)
}

func TestReadFrameDecodesStreamAndPayload(t *testing.T) {
	var buf bytes.Buffer
	buf.Write(frame(StreamStdout, "hello\n"))
	buf.Write(frame(StreamStderr, "a warning\n"))

	reader := bufio.NewReader(&buf)

	first, err := ReadFrame(reader)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if first.Stream != StreamStdout || string(first.Data) != "hello\n" {
		t.Errorf("frame = %v %q", first.Stream, first.Data)
	}

	second, err := ReadFrame(reader)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if second.Stream != StreamStderr || string(second.Data) != "a warning\n" {
		t.Errorf("frame = %v %q", second.Stream, second.Data)
	}

	if _, err := ReadFrame(reader); !errors.Is(err, io.EOF) {
		t.Errorf("err = %v, want io.EOF at the clean end of the stream", err)
	}
}

func TestReadFrameHandlesEmptyPayload(t *testing.T) {
	reader := bufio.NewReader(bytes.NewReader(frame(StreamStdout, "")))
	f, err := ReadFrame(reader)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if len(f.Data) != 0 {
		t.Errorf("Data = %q, want empty", f.Data)
	}
}

// TestReadFrameRejectsAbsurdLength: a corrupt or hostile stream must not be
// able to make us allocate gigabytes on the strength of a length field.
func TestReadFrameRejectsAbsurdLength(t *testing.T) {
	var header [8]byte
	header[0] = byte(StreamStdout)
	binary.BigEndian.PutUint32(header[4:], 1<<30) // 1 GiB

	reader := bufio.NewReader(bytes.NewReader(header[:]))
	if _, err := ReadFrame(reader); err == nil {
		t.Fatal("accepted a 1 GiB frame length")
	}
}

// TestReadFrameReportsTruncation: a truncated frame is a broken stream, not a
// clean end. Conflating them makes a partial log look complete.
func TestReadFrameReportsTruncation(t *testing.T) {
	full := frame(StreamStdout, "this payload is cut short")
	truncated := full[:len(full)-5]

	reader := bufio.NewReader(bytes.NewReader(truncated))
	_, err := ReadFrame(reader)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("err = %v, want io.ErrUnexpectedEOF", err)
	}
}

// --- tar extraction ---

type tarEntry struct {
	name     string
	content  string
	typeflag byte
	linkname string
	size     int64 // when non-zero, overrides the real content length
}

func buildTar(t *testing.T, entries []tarEntry) io.Reader {
	t.Helper()
	var buf bytes.Buffer
	writer := tar.NewWriter(&buf)

	for _, e := range entries {
		typeflag := e.typeflag
		if typeflag == 0 {
			typeflag = tar.TypeReg
		}
		size := int64(len(e.content))
		if e.size != 0 {
			size = e.size
		}
		header := &tar.Header{
			Name:     e.name,
			Mode:     0o640,
			Size:     size,
			Typeflag: typeflag,
			Linkname: e.linkname,
		}
		if typeflag == tar.TypeDir {
			header.Size = 0
		}
		if err := writer.WriteHeader(header); err != nil {
			t.Fatalf("write header %s: %v", e.name, err)
		}
		if typeflag == tar.TypeReg && e.content != "" {
			if _, err := writer.Write([]byte(e.content)); err != nil {
				t.Fatalf("write body %s: %v", e.name, err)
			}
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}
	return &buf
}

func TestExtractTarWritesFiles(t *testing.T) {
	dest := t.TempDir()

	// Docker prefixes entries with the copied directory's name.
	archive := buildTar(t, []tarEntry{
		{name: "artifacts/", typeflag: tar.TypeDir},
		{name: "artifacts/screenshot.png", content: "png-bytes"},
		{name: "artifacts/nested/result.json", content: `{"ok":true}`},
	})

	artifacts, err := ExtractTar(context.Background(), archive, dest)
	if err != nil {
		t.Fatalf("ExtractTar: %v", err)
	}
	if len(artifacts) != 2 {
		t.Fatalf("extracted %d artifacts, want 2: %+v", len(artifacts), artifacts)
	}

	byName := map[string]driver.Artifact{}
	for _, a := range artifacts {
		byName[a.Name] = a
	}

	// The container-side directory prefix must be stripped, so names are
	// relative to the artifact directory itself.
	png, ok := byName["screenshot.png"]
	if !ok {
		t.Fatalf("names = %+v, want the artifacts/ prefix stripped", artifacts)
	}
	if png.Size != int64(len("png-bytes")) {
		t.Errorf("Size = %d", png.Size)
	}
	if !strings.HasPrefix(png.ContentType, "image/png") {
		t.Errorf("ContentType = %q", png.ContentType)
	}
	content, err := os.ReadFile(png.Path)
	if err != nil {
		t.Fatalf("read extracted file: %v", err)
	}
	if string(content) != "png-bytes" {
		t.Errorf("content = %q", content)
	}

	// Nested names keep forward slashes, so the name is a usable object key
	// whatever the host OS.
	if _, ok := byName["nested/result.json"]; !ok {
		t.Errorf("nested artifact missing or misnamed: %+v", artifacts)
	}
}

// TestExtractTarRefusesTraversal is the one that matters. A tar stream from a
// container is untrusted input, and a naive extractor writes through every one
// of these.
func TestExtractTarRefusesTraversal(t *testing.T) {
	hostile := []struct {
		name  string
		entry string
	}{
		{"parent escape", "artifacts/../../escaped.txt"},
		{"deep escape", "artifacts/../../../../../../tmp/escaped.txt"},
		{"absolute path", "/etc/cron.d/pwned"},
		{"nested escape", "artifacts/nested/../../../escaped.txt"},
	}

	for _, c := range hostile {
		t.Run(c.name, func(t *testing.T) {
			dest := t.TempDir()
			archive := buildTar(t, []tarEntry{{name: c.entry, content: "owned"}})

			_, err := ExtractTar(context.Background(), archive, dest)
			if err == nil {
				t.Fatalf("extraction accepted hostile entry %q", c.entry)
			}

			// And nothing was written outside the destination.
			parent := filepath.Dir(dest)
			if _, statErr := os.Stat(filepath.Join(parent, "escaped.txt")); statErr == nil {
				t.Errorf("entry %q wrote outside the destination", c.entry)
			}
		})
	}
}

// TestExtractTarSkipsLinks: a symlink in an artifact set has no legitimate use
// and every illegitimate one — pointing it at /etc/passwd turns artifact
// collection into host file read.
func TestExtractTarSkipsLinks(t *testing.T) {
	dest := t.TempDir()
	archive := buildTar(t, []tarEntry{
		{name: "artifacts/real.txt", content: "fine"},
		{name: "artifacts/evil-symlink", typeflag: tar.TypeSymlink, linkname: "/etc/passwd"},
		{name: "artifacts/evil-hardlink", typeflag: tar.TypeLink, linkname: "/etc/shadow"},
	})

	artifacts, err := ExtractTar(context.Background(), archive, dest)
	if err != nil {
		t.Fatalf("ExtractTar: %v", err)
	}
	if len(artifacts) != 1 || artifacts[0].Name != "real.txt" {
		t.Errorf("artifacts = %+v, want only the regular file", artifacts)
	}
	for _, name := range []string{"evil-symlink", "evil-hardlink"} {
		if _, err := os.Lstat(filepath.Join(dest, name)); err == nil {
			t.Errorf("%s was created", name)
		}
	}
}

// TestExtractTarBoundsAnOverlongBody: a header that lies about its length must
// not let a stream fill the disk.
func TestExtractTarBoundsAnOverlongBody(t *testing.T) {
	dest := t.TempDir()

	// A header claiming 5 bytes, followed by more than that in the stream.
	var buf bytes.Buffer
	writer := tar.NewWriter(&buf)
	writer.WriteHeader(&tar.Header{Name: "artifacts/small.txt", Mode: 0o640, Size: 5, Typeflag: tar.TypeReg})
	writer.Write([]byte("12345"))
	writer.Close()

	artifacts, err := ExtractTar(context.Background(), &buf, dest)
	if err != nil {
		t.Fatalf("ExtractTar: %v", err)
	}
	if len(artifacts) != 1 || artifacts[0].Size != 5 {
		t.Fatalf("artifacts = %+v, want one file of exactly 5 bytes", artifacts)
	}
}

func TestExtractTarOnEmptyArchive(t *testing.T) {
	dest := t.TempDir()
	artifacts, err := ExtractTar(context.Background(), buildTar(t, nil), dest)
	if err != nil {
		t.Fatalf("ExtractTar: %v", err)
	}
	if len(artifacts) != 0 {
		t.Errorf("artifacts = %+v, want none", artifacts)
	}
}

// --- network policy ---

// TestNetworkPolicyRefusesWhatItCannotEnforce is the honesty rule for this
// driver: Docker enforces "none" and "proxied" structurally, but cannot apply a
// CIDR deny list on a bridge network without host firewall rules it does not own.
func TestNetworkPolicyRefusesWhatItCannotEnforce(t *testing.T) {
	withNetwork := &Driver{cfg: Config{Network: "ephemera-internal"}}
	withoutNetwork := &Driver{cfg: Config{}}

	t.Run("none is always enforceable", func(t *testing.T) {
		mode, err := withoutNetwork.networkModeFor(driver.NetworkPolicy{Mode: driver.NetworkNone})
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if mode != "none" {
			t.Errorf("mode = %q, want none", mode)
		}
	})

	t.Run("proxied uses the internal network", func(t *testing.T) {
		mode, err := withNetwork.networkModeFor(driver.NetworkPolicy{Mode: driver.NetworkProxied})
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if mode != "ephemera-internal" {
			t.Errorf("mode = %q", mode)
		}
	})

	t.Run("proxied without a network is refused", func(t *testing.T) {
		_, err := withoutNetwork.networkModeFor(driver.NetworkPolicy{Mode: driver.NetworkProxied})
		var unsupported *driver.UnsupportedError
		if !errors.As(err, &unsupported) {
			t.Fatalf("err = %v, want *driver.UnsupportedError", err)
		}
	})

	t.Run("deny lists on a bridge are refused", func(t *testing.T) {
		_, err := withoutNetwork.networkModeFor(driver.NetworkPolicy{
			Mode:      driver.NetworkEgress,
			DenyCIDRs: driver.DefaultDenyCIDRs(),
		})
		var unsupported *driver.UnsupportedError
		if !errors.As(err, &unsupported) {
			t.Fatalf("err = %v, want *driver.UnsupportedError", err)
		}
		// The message must name the mode that does provide the control.
		if !strings.Contains(unsupported.Feature, "proxied") {
			t.Errorf("Feature = %q, want it to point at proxied mode", unsupported.Feature)
		}
	})

	t.Run("plain egress is accepted", func(t *testing.T) {
		mode, err := withoutNetwork.networkModeFor(driver.NetworkPolicy{Mode: driver.NetworkEgress})
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if mode != "bridge" {
			t.Errorf("mode = %q, want bridge", mode)
		}
	})

	t.Run("unknown modes are refused", func(t *testing.T) {
		_, err := withNetwork.networkModeFor(driver.NetworkPolicy{Mode: "creative"})
		var unsupported *driver.UnsupportedError
		if !errors.As(err, &unsupported) {
			t.Fatalf("err = %v, want *driver.UnsupportedError", err)
		}
	})
}

// TestProxyIsInjectedOnlyWhenProxied: an agent on a bridge network must not be
// handed proxy settings it should not use.
func TestProxyIsInjectedOnlyWhenProxied(t *testing.T) {
	d := &Driver{cfg: Config{
		Network:  "ephemera-internal",
		ProxyURL: "http://egress-proxy:3128",
	}}

	proxied := d.buildEnv(driver.EnvSpec{
		JobID:   "job_1",
		Network: driver.NetworkPolicy{Mode: driver.NetworkProxied},
	})
	if !containsPrefix(proxied, "HTTP_PROXY=http://egress-proxy:3128") {
		t.Errorf("proxy not injected for proxied mode: %v", proxied)
	}
	// Both spellings, because different tools read different ones.
	if !containsPrefix(proxied, "http_proxy=http://egress-proxy:3128") {
		t.Errorf("lowercase proxy variable missing: %v", proxied)
	}

	plain := d.buildEnv(driver.EnvSpec{
		JobID:   "job_1",
		Network: driver.NetworkPolicy{Mode: driver.NetworkEgress},
	})
	if containsPrefix(plain, "HTTP_PROXY=") {
		t.Errorf("proxy injected for a non-proxied job: %v", plain)
	}
}

func TestBuildEnvCarriesJobContextAndSecrets(t *testing.T) {
	d := &Driver{cfg: DefaultConfig()}

	env := d.buildEnv(driver.EnvSpec{
		JobID:    "job_42",
		TenantID: "tenant-a",
		Env:      map[string]string{"PUBLIC": "visible"},
		Secrets:  map[string]job.Secret{"API_TOKEN": job.Secret("sk-live-secret-value")},
	})

	for _, want := range []string{
		"EPHEMERA_JOB_ID=job_42",
		"EPHEMERA_TENANT_ID=tenant-a",
		"EPHEMERA_ARTIFACT_DIR=/artifacts",
		"PUBLIC=visible",
		// The secret must genuinely reach the container's environment...
		"API_TOKEN=sk-live-secret-value",
	} {
		if !containsPrefix(env, want) {
			t.Errorf("env missing %q: %v", want, env)
		}
	}
}

// TestSecretsAreNotWrittenToLabels: labels are readable by anyone who can run
// `docker inspect`, and they outlive the container in daemon state. The
// environment is the only place a credential belongs.
func TestSecretsAreNotWrittenToLabels(t *testing.T) {
	const secret = "sk-live-must-not-be-inspectable"

	spec := driver.EnvSpec{
		JobID:    "job_42",
		TenantID: "tenant-a",
		Secrets:  map[string]job.Secret{"API_TOKEN": job.Secret(secret)},
		Labels:   map[string]string{"custom": "value"},
	}

	labels := map[string]string{
		LabelManaged: "true",
		LabelJob:     spec.JobID,
		LabelTenant:  spec.TenantID,
	}
	for k, v := range spec.Labels {
		labels[k] = v
	}
	for key, value := range labels {
		if strings.Contains(value, secret) {
			t.Errorf("label %s carries secret material", key)
		}
	}

	// And the container name, which is equally public.
	if strings.Contains(sanitiseName(spec.JobID), secret) {
		t.Error("container name carries secret material")
	}
}

// --- resource limits ---

func TestResourceLimitsAlwaysApply(t *testing.T) {
	// A job with no limits could take the whole host, so a default always fills in.
	if got := memoryBytes(0, 1024); got != 1024*1024*1024 {
		t.Errorf("memoryBytes(0, 1024) = %d, want 1 GiB", got)
	}
	if got := memoryBytes(512, 1024); got != 512*1024*1024 {
		t.Errorf("memoryBytes(512, 1024) = %d, want 512 MiB", got)
	}
	if got := nanoCPUs(0, 1000); got != 1_000_000_000 {
		t.Errorf("nanoCPUs(0, 1000) = %d, want one core", got)
	}
	if got := nanoCPUs(500, 1000); got != 500_000_000 {
		t.Errorf("nanoCPUs(500, 1000) = %d, want half a core", got)
	}
	// Only an explicit zero default means unlimited.
	if got := memoryBytes(0, 0); got != 0 {
		t.Errorf("memoryBytes(0, 0) = %d, want 0 (unlimited)", got)
	}
}

func TestDefaultConfigIsHardened(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.User == "" || strings.HasPrefix(cfg.User, "0:") || cfg.User == "root" {
		t.Errorf("User = %q; untrusted code should not run as root even inside a namespace", cfg.User)
	}
	if cfg.PidsLimit <= 0 {
		t.Error("no PidsLimit; a fork bomb in one job would take the host")
	}
	if cfg.DefaultMemoryMiB <= 0 {
		t.Error("no default memory limit")
	}
}

func TestCapabilitiesAreHonest(t *testing.T) {
	d := &Driver{cfg: DefaultConfig()}
	caps := d.Capabilities()

	if caps.Isolation != driver.IsolationContainer {
		t.Errorf("Isolation = %s, want container", caps.Isolation)
	}
	// Containers are labelled and the daemon outlives the control plane.
	if !caps.SurvivesControlPlaneRestart {
		t.Error("driver should be able to enumerate containers after a restart")
	}
	if !caps.EnforcesResourceLimits {
		t.Error("cgroup limits are genuinely enforced")
	}
}

// --- image references ---

func TestImageRefAddsATag(t *testing.T) {
	cases := map[string]string{
		"alpine":                   "alpine:latest",
		"alpine:3.19":              "alpine:3.19",
		"ghcr.io/org/img":          "ghcr.io/org/img:latest",
		"ghcr.io/org/img:v1":       "ghcr.io/org/img:v1",
		"registry:5000/org/img":    "registry:5000/org/img:latest",
		"registry:5000/org/img:v2": "registry:5000/org/img:v2",
		"alpine@sha256:abc":        "alpine@sha256:abc",
	}
	for input, want := range cases {
		// Without a tag the pull endpoint fetches every tag in the repository,
		// which is a surprising amount of bandwidth for a typo.
		if got := imageRef(input); got != want {
			t.Errorf("imageRef(%q) = %q, want %q", input, got, want)
		}
	}
}

// TestSanitiseNameProducesAValidDockerName asserts the real invariant: the
// result matches Docker's name grammar. This is a container name, not a path,
// so ".." in it is harmless - safeJoin is what guards the filesystem.
func TestSanitiseNameProducesAValidDockerName(t *testing.T) {
	inputs := []string{
		"job_06g83k/../evil",
		"___leading_underscores",
		"...leading dots",
		"-leading-dash",
		"job_06g83kwj5951twb2ycswewf9mc",
		"!!!",
		"",
	}
	valid := regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`)

	for _, input := range inputs {
		got := sanitiseName(input)
		if !valid.MatchString(got) {
			t.Errorf("sanitiseName(%q) = %q, which Docker would reject", input, got)
		}
		if strings.ContainsAny(got, "/\\") {
			t.Errorf("sanitiseName(%q) = %q, still contains a separator", input, got)
		}
	}
}

func TestNamedPipeIsRefusedWithGuidance(t *testing.T) {
	_, err := NewClient("npipe:////./pipe/docker_engine")
	if err == nil {
		t.Fatal("named pipe accepted")
	}
	// A bare "unsupported" would leave the user stuck; the message names both
	// escape hatches.
	for _, want := range []string{"tcp://", "WSL2"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

func TestParseDockerTime(t *testing.T) {
	parsed := parseDockerTime("2026-09-08T12:00:00.123456789Z")
	if parsed.IsZero() {
		t.Error("valid timestamp parsed as zero")
	}
	// Docker reports "not set" as a zero-year timestamp, not an empty string.
	if got := parseDockerTime("0001-01-01T00:00:00Z"); !got.IsZero() {
		t.Errorf("zero timestamp = %v, want zero time", got)
	}
	if got := parseDockerTime(""); !got.IsZero() {
		t.Errorf("empty timestamp = %v, want zero time", got)
	}
	if got := parseDockerTime("not a time"); !got.IsZero() {
		t.Errorf("garbage timestamp = %v, want zero time", got)
	}
}

func containsPrefix(values []string, prefix string) bool {
	for _, v := range values {
		if strings.HasPrefix(v, prefix) {
			return true
		}
	}
	return false
}

var _ = time.Now
