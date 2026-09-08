package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bytes-as/airlock/internal/admission"
	"github.com/bytes-as/airlock/internal/artifact"
	"github.com/bytes-as/airlock/internal/driver"
	"github.com/bytes-as/airlock/internal/job"
	"github.com/bytes-as/airlock/internal/logstream"
	"github.com/bytes-as/airlock/internal/queue"
	"github.com/bytes-as/airlock/internal/queue/embedded"
)

// A real HTTP server over a real bbolt queue and a real artifact store on disk.

type fixture struct {
	t         *testing.T
	server    *httptest.Server
	queue     *embedded.Queue
	artifacts *artifact.Local
	logs      *logstream.Broker
	admission *admission.Controller
	now       time.Time
}

var base = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

func newFixture(t *testing.T, cfg Config, limits admission.Limits) *fixture {
	t.Helper()
	root := t.TempDir()

	f := &fixture{t: t, now: base}
	clock := func() time.Time { return f.now }

	q, err := embedded.Open(filepath.Join(root, "queue.db"), embedded.WithClock(clock))
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	t.Cleanup(func() { q.Close() })
	f.queue = q

	store, err := artifact.NewLocal(filepath.Join(root, "artifacts"),
		[]byte("api-test-signing-key-0123456789"), artifact.WithClock(clock))
	if err != nil {
		t.Fatalf("new artifact store: %v", err)
	}
	f.artifacts = store

	f.logs = logstream.NewBroker()
	f.admission = admission.New(limits, admission.WithClock(clock))

	s, err := NewServer(cfg, Deps{
		Queue:     q,
		Admission: f.admission,
		Artifacts: store,
		Logs:      f.logs,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		Clock:     clock,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	f.server = httptest.NewServer(s.Handler())
	t.Cleanup(f.server.Close)
	return f
}

func generousLimits() admission.Limits {
	return admission.Limits{SubmitsPerSecond: 1000, Burst: 1000, MaxConcurrent: 100}
}

func (f *fixture) do(method, path string, body any, headers map[string]string) *http.Response {
	f.t.Helper()

	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			f.t.Fatalf("marshal body: %v", err)
		}
		reader = bytes.NewReader(encoded)
	}

	req, err := http.NewRequest(method, f.server.URL+path, reader)
	if err != nil {
		f.t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := f.server.Client().Do(req)
	if err != nil {
		f.t.Fatalf("do request: %v", err)
	}
	return resp
}

func decode[T any](t *testing.T, resp *http.Response) T {
	t.Helper()
	defer resp.Body.Close()
	var out T
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return out
}

func validSubmission() SubmitRequest {
	return SubmitRequest{Image: "airlock/agent:latest", Command: []string{"run"}}
}

func TestSubmitAcceptsAndReturnsAJob(t *testing.T) {
	f := newFixture(t, DefaultConfig(), generousLimits())

	resp := f.do("POST", "/v1/jobs", validSubmission(), nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); !strings.HasPrefix(loc, "/v1/jobs/job_") {
		t.Errorf("Location = %q", loc)
	}

	created := decode[JobResponse](t, resp)
	if created.ID == "" {
		t.Fatal("no job ID returned")
	}
	if created.State != string(job.StateQueued) {
		t.Errorf("state = %s, want queued", created.State)
	}
	// The response must tell the caller where to watch and where results land,
	// so a client never has to construct URLs by hand.
	if created.LogsURL == "" || created.ArtifactsURL == "" {
		t.Errorf("response does not link to logs and artifacts: %+v", created)
	}
}

func TestSubmitRejectsInvalidSpecs(t *testing.T) {
	f := newFixture(t, DefaultConfig(), generousLimits())

	cases := []struct {
		name string
		body SubmitRequest
	}{
		{"neither image nor command", SubmitRequest{}},
		{"negative deadline", SubmitRequest{Image: "x", DeadlineSeconds: -1}},
		{"negative attempts", SubmitRequest{Image: "x", MaxAttempts: -1}},
		{"incomplete secret ref", SubmitRequest{Image: "x", Secrets: []job.SecretRef{{Name: "A"}}}},
		{"unknown priority", SubmitRequest{Image: "x", Priority: "urgent"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp := f.do("POST", "/v1/jobs", c.body, nil)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", resp.StatusCode)
			}
			body := decode[ErrorResponse](t, resp)
			if body.Error == "" || body.Message == "" {
				t.Errorf("error response is not actionable: %+v", body)
			}
		})
	}
}

func TestSubmitRejectsMalformedJSON(t *testing.T) {
	f := newFixture(t, DefaultConfig(), generousLimits())

	req, _ := http.NewRequest("POST", f.server.URL+"/v1/jobs", strings.NewReader("{not json"))
	resp, err := f.server.Client().Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

// TestRateLimitReturnsRetryAfterThatWorks is the contract that matters: a
// client that obeys Retry-After must succeed. A hint that does not work
// produces the retry storm the limit was defending against.
func TestRateLimitReturnsRetryAfterThatWorks(t *testing.T) {
	f := newFixture(t, DefaultConfig(), admission.Limits{
		SubmitsPerSecond: 4, Burst: 2, MaxConcurrent: 100,
	})

	for i := 0; i < 2; i++ {
		resp := f.do("POST", "/v1/jobs", validSubmission(), nil)
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("submission %d: status = %d", i, resp.StatusCode)
		}
	}

	resp := f.do("POST", "/v1/jobs", validSubmission(), nil)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", resp.StatusCode)
	}

	retryAfter := resp.Header.Get("Retry-After")
	if retryAfter == "" {
		t.Fatal("429 with no Retry-After header")
	}
	seconds, err := strconv.Atoi(retryAfter)
	if err != nil || seconds < 1 {
		t.Fatalf("Retry-After = %q, want a positive integer", retryAfter)
	}

	body := decode[ErrorResponse](t, resp)
	if body.Error != string(admission.OutcomeRateLimited) {
		t.Errorf("error = %q, want rate_limited", body.Error)
	}
	if body.RetryAfterSeconds != seconds {
		t.Errorf("body says %ds, header says %ds", body.RetryAfterSeconds, seconds)
	}

	// Honour the hint; it must then succeed.
	f.now = f.now.Add(time.Duration(seconds) * time.Second)
	after := f.do("POST", "/v1/jobs", validSubmission(), nil)
	defer after.Body.Close()
	if after.StatusCode != http.StatusAccepted {
		t.Errorf("status = %d after honouring Retry-After of %ds, want 202", after.StatusCode, seconds)
	}
}

// TestQueueFullIs503NotAClientError: saturation is our problem, not the
// caller's, and the status code should say which.
func TestQueueFullIs503NotAClientError(t *testing.T) {
	f := newFixture(t, DefaultConfig(), generousLimits())
	f.admission = admission.New(generousLimits(),
		admission.WithClock(func() time.Time { return f.now }),
		admission.WithGlobalCapacity(2))

	// Rebuild the server with the capacity-limited controller.
	s, err := NewServer(DefaultConfig(), Deps{
		Queue:     f.queue,
		Admission: f.admission,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		Clock:     func() time.Time { return f.now },
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	f.server.Close()
	f.server = httptest.NewServer(s.Handler())
	t.Cleanup(f.server.Close)

	for i := 0; i < 2; i++ {
		resp := f.do("POST", "/v1/jobs", validSubmission(), nil)
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("submission %d: status = %d", i, resp.StatusCode)
		}
	}

	resp := f.do("POST", "/v1/jobs", validSubmission(), nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 when the system is saturated", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Error("503 with no Retry-After")
	}
}

func TestGetJob(t *testing.T) {
	f := newFixture(t, DefaultConfig(), generousLimits())

	created := decode[JobResponse](t, f.do("POST", "/v1/jobs", validSubmission(), nil))

	resp := f.do("GET", "/v1/jobs/"+created.ID, nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	fetched := decode[JobResponse](t, resp)
	if fetched.ID != created.ID {
		t.Errorf("ID = %s, want %s", fetched.ID, created.ID)
	}
}

func TestGetUnknownJobIs404(t *testing.T) {
	f := newFixture(t, DefaultConfig(), generousLimits())
	resp := f.do("GET", "/v1/jobs/job_does_not_exist", nil, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

// TestTenantsCannotSeeEachOthersJobs, and the response is 404 rather than 403:
// confirming a job exists but belongs to someone else leaks the existence of
// other tenants' work.
func TestTenantsCannotSeeEachOthersJobs(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Tokens = map[string]string{"token-a": "tenant-a", "token-b": "tenant-b"}
	f := newFixture(t, cfg, generousLimits())

	authA := map[string]string{"Authorization": "Bearer token-a"}
	authB := map[string]string{"Authorization": "Bearer token-b"}

	created := decode[JobResponse](t, f.do("POST", "/v1/jobs", validSubmission(), authA))
	if created.TenantID != "tenant-a" {
		t.Fatalf("TenantID = %q, want tenant-a", created.TenantID)
	}

	resp := f.do("GET", "/v1/jobs/"+created.ID, nil, authB)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404 so the job's existence is not confirmed", resp.StatusCode)
	}

	// Nor in a listing.
	list := decode[struct {
		Jobs []JobResponse `json:"jobs"`
	}](t, f.do("GET", "/v1/jobs", nil, authB))
	if len(list.Jobs) != 0 {
		t.Errorf("tenant-b sees %d of tenant-a's jobs", len(list.Jobs))
	}
}

func TestAuthentication(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Tokens = map[string]string{"good-token": "tenant-a"}
	f := newFixture(t, cfg, generousLimits())

	cases := []struct {
		name    string
		headers map[string]string
	}{
		{"no header", nil},
		{"wrong scheme", map[string]string{"Authorization": "Basic Z29vZA=="}},
		{"unknown token", map[string]string{"Authorization": "Bearer nope"}},
		{"empty bearer", map[string]string{"Authorization": "Bearer "}},
	}

	var bodies []string
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp := f.do("POST", "/v1/jobs", validSubmission(), c.headers)
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", resp.StatusCode)
			}
			if resp.Header.Get("WWW-Authenticate") == "" {
				t.Error("401 without WWW-Authenticate")
			}
			raw, _ := io.ReadAll(resp.Body)
			bodies = append(bodies, string(raw))
		})
	}

	// Every rejection must look identical. Distinguishing "no token" from
	// "wrong token" tells an attacker when they have found a real one.
	for i := 1; i < len(bodies); i++ {
		if bodies[i] != bodies[0] {
			t.Errorf("auth failures are distinguishable:\n%s\nvs\n%s", bodies[0], bodies[i])
		}
	}

	ok := f.do("POST", "/v1/jobs", validSubmission(), map[string]string{"Authorization": "Bearer good-token"})
	defer ok.Body.Close()
	if ok.StatusCode != http.StatusAccepted {
		t.Errorf("valid token rejected: %d", ok.StatusCode)
	}
}

func TestListFiltersAndLimits(t *testing.T) {
	f := newFixture(t, DefaultConfig(), generousLimits())

	for i := 0; i < 5; i++ {
		f.do("POST", "/v1/jobs", validSubmission(), nil).Body.Close()
	}

	list := decode[struct {
		Jobs  []JobResponse `json:"jobs"`
		Count int           `json:"count"`
	}](t, f.do("GET", "/v1/jobs?limit=3", nil, nil))
	if list.Count != 3 || len(list.Jobs) != 3 {
		t.Errorf("count = %d, want 3", list.Count)
	}

	byState := decode[struct {
		Count int `json:"count"`
	}](t, f.do("GET", "/v1/jobs?state=queued", nil, nil))
	if byState.Count != 5 {
		t.Errorf("queued count = %d, want 5", byState.Count)
	}

	for _, bad := range []string{"?state=nonsense", "?limit=0", "?limit=99999", "?limit=abc"} {
		resp := f.do("GET", "/v1/jobs"+bad, nil, nil)
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("GET /v1/jobs%s = %d, want 400", bad, resp.StatusCode)
		}
	}
}

// TestLogsStreamOverSSE checks the flight recorder's live half end to end,
// including that the wrapper preserves flushing.
func TestLogsStreamOverSSE(t *testing.T) {
	f := newFixture(t, DefaultConfig(), generousLimits())
	created := decode[JobResponse](t, f.do("POST", "/v1/jobs", validSubmission(), nil))

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET", f.server.URL+created.LogsURL, nil)
	resp, err := f.server.Client().Do(req)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}

	// Publish after the stream is open, so this exercises live delivery rather
	// than replayed history.
	go func() {
		time.Sleep(50 * time.Millisecond)
		f.logs.Publish(created.ID, driver.LogLine{
			At: base, Stream: driver.StreamStdout, Text: "hello from the agent",
		})
		time.Sleep(50 * time.Millisecond)
		f.logs.End(created.ID)
	}()

	var sawLine, sawEnd bool
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data: ") {
			payload := strings.TrimPrefix(line, "data: ")
			if strings.Contains(payload, "hello from the agent") {
				sawLine = true
			}
		}
		if line == "event: end" {
			sawEnd = true
			break
		}
	}

	if !sawLine {
		t.Error("agent output never arrived over SSE")
	}
	if !sawEnd {
		t.Error("stream did not signal end; a client would hang forever")
	}
}

func TestArtifactListingReturnsSignedURLs(t *testing.T) {
	f := newFixture(t, DefaultConfig(), generousLimits())
	created := decode[JobResponse](t, f.do("POST", "/v1/jobs", validSubmission(), nil))

	// Put a real file in the store, as the scheduler would.
	src := filepath.Join(t.TempDir(), "screenshot.png")
	writeFile(t, src, "png-bytes-here")
	if _, err := f.artifacts.Put(context.Background(), created.ID,
		[]driver.Artifact{{Name: "screenshot.png", Path: src}}); err != nil {
		t.Fatalf("put artifact: %v", err)
	}

	listing := decode[struct {
		Artifacts []struct {
			Name string `json:"name"`
			Size int64  `json:"size"`
			URL  string `json:"url"`
		} `json:"artifacts"`
	}](t, f.do("GET", created.ArtifactsURL, nil, nil))

	if len(listing.Artifacts) != 1 {
		t.Fatalf("artifacts = %d, want 1", len(listing.Artifacts))
	}
	a := listing.Artifacts[0]
	if a.Name != "screenshot.png" || a.Size == 0 {
		t.Errorf("artifact = %+v", a)
	}

	// Follow the signed URL. It carries its own authorisation, so it works
	// without a token - which is the point of handing one to a browser.
	parsed, err := url.Parse(a.URL)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	resp := f.do("GET", parsed.RequestURI(), nil, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("download = %d: %s", resp.StatusCode, body)
	}
	content, _ := io.ReadAll(resp.Body)
	if string(content) != "png-bytes-here" {
		t.Errorf("content = %q", content)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "image/png") {
		t.Errorf("Content-Type = %q", ct)
	}
}

func TestArtifactDownloadRequiresAValidSignature(t *testing.T) {
	f := newFixture(t, DefaultConfig(), generousLimits())
	created := decode[JobResponse](t, f.do("POST", "/v1/jobs", validSubmission(), nil))

	src := filepath.Join(t.TempDir(), "shot.png")
	writeFile(t, src, "bytes")
	if _, err := f.artifacts.Put(context.Background(), created.ID,
		[]driver.Artifact{{Name: "shot.png", Path: src}}); err != nil {
		t.Fatalf("put: %v", err)
	}

	path := fmt.Sprintf("/v1/jobs/%s/artifacts/shot.png", created.ID)
	future := f.now.Add(time.Hour).Unix()

	cases := []struct {
		name  string
		query string
		want  int
	}{
		{"no signature", "", http.StatusUnauthorized},
		{"forged signature", fmt.Sprintf("?expires=%d&signature=forged", future), http.StatusUnauthorized},
		{"missing expiry", "?signature=whatever", http.StatusUnauthorized},
		{"non-numeric expiry", "?expires=soon&signature=whatever", http.StatusUnauthorized},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp := f.do("GET", path+c.query, nil, nil)
			defer resp.Body.Close()
			if resp.StatusCode != c.want {
				t.Errorf("status = %d, want %d", resp.StatusCode, c.want)
			}
		})
	}

	// A genuine but expired link reports 410, so a client knows to ask for a
	// fresh one rather than assume it is forbidden.
	signed, err := f.artifacts.SignedURL(created.ID, "shot.png", time.Minute)
	if err != nil {
		t.Fatalf("SignedURL: %v", err)
	}
	parsed, _ := url.Parse(signed)
	f.now = f.now.Add(2 * time.Minute)

	resp := f.do("GET", parsed.RequestURI(), nil, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusGone {
		t.Errorf("expired link = %d, want 410", resp.StatusCode)
	}
}

func TestHealthAndReadiness(t *testing.T) {
	f := newFixture(t, DefaultConfig(), generousLimits())

	for _, path := range []string{"/healthz", "/readyz"} {
		resp := f.do("GET", path, nil, nil)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("%s = %d, want 200", path, resp.StatusCode)
		}
	}

	// Readiness depends on storage; liveness must not. Closing the queue makes
	// readiness fail while the process is still perfectly alive.
	f.queue.Close()

	resp := f.do("GET", "/readyz", nil, nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("/readyz with a dead queue = %d, want 503", resp.StatusCode)
	}

	live := f.do("GET", "/healthz", nil, nil)
	live.Body.Close()
	if live.StatusCode != http.StatusOK {
		t.Errorf("/healthz = %d; liveness must not depend on storage, or an orchestrator "+
			"restarts a process that was doing nothing wrong", live.StatusCode)
	}
}

func TestPriorityParsing(t *testing.T) {
	f := newFixture(t, DefaultConfig(), generousLimits())

	cases := map[string]job.Priority{
		"":       job.PriorityNormal,
		"normal": job.PriorityNormal,
		"low":    job.PriorityLow,
		"high":   job.PriorityHigh,
		"HIGH":   job.PriorityHigh,
	}
	for input, want := range cases {
		body := validSubmission()
		body.Priority = input
		created := decode[JobResponse](t, f.do("POST", "/v1/jobs", body, nil))
		if created.Priority != int(want) {
			t.Errorf("priority %q = %d, want %d", input, created.Priority, want)
		}
	}
}

func TestFailureIsExplainedToTheCaller(t *testing.T) {
	f := newFixture(t, DefaultConfig(), generousLimits())
	created := decode[JobResponse](t, f.do("POST", "/v1/jobs", validSubmission(), nil))

	// Drive the job to a terminal failure the way the scheduler would.
	claimed, err := f.queue.Claim(context.Background(), "worker-1", time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := claimed.Fail(job.FailureAgentCrash.Newf("agent exited with code 7"), f.now); err != nil {
		t.Fatalf("fail: %v", err)
	}
	if err := f.queue.Complete(context.Background(), claimed); err != nil {
		t.Fatalf("complete: %v", err)
	}

	fetched := decode[JobResponse](t, f.do("GET", "/v1/jobs/"+created.ID, nil, nil))
	if fetched.Failure == nil {
		t.Fatal("no failure in the response")
	}
	// The caller needs all three: what happened, whose fault it is, and whether
	// retrying is worth their time.
	if fetched.Failure.Kind != string(job.FailureAgentCrash) {
		t.Errorf("kind = %q", fetched.Failure.Kind)
	}
	if fetched.Failure.Fault != string(job.FaultUser) {
		t.Errorf("fault = %q, want user", fetched.Failure.Fault)
	}
	if fetched.Failure.Retryable {
		t.Error("an agent crash was reported as retryable")
	}
}

func TestOversizedBodyIsRejected(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxRequestBytes = 512
	f := newFixture(t, cfg, generousLimits())

	body := validSubmission()
	body.Env = map[string]string{"BIG": strings.Repeat("x", 4096)}

	resp := f.do("POST", "/v1/jobs", body, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for an oversized body", resp.StatusCode)
	}
}

func TestOpenModeUsesTheTenantHeader(t *testing.T) {
	f := newFixture(t, DefaultConfig(), generousLimits()) // no tokens configured

	created := decode[JobResponse](t, f.do("POST", "/v1/jobs", validSubmission(),
		map[string]string{"X-Tenant-ID": "tenant-from-header"}))
	if created.TenantID != "tenant-from-header" {
		t.Errorf("TenantID = %q", created.TenantID)
	}

	anon := decode[JobResponse](t, f.do("POST", "/v1/jobs", validSubmission(), nil))
	if anon.TenantID != "default" {
		t.Errorf("TenantID = %q, want default", anon.TenantID)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o640); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

var _ = queue.ErrNotFound

// TestSubmitCarriesEgressRegion: the field must reach the job spec, or the
// driver's region support is unreachable from the API and therefore dead.
func TestSubmitCarriesEgressRegion(t *testing.T) {
	f := newFixture(t, DefaultConfig(), generousLimits())

	req := validSubmission()
	req.EgressRegion = "eu"

	resp := f.do("POST", "/v1/jobs", req, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	created := decode[JobResponse](t, resp)

	stored, err := f.queue.Get(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if stored.Spec.EgressRegion != "eu" {
		t.Errorf("EgressRegion = %q, want %q - the request never reached the spec", stored.Spec.EgressRegion, "eu")
	}
}
