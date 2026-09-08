// Package api exposes the control plane over HTTP.
//
// Design notes worth defending:
//
//   - Admission decisions become real HTTP semantics. A refused submission is
//     429 with Retry-After, not 500 and not a silent queue. A client that obeys
//     Retry-After must succeed, which is asserted in the tests.
//
//   - Logs stream over Server-Sent Events rather than WebSocket. The traffic is
//     one-way, SSE is plain HTTP with no upgrade handshake, and browsers
//     reconnect natively. WebSocket would buy bidirectionality we do not need.
//
//   - Artifact downloads verify a signature rather than a session. That keeps
//     the local store and S3 answering "who can read this, and for how long?"
//     the same way, so moving to S3 changes the backend and not the model.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bytes-as/airlock/internal/admission"
	"github.com/bytes-as/airlock/internal/artifact"
	"github.com/bytes-as/airlock/internal/job"
	"github.com/bytes-as/airlock/internal/logstream"
	"github.com/bytes-as/airlock/internal/queue"
	"github.com/bytes-as/airlock/internal/scheduler"
)

// Config tunes the HTTP surface.
type Config struct {
	// Tokens maps bearer tokens to tenant IDs. When empty the server runs in
	// open mode: the tenant comes from a header and anyone may call. That is
	// fine for a local demo and unacceptable anywhere else, so the server says
	// so loudly at startup rather than leaving it to be discovered.
	Tokens map[string]string

	// ArtifactURLTTL is how long issued download links stay valid.
	ArtifactURLTTL time.Duration

	// MaxRequestBytes bounds a submission body, so a malformed or hostile
	// client cannot exhaust memory before validation runs.
	MaxRequestBytes int64
}

// DefaultConfig returns sensible HTTP settings.
func DefaultConfig() Config {
	return Config{
		ArtifactURLTTL:  15 * time.Minute,
		MaxRequestBytes: 1 << 20, // 1 MiB
	}
}

// Deps are the collaborators the API needs.
type Deps struct {
	Queue     queue.Queue
	Admission *admission.Controller
	Artifacts *artifact.Local
	Logs      *logstream.Broker
	Scheduler *scheduler.Scheduler
	Logger    *slog.Logger
	Clock     func() time.Time
}

// Server is the HTTP API.
type Server struct {
	cfg  Config
	deps Deps
	log  *slog.Logger
	now  func() time.Time
}

// NewServer builds the API.
func NewServer(cfg Config, deps Deps) (*Server, error) {
	if deps.Queue == nil {
		return nil, errors.New("api: Queue is required")
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.Clock == nil {
		deps.Clock = time.Now
	}
	if cfg.ArtifactURLTTL <= 0 {
		cfg.ArtifactURLTTL = DefaultConfig().ArtifactURLTTL
	}
	if cfg.MaxRequestBytes <= 0 {
		cfg.MaxRequestBytes = DefaultConfig().MaxRequestBytes
	}
	if len(cfg.Tokens) == 0 {
		deps.Logger.Warn("API is running with no tokens configured: " +
			"any caller may submit jobs as any tenant. Set tokens before exposing this beyond localhost.")
	}
	return &Server{cfg: cfg, deps: deps, log: deps.Logger, now: deps.Clock}, nil
}

// Handler returns the routed HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /v1/jobs", s.handleSubmit)
	mux.HandleFunc("GET /v1/jobs", s.handleList)
	mux.HandleFunc("GET /v1/jobs/{id}", s.handleGet)
	mux.HandleFunc("GET /v1/jobs/{id}/logs", s.handleLogs)
	mux.HandleFunc("GET /v1/jobs/{id}/artifacts", s.handleListArtifacts)
	mux.HandleFunc("GET /v1/jobs/{id}/artifacts/{name...}", s.handleDownloadArtifact)
	mux.HandleFunc("GET /v1/stats", s.handleStats)
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /readyz", s.handleReady)

	return s.withLogging(mux)
}

// --- request and response shapes ---

// SubmitRequest is the job submission body.
type SubmitRequest struct {
	Image    string            `json:"image,omitempty"`
	Command  []string          `json:"command,omitempty"`
	Input    map[string]string `json:"input,omitempty"`
	Env      map[string]string `json:"env,omitempty"`
	Secrets  []job.SecretRef   `json:"secrets,omitempty"`
	Priority string            `json:"priority,omitempty"`
	// DeadlineSeconds bounds the run. Zero takes the platform default.
	DeadlineSeconds int   `json:"deadline_seconds,omitempty"`
	MaxAttempts     int   `json:"max_attempts,omitempty"`
	CPUMillis       int64 `json:"cpu_millis,omitempty"`
	MemoryMiB       int64 `json:"memory_mib,omitempty"`
	// EgressRegion requests an egress point in a particular location. Refused
	// with 400 if the platform has none, rather than run from somewhere else.
	EgressRegion string `json:"egress_region,omitempty"`
}

// JobResponse is the API's view of a job.
//
// A hand-written projection rather than the job struct itself: the wire format
// is a contract with clients, and coupling it to an internal type means every
// internal refactor risks silently changing the API.
type JobResponse struct {
	ID          string     `json:"id"`
	TenantID    string     `json:"tenant_id"`
	State       string     `json:"state"`
	Priority    int        `json:"priority"`
	Attempts    int        `json:"attempts"`
	SubmittedAt time.Time  `json:"submitted_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	EndedAt     *time.Time `json:"ended_at,omitempty"`
	DurationMS  int64      `json:"duration_ms,omitempty"`

	Failure *FailureResponse `json:"failure,omitempty"`

	LogsURL      string `json:"logs_url"`
	ArtifactsURL string `json:"artifacts_url"`
}

// FailureResponse explains a failure to the caller.
type FailureResponse struct {
	Kind      string `json:"kind"`
	Fault     string `json:"fault"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

// ErrorResponse is the shape of every error the API returns.
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
	// RetryAfterSeconds mirrors the Retry-After header, for clients that read
	// the body rather than the headers.
	RetryAfterSeconds int `json:"retry_after_seconds,omitempty"`
}

// --- handlers ---

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	tenant, ok := s.authenticate(w, r)
	if !ok {
		return
	}

	var req SubmitRequest
	body := http.MaxBytesReader(w, r.Body, s.cfg.MaxRequestBytes)
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid_body", "could not parse the request body as JSON")
		return
	}

	spec, err := specFromRequest(req)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid_spec", err.Error())
		return
	}

	priority, err := parsePriority(req.Priority)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid_priority", err.Error())
		return
	}

	// Admission before enqueue. Depth comes from the queue so it stays the
	// single source of truth on how much work is outstanding.
	depth := 0
	if stats, err := s.deps.Queue.Stats(r.Context()); err == nil {
		depth = stats.Ready + stats.Claimed
	}
	if s.deps.Admission != nil {
		if decision := s.deps.Admission.Admit(tenant, depth); !decision.Allowed() {
			s.writeRefusal(w, decision)
			return
		}
	}

	j := job.New(tenant, priority, spec, s.now())
	if err := s.deps.Queue.Enqueue(r.Context(), j); err != nil {
		if errors.Is(err, queue.ErrFull) {
			s.writeRefusal(w, admission.Decision{
				Outcome:    admission.OutcomeQueueFull,
				RetryAfter: 5 * time.Second,
				Reason:     "the queue is at capacity",
			})
			return
		}
		s.log.Error("enqueue failed", "tenant", tenant, "error", err)
		s.writeError(w, http.StatusInternalServerError, "enqueue_failed", "could not accept the job")
		return
	}

	s.log.Info("job accepted", "job", j.ID, "tenant", tenant, "priority", priority)
	w.Header().Set("Location", "/v1/jobs/"+j.ID)
	s.writeJSON(w, http.StatusAccepted, s.jobResponse(j))
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	tenant, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	j, ok := s.lookupJob(w, r, tenant)
	if !ok {
		return
	}
	s.writeJSON(w, http.StatusOK, s.jobResponse(j))
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	tenant, ok := s.authenticate(w, r)
	if !ok {
		return
	}

	filter := queue.Filter{TenantID: tenant, Limit: 50}
	if state := r.URL.Query().Get("state"); state != "" {
		filter.State = job.State(state)
		if !filter.State.Valid() {
			s.writeError(w, http.StatusBadRequest, "invalid_state", fmt.Sprintf("unknown job state %q", state))
			return
		}
	}
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 || n > 500 {
			s.writeError(w, http.StatusBadRequest, "invalid_limit", "limit must be between 1 and 500")
			return
		}
		filter.Limit = n
	}

	jobs, err := s.deps.Queue.List(r.Context(), filter)
	if err != nil {
		s.log.Error("list failed", "error", err)
		s.writeError(w, http.StatusInternalServerError, "list_failed", "could not list jobs")
		return
	}

	out := make([]JobResponse, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, s.jobResponse(j))
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"jobs": out, "count": len(out)})
}

// handleLogs streams a job's output as Server-Sent Events.
func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	tenant, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	j, ok := s.lookupJob(w, r, tenant)
	if !ok {
		return
	}
	if s.deps.Logs == nil {
		s.writeError(w, http.StatusServiceUnavailable, "logs_unavailable", "log streaming is not configured")
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		s.writeError(w, http.StatusInternalServerError, "streaming_unsupported", "this server cannot stream")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// Proxies that buffer would defeat the point of streaming entirely.
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	lines, cancel := s.deps.Logs.Subscribe(r.Context(), j.ID)
	defer cancel()

	// Keep-alive comments stop intermediaries closing an idle connection during
	// a quiet stretch of a long job.
	keepAlive := time.NewTicker(15 * time.Second)
	defer keepAlive.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-keepAlive.C:
			fmt.Fprint(w, ": keep-alive\n\n")
			flusher.Flush()
		case line, open := <-lines:
			if !open {
				fmt.Fprint(w, "event: end\ndata: {}\n\n")
				flusher.Flush()
				return
			}
			encoded, err := json.Marshal(line)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "data: %s\n\n", encoded)
			flusher.Flush()
		}
	}
}

func (s *Server) handleListArtifacts(w http.ResponseWriter, r *http.Request) {
	tenant, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	j, ok := s.lookupJob(w, r, tenant)
	if !ok {
		return
	}
	if s.deps.Artifacts == nil {
		s.writeError(w, http.StatusServiceUnavailable, "artifacts_unavailable", "artifact storage is not configured")
		return
	}

	stored, err := s.deps.Artifacts.List(r.Context(), j.ID)
	if err != nil {
		s.log.Error("artifact list failed", "job", j.ID, "error", err)
		s.writeError(w, http.StatusInternalServerError, "artifact_list_failed", "could not list artifacts")
		return
	}

	type artifactResponse struct {
		Name        string    `json:"name"`
		Size        int64     `json:"size"`
		ContentType string    `json:"content_type"`
		StoredAt    time.Time `json:"stored_at"`
		URL         string    `json:"url"`
		ExpiresIn   string    `json:"expires_in"`
	}

	out := make([]artifactResponse, 0, len(stored))
	for _, a := range stored {
		url, err := s.deps.Artifacts.SignedURL(j.ID, a.Name, s.cfg.ArtifactURLTTL)
		if err != nil {
			s.log.Error("could not sign artifact URL", "job", j.ID, "artifact", a.Name, "error", err)
			continue
		}
		out = append(out, artifactResponse{
			Name: a.Name, Size: a.Size, ContentType: a.ContentType, StoredAt: a.StoredAt,
			URL: url, ExpiresIn: s.cfg.ArtifactURLTTL.String(),
		})
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"artifacts": out, "count": len(out)})
}

// handleDownloadArtifact serves a file to a caller holding a valid signature.
//
// Signature rather than session: the same model works for the local store and
// for S3, and a signed link can be handed to a browser, a CI job or a colleague
// without giving away an API token.
func (s *Server) handleDownloadArtifact(w http.ResponseWriter, r *http.Request) {
	if s.deps.Artifacts == nil {
		s.writeError(w, http.StatusServiceUnavailable, "artifacts_unavailable", "artifact storage is not configured")
		return
	}

	jobID := r.PathValue("id")
	name := r.PathValue("name")
	signature := r.URL.Query().Get("signature")
	expiresRaw := r.URL.Query().Get("expires")

	if signature == "" || expiresRaw == "" {
		s.writeError(w, http.StatusUnauthorized, "unsigned_request", "this URL requires a signature and an expiry")
		return
	}
	expires, err := strconv.ParseInt(expiresRaw, 10, 64)
	if err != nil {
		s.writeError(w, http.StatusUnauthorized, "invalid_signature", "the link is not valid")
		return
	}

	if err := s.deps.Artifacts.VerifySignedURL(jobID, name, signature, expires); err != nil {
		if errors.Is(err, artifact.ErrLinkExpired) {
			s.writeError(w, http.StatusGone, "link_expired", "this link has expired; request a fresh one")
			return
		}
		s.writeError(w, http.StatusUnauthorized, "invalid_signature", "the link is not valid")
		return
	}

	reader, meta, err := s.deps.Artifacts.Open(r.Context(), jobID, name)
	if err != nil {
		s.writeError(w, http.StatusNotFound, "artifact_not_found", "no such artifact")
		return
	}
	defer reader.Close()

	w.Header().Set("Content-Type", meta.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
	// Screenshots and video are meant to be viewed, so no forced download; but
	// the filename is quoted so a name with spaces cannot break the header.
	w.Header().Set("Content-Disposition", fmt.Sprintf("inline; filename=%q", pathBase(meta.Name)))
	w.WriteHeader(http.StatusOK)
	if _, err := io.Copy(w, reader); err != nil {
		s.log.Warn("artifact download interrupted", "job", jobID, "artifact", name, "error", err)
	}
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authenticate(w, r); !ok {
		return
	}
	if s.deps.Scheduler == nil {
		s.writeError(w, http.StatusServiceUnavailable, "stats_unavailable", "no scheduler is attached")
		return
	}
	stats, err := s.deps.Scheduler.Stats(r.Context())
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "stats_failed", "could not read stats")
		return
	}
	s.writeJSON(w, http.StatusOK, stats)
}

// handleHealth reports that the process is alive. Deliberately does not touch
// storage: a liveness probe that fails when the database is slow causes an
// orchestrator to restart a process that was doing nothing wrong.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReady reports that the process can serve traffic, which does require
// storage. This is the one an orchestrator should gate traffic on.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if _, err := s.deps.Queue.Stats(r.Context()); err != nil {
		s.writeError(w, http.StatusServiceUnavailable, "not_ready", "queue is not reachable")
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// --- helpers ---

// authenticate resolves the caller's tenant.
func (s *Server) authenticate(w http.ResponseWriter, r *http.Request) (string, bool) {
	if len(s.cfg.Tokens) == 0 {
		// Open mode. The header is a convenience for local use; the startup
		// warning is what keeps this from being a silent hole.
		tenant := r.Header.Get("X-Tenant-ID")
		if tenant == "" {
			tenant = "default"
		}
		return tenant, true
	}

	header := r.Header.Get("Authorization")
	token, found := strings.CutPrefix(header, "Bearer ")
	if !found || token == "" {
		w.Header().Set("WWW-Authenticate", `Bearer realm="airlock"`)
		s.writeError(w, http.StatusUnauthorized, "unauthenticated", "a bearer token is required")
		return "", false
	}
	tenant, ok := s.cfg.Tokens[token]
	if !ok {
		// Deliberately identical to the missing-token response: distinguishing
		// "no token" from "wrong token" tells an attacker when they have found
		// a real one.
		w.Header().Set("WWW-Authenticate", `Bearer realm="airlock"`)
		s.writeError(w, http.StatusUnauthorized, "unauthenticated", "a bearer token is required")
		return "", false
	}
	return tenant, true
}

// lookupJob fetches a job and enforces tenant ownership.
func (s *Server) lookupJob(w http.ResponseWriter, r *http.Request, tenant string) (*job.Job, bool) {
	id := r.PathValue("id")
	j, err := s.deps.Queue.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, queue.ErrNotFound) {
			s.writeError(w, http.StatusNotFound, "job_not_found", "no such job")
			return nil, false
		}
		s.log.Error("job lookup failed", "job", id, "error", err)
		s.writeError(w, http.StatusInternalServerError, "lookup_failed", "could not read the job")
		return nil, false
	}
	if j.TenantID != tenant {
		// 404, not 403. Confirming a job exists but belongs to someone else
		// leaks the existence of other tenants' work.
		s.writeError(w, http.StatusNotFound, "job_not_found", "no such job")
		return nil, false
	}
	return j, true
}

func (s *Server) jobResponse(j *job.Job) JobResponse {
	resp := JobResponse{
		ID:           j.ID,
		TenantID:     j.TenantID,
		State:        string(j.State),
		Priority:     int(j.Priority),
		Attempts:     j.Attempts,
		SubmittedAt:  j.SubmittedAt,
		StartedAt:    j.StartedAt,
		EndedAt:      j.EndedAt,
		LogsURL:      "/v1/jobs/" + j.ID + "/logs",
		ArtifactsURL: "/v1/jobs/" + j.ID + "/artifacts",
	}
	if d := j.Duration(s.now()); d > 0 {
		resp.DurationMS = d.Milliseconds()
	}
	if j.Failure != nil {
		resp.Failure = &FailureResponse{
			Kind:      string(j.Failure.Kind),
			Fault:     string(j.Failure.Kind.Fault()),
			Message:   j.Failure.Message,
			Retryable: j.Failure.Kind.Retryable(),
		}
	}
	return resp
}

// writeRefusal turns an admission decision into HTTP semantics a client can act
// on without reading documentation.
func (s *Server) writeRefusal(w http.ResponseWriter, d admission.Decision) {
	seconds := int(d.RetryAfter.Seconds())
	if seconds < 1 {
		seconds = 1
	}
	w.Header().Set("Retry-After", strconv.Itoa(seconds))

	status := http.StatusTooManyRequests
	if d.Outcome == admission.OutcomeQueueFull {
		// 503: the system is saturated, which is our problem, not the caller's.
		status = http.StatusServiceUnavailable
	}
	s.writeJSONWithRetry(w, status, ErrorResponse{
		Error:             string(d.Outcome),
		Message:           d.Reason,
		RetryAfterSeconds: seconds,
	})
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		s.log.Warn("could not write response", "error", err)
	}
}

func (s *Server) writeJSONWithRetry(w http.ResponseWriter, status int, body ErrorResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func (s *Server) writeError(w http.ResponseWriter, status int, code, message string) {
	s.writeJSON(w, status, ErrorResponse{Error: code, Message: message})
}

// withLogging records one line per request.
func (s *Server) withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := s.now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)

		// Health checks are the majority of traffic in any orchestrated
		// deployment and carry no information; logging them buries what matters.
		if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
			return
		}
		s.log.Info("request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", recorder.status,
			"duration_ms", s.now().Sub(start).Milliseconds())
	})
}

// statusRecorder captures the response status for logging.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// Flush forwards to the underlying writer, which SSE streaming requires. Without
// it the wrapper would silently disable streaming — the handler would call
// Flush, find the interface unimplemented, and buffer everything to the end.
func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func specFromRequest(req SubmitRequest) (job.Spec, error) {
	if req.Image == "" && len(req.Command) == 0 {
		return job.Spec{}, errors.New("a job needs an image or a command")
	}
	if req.DeadlineSeconds < 0 {
		return job.Spec{}, errors.New("deadline_seconds must not be negative")
	}
	if req.MaxAttempts < 0 {
		return job.Spec{}, errors.New("max_attempts must not be negative")
	}
	for _, ref := range req.Secrets {
		if ref.Name == "" || ref.Source == "" || ref.Key == "" {
			return job.Spec{}, errors.New("every secret reference needs a name, a source and a key")
		}
	}
	return job.Spec{
		Image:        req.Image,
		Command:      req.Command,
		Input:        req.Input,
		Env:          req.Env,
		Secrets:      req.Secrets,
		Deadline:     time.Duration(req.DeadlineSeconds) * time.Second,
		MaxAttempts:  req.MaxAttempts,
		Resources:    job.Resources{CPUMillis: req.CPUMillis, MemoryMiB: req.MemoryMiB},
		EgressRegion: strings.TrimSpace(req.EgressRegion),
	}, nil
}

func parsePriority(raw string) (job.Priority, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "normal":
		return job.PriorityNormal, nil
	case "low":
		return job.PriorityLow, nil
	case "high":
		return job.PriorityHigh, nil
	}
	return 0, fmt.Errorf("priority must be low, normal or high (got %q)", raw)
}

func pathBase(name string) string {
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		return name[i+1:]
	}
	return name
}

// Serve runs the HTTP server until ctx is cancelled, then shuts down gracefully.
func (s *Server) Serve(ctx context.Context, addr string) error {
	server := &http.Server{
		Addr:    addr,
		Handler: s.Handler(),
		// No WriteTimeout: SSE connections are long-lived by design, and a
		// write timeout would sever the log stream of any job that runs longer
		// than it. ReadHeaderTimeout still guards against slowloris.
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	errs := make(chan error, 1)
	go func() {
		s.log.Info("api listening", "addr", addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs <- err
		}
	}()

	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		s.log.Info("api shutting down")
		return server.Shutdown(shutdownCtx)
	}
}
