// Command ephemerad is the ephemera control plane.
//
// It is the composition root: every wiring decision, every default, and every
// deliberate downgrade is made here and nowhere else. The packages below it
// take their collaborators as parameters and make no global choices, which is
// what makes them testable and what makes this file worth reading first.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"ephemera/internal/admission"
	"ephemera/internal/api"
	"ephemera/internal/artifact"
	"ephemera/internal/driver"
	"ephemera/internal/driver/process"
	"ephemera/internal/logstream"
	"ephemera/internal/queue/embedded"
	"ephemera/internal/reaper"
	"ephemera/internal/scheduler"
	"ephemera/internal/secrets"
)

// version is stamped at build time with -ldflags "-X main.version=...".
var version = "dev"

type options struct {
	addr        string
	dataDir     string
	driverName  string
	workers     int
	queueDepth  int
	logFormat   string
	logLevel    string
	baseURL     string
	signingKey  string
	tokens      string
	secretsDir  string
	envPrefix   string
	maxLifetime time.Duration
	deadline    time.Duration
	maxDeadline time.Duration
	showVersion bool
}

func main() {
	opts := parseFlags()

	if opts.showVersion {
		fmt.Printf("ephemerad %s\n", version)
		return
	}

	log := newLogger(opts)

	if err := run(opts, log); err != nil {
		log.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func parseFlags() options {
	var o options

	flag.StringVar(&o.addr, "addr", envOr("EPHEMERA_ADDR", ":8080"), "address for the HTTP API")
	flag.StringVar(&o.dataDir, "data-dir", envOr("EPHEMERA_DATA_DIR", "./data"), "directory for the queue, environments and artifacts")
	flag.StringVar(&o.driverName, "driver", envOr("EPHEMERA_DRIVER", "process"), "compute driver: process")
	flag.IntVar(&o.workers, "workers", envIntOr("EPHEMERA_WORKERS", 8), "jobs that may run simultaneously")
	flag.IntVar(&o.queueDepth, "queue-depth", envIntOr("EPHEMERA_QUEUE_DEPTH", 1000), "maximum queued jobs before submissions are refused")
	flag.StringVar(&o.logFormat, "log-format", envOr("EPHEMERA_LOG_FORMAT", "text"), "log format: text or json")
	flag.StringVar(&o.logLevel, "log-level", envOr("EPHEMERA_LOG_LEVEL", "info"), "log level: debug, info, warn or error")
	flag.StringVar(&o.baseURL, "base-url", envOr("EPHEMERA_BASE_URL", "http://localhost:8080"), "public origin used when signing artifact links")
	flag.StringVar(&o.signingKey, "signing-key", os.Getenv("EPHEMERA_SIGNING_KEY"), "key for signing artifact links; generated if empty, which invalidates old links on restart")
	flag.StringVar(&o.tokens, "tokens", os.Getenv("EPHEMERA_TOKENS"), "comma-separated token=tenant pairs; empty means no authentication")
	flag.StringVar(&o.secretsDir, "secrets-dir", envOr("EPHEMERA_SECRETS_DIR", ""), "directory of secret files for the 'file' source")
	flag.StringVar(&o.envPrefix, "secret-env-prefix", envOr("EPHEMERA_SECRET_ENV_PREFIX", "EPHEMERA_SECRET_"), "prefix for the 'env' secret source")
	flag.DurationVar(&o.maxLifetime, "max-env-lifetime", envDurationOr("EPHEMERA_MAX_ENV_LIFETIME", time.Hour), "absolute cap on how long any environment may exist")
	flag.DurationVar(&o.deadline, "default-deadline", envDurationOr("EPHEMERA_DEFAULT_DEADLINE", 5*time.Minute), "deadline for jobs that request none")
	flag.DurationVar(&o.maxDeadline, "max-deadline", envDurationOr("EPHEMERA_MAX_DEADLINE", 30*time.Minute), "largest deadline a caller may request")
	flag.BoolVar(&o.showVersion, "version", false, "print the version and exit")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), `ephemerad - control plane for ephemeral job environments

Usage:
  ephemerad [flags]

Every flag can also be set by environment variable (shown in its default).

Quick start:
  ephemerad --data-dir ./data

Then submit a job:
  ephemera submit --command "echo hello"

`)
		flag.PrintDefaults()
	}
	flag.Parse()
	return o
}

func run(opts options, log *slog.Logger) error {
	if opts.workers <= 0 {
		return fmt.Errorf("--workers must be positive, got %d", opts.workers)
	}

	dataDir, err := filepath.Abs(opts.dataDir)
	if err != nil {
		return fmt.Errorf("resolve data dir: %w", err)
	}
	if err := os.MkdirAll(dataDir, 0o750); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	log.Info("starting", "version", version, "data_dir", dataDir)

	// --- storage ---

	q, err := embedded.Open(filepath.Join(dataDir, "queue.db"), embedded.WithCapacity(opts.queueDepth))
	if err != nil {
		return err
	}
	defer q.Close()

	signingKey := []byte(opts.signingKey)
	if len(signingKey) == 0 {
		log.Warn("no signing key configured; artifact links will not survive a restart. " +
			"Set EPHEMERA_SIGNING_KEY to keep them valid.")
	}
	artifacts, err := artifact.NewLocal(
		filepath.Join(dataDir, "artifacts"),
		signingKey,
		artifact.WithBaseURL(opts.baseURL),
	)
	if err != nil {
		return err
	}

	// --- driver ---

	drv, err := buildDriver(opts, dataDir)
	if err != nil {
		return err
	}
	caps := drv.Capabilities()
	log.Info("driver ready",
		"driver", drv.Name(),
		"isolation", caps.Isolation.String(),
		"enforces_network_policy", caps.EnforcesNetworkPolicy,
		"enforces_resource_limits", caps.EnforcesResourceLimits)

	// --- scheduler ---

	schedCfg := scheduler.DefaultConfig()
	schedCfg.Workers = opts.workers
	schedCfg.DefaultDeadline = opts.deadline
	schedCfg.MaxDeadline = opts.maxDeadline
	schedCfg.ArtifactDir = artifactDirFor(drv)

	// Downgrade deliberately and say so. Silently dropping a security control
	// is the one behaviour this system refuses; announcing it is the price of
	// letting the process driver run at all.
	schedCfg, dropped := schedCfg.RelaxedFor(drv)
	for _, control := range dropped {
		log.Warn("control not enforced by this driver",
			"driver", drv.Name(), "control", control,
			"consequence", "jobs run without this protection")
	}

	broker := logstream.NewBroker()
	limits := admission.DefaultLimits()
	limits.MaxConcurrent = opts.workers
	ctrl := admission.New(limits, admission.WithGlobalCapacity(opts.queueDepth))

	resolver := secrets.NewChain(buildSecretSources(opts)...)

	sched, err := scheduler.New(schedCfg, scheduler.Deps{
		Queue:     q,
		Driver:    drv,
		Admission: ctrl,
		Secrets:   resolver,
		Artifacts: artifacts,
		Logs:      broker,
		Logger:    log.With("component", "scheduler"),
	})
	if err != nil {
		return err
	}

	// --- reaper ---

	reaperCfg := reaper.DefaultConfig()
	reaperCfg.MaxLifetime = opts.maxLifetime
	sweeper, err := reaper.New(reaperCfg, reaper.Deps{
		Driver: drv,
		Queue:  q,
		Logger: log.With("component", "reaper"),
	})
	if err != nil {
		return err
	}

	// --- api ---

	apiCfg := api.DefaultConfig()
	apiCfg.Tokens, err = parseTokens(opts.tokens)
	if err != nil {
		return err
	}
	server, err := api.NewServer(apiCfg, api.Deps{
		Queue:     q,
		Admission: ctrl,
		Artifacts: artifacts,
		Logs:      broker,
		Scheduler: sched,
		Logger:    log.With("component", "api"),
	})
	if err != nil {
		return err
	}

	// --- run until signalled ---

	// NotifyContext rather than a manual signal handler: a second Ctrl-C
	// restores default behaviour and kills the process, so an operator is never
	// stuck waiting on a shutdown that has itself wedged.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	errs := make(chan error, 3)

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := sched.Run(ctx); err != nil {
			errs <- fmt.Errorf("scheduler: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := sweeper.Run(ctx); err != nil {
			errs <- fmt.Errorf("reaper: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := server.Serve(ctx, opts.addr); err != nil {
			errs <- fmt.Errorf("api: %w", err)
		}
	}()

	log.Info("ready",
		"addr", opts.addr,
		"workers", opts.workers,
		"driver", drv.Name(),
		"authentication", len(apiCfg.Tokens) > 0)

	wg.Wait()
	close(errs)

	// Report the first real failure, if any. Shutdown itself is not an error.
	for err := range errs {
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
	}
	log.Info("stopped cleanly")
	return nil
}

// buildDriver selects the compute driver.
func buildDriver(opts options, dataDir string) (driver.Driver, error) {
	switch strings.ToLower(opts.driverName) {
	case "process":
		return process.New(filepath.Join(dataDir, "environments"))
	default:
		return nil, fmt.Errorf("unknown driver %q (available: process)", opts.driverName)
	}
}

// artifactDirFor returns the path inside an environment where agents write.
//
// The process driver runs on the host, so it hands the agent an absolute path
// through EPHEMERA_ARTIFACT_DIR and this value is unused. Container drivers
// mount a fixed path instead.
func artifactDirFor(d driver.Driver) string {
	if d.Name() == "process" {
		return ""
	}
	return "/artifacts"
}

// buildSecretSources assembles the secret backends.
func buildSecretSources(opts options) []secrets.Source {
	sources := []secrets.Source{
		&secrets.EnvSource{Prefix: opts.envPrefix},
	}
	if opts.secretsDir != "" {
		sources = append(sources, &secrets.FileSource{Root: opts.secretsDir})
	}
	return sources
}

// parseTokens reads "token=tenant,token2=tenant2".
func parseTokens(raw string) (map[string]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	out := map[string]string{}
	for _, pair := range strings.Split(raw, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		token, tenant, ok := strings.Cut(pair, "=")
		if !ok || token == "" || tenant == "" {
			// Never echo the raw value: it contains credentials, and this
			// error is going straight into a log or a terminal.
			return nil, errors.New("--tokens entries must look like token=tenant")
		}
		out[token] = tenant
	}
	return out, nil
}

func newLogger(opts options) *slog.Logger {
	level := slog.LevelInfo
	switch strings.ToLower(opts.logLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}

	handlerOpts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler = slog.NewTextHandler(os.Stderr, handlerOpts)
	if strings.EqualFold(opts.logFormat, "json") {
		handler = slog.NewJSONHandler(os.Stderr, handlerOpts)
	}
	return slog.New(handler)
}

func envOr(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

func envIntOr(key string, fallback int) int {
	if v, ok := os.LookupEnv(key); ok {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
	}
	return fallback
}

func envDurationOr(key string, fallback time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
