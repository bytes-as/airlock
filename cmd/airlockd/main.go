// Command airlockd is the airlock control plane.
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
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/bytes-as/airlock/internal/admission"
	"github.com/bytes-as/airlock/internal/api"
	"github.com/bytes-as/airlock/internal/artifact"
	"github.com/bytes-as/airlock/internal/driver"
	"github.com/bytes-as/airlock/internal/driver/docker"
	"github.com/bytes-as/airlock/internal/driver/process"

	"github.com/bytes-as/airlock/internal/driver/fargate"
	"github.com/bytes-as/airlock/internal/logstream"
	"github.com/bytes-as/airlock/internal/queue/embedded"
	"github.com/bytes-as/airlock/internal/reaper"
	"github.com/bytes-as/airlock/internal/scheduler"
	"github.com/bytes-as/airlock/internal/secrets"
)

// version is stamped at build time with -ldflags "-X main.version=...".
var version = "dev"

type options struct {
	addr                   string
	dataDir                string
	driverName             string
	workers                int
	queueDepth             int
	logFormat              string
	logLevel               string
	baseURL                string
	signingKey             string
	tokens                 string
	secretsDir             string
	envPrefix              string
	dockerHost             string
	dockerNetwork          string
	ecsCluster             string
	jobTaskDefinition      string
	jobContainerName       string
	subnets                string
	securityGroups         string
	assignPublicIP         bool
	artifactBucket         string
	jobLogGroup            string
	egressProxy            string
	egressProxyPool        string
	pullPolicy             string
	submitsPerSecond       float64
	submitBurst            float64
	maxConcurrentPerTenant int
	maxLifetime            time.Duration
	provisionTimeout       time.Duration
	deadline               time.Duration
	maxDeadline            time.Duration
	showVersion            bool
}

func main() {
	opts := parseFlags()

	if opts.showVersion {
		fmt.Printf("airlockd %s\n", version)
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

	flag.StringVar(&o.addr, "addr", envOr("AIRLOCK_ADDR", ":8080"), "address for the HTTP API")
	flag.StringVar(&o.dataDir, "data-dir", envOr("AIRLOCK_DATA_DIR", "./data"), "directory for the queue, environments and artifacts")
	flag.StringVar(&o.driverName, "driver", envOr("AIRLOCK_DRIVER", "process"), "compute driver: process, docker or fargate")
	flag.IntVar(&o.workers, "workers", envIntOr("AIRLOCK_WORKERS", 8), "jobs that may run simultaneously")
	flag.IntVar(&o.queueDepth, "queue-depth", envIntOr("AIRLOCK_QUEUE_DEPTH", 1000), "maximum queued jobs before submissions are refused")
	flag.StringVar(&o.logFormat, "log-format", envOr("AIRLOCK_LOG_FORMAT", "text"), "log format: text or json")
	flag.StringVar(&o.logLevel, "log-level", envOr("AIRLOCK_LOG_LEVEL", "info"), "log level: debug, info, warn or error")
	flag.StringVar(&o.baseURL, "base-url", os.Getenv("AIRLOCK_BASE_URL"), "public origin for artifact links (derived from --addr when empty)")
	flag.StringVar(&o.signingKey, "signing-key", os.Getenv("AIRLOCK_SIGNING_KEY"), "key for signing artifact links; generated if empty, which invalidates old links on restart")
	flag.StringVar(&o.tokens, "tokens", os.Getenv("AIRLOCK_TOKENS"), "comma-separated token=tenant pairs; empty means no authentication")
	flag.StringVar(&o.secretsDir, "secrets-dir", envOr("AIRLOCK_SECRETS_DIR", ""), "directory of secret files for the 'file' source")
	flag.StringVar(&o.envPrefix, "secret-env-prefix", envOr("AIRLOCK_SECRET_ENV_PREFIX", "AIRLOCK_SECRET_"), "prefix for the 'env' secret source")
	flag.DurationVar(&o.provisionTimeout, "provision-timeout", envDurationOr("AIRLOCK_PROVISION_TIMEOUT", 2*time.Minute), "how long an environment may take to provision and start before the job is failed")
	flag.DurationVar(&o.maxLifetime, "max-env-lifetime", envDurationOr("AIRLOCK_MAX_ENV_LIFETIME", time.Hour), "absolute cap on how long any environment may exist")
	flag.DurationVar(&o.deadline, "default-deadline", envDurationOr("AIRLOCK_DEFAULT_DEADLINE", 5*time.Minute), "deadline for jobs that request none")
	flag.DurationVar(&o.maxDeadline, "max-deadline", envDurationOr("AIRLOCK_MAX_DEADLINE", 30*time.Minute), "largest deadline a caller may request")
	flag.StringVar(&o.dockerHost, "docker-host", os.Getenv("DOCKER_HOST"), "docker daemon address (default: DOCKER_HOST or the platform socket)")

	// --- fargate driver. Every one of these is a Terraform output; the runbook
	// maps them one to one, and the driver refuses to start without them
	// rather than failing later on the first job.
	flag.StringVar(&o.egressProxyPool, "egress-proxy-pool", os.Getenv("AIRLOCK_EGRESS_PROXY_POOL"),
		"comma-separated egress proxies to rotate jobs across, each `url` or `region=url` (e.g. \"eu=http://p-eu:8888,us=http://p-us:8888\")")

	flag.StringVar(&o.ecsCluster, "ecs-cluster", os.Getenv("AIRLOCK_ECS_CLUSTER"), "ECS cluster for the fargate driver")
	flag.StringVar(&o.jobTaskDefinition, "job-task-definition", os.Getenv("AIRLOCK_JOB_TASK_DEFINITION"), "ECS task definition family for job tasks")
	flag.StringVar(&o.jobContainerName, "job-container-name", envOr("AIRLOCK_JOB_CONTAINER_NAME", "agent"), "container name inside the job task definition")
	flag.StringVar(&o.subnets, "subnets", os.Getenv("AIRLOCK_SUBNETS"), "comma-separated subnet IDs for job tasks")
	flag.StringVar(&o.securityGroups, "security-groups", envOr("AIRLOCK_SECURITY_GROUP", os.Getenv("AIRLOCK_SECURITY_GROUPS")), "comma-separated security group IDs for job tasks")
	flag.BoolVar(&o.assignPublicIP, "assign-public-ip", os.Getenv("AIRLOCK_ASSIGN_PUBLIC_IP") == "1", "give job tasks a public IP (only for public subnets)")
	flag.StringVar(&o.artifactBucket, "artifact-bucket", os.Getenv("AIRLOCK_ARTIFACT_BUCKET"), "S3 bucket agents upload artifacts to")
	flag.StringVar(&o.jobLogGroup, "job-log-group", os.Getenv("AIRLOCK_JOB_LOG_GROUP"), "CloudWatch Logs group the job task definition writes to")
	flag.StringVar(&o.dockerNetwork, "docker-network", envOr("AIRLOCK_DOCKER_NETWORK", ""), "internal docker network for job containers; required for proxied egress")
	flag.StringVar(&o.egressProxy, "egress-proxy", envOr("AIRLOCK_EGRESS_PROXY", ""), "proxy URL injected into job containers on an internal network")
	flag.StringVar(&o.pullPolicy, "pull-policy", envOr("AIRLOCK_PULL_POLICY", "if-missing"), "image pull policy: always, if-missing or never")
	flag.Float64Var(&o.submitsPerSecond, "submits-per-second", 0, "per-tenant submission rate (0 uses the default)")
	flag.Float64Var(&o.submitBurst, "submit-burst", 0, "per-tenant submission burst (0 uses the default)")
	flag.IntVar(&o.maxConcurrentPerTenant, "max-concurrent-per-tenant", 0, "per-tenant concurrent job cap (0 uses the default)")
	flag.BoolVar(&o.showVersion, "version", false, "print the version and exit")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), `airlockd - control plane for ephemeral job environments

Usage:
  airlockd [flags]

Every flag can also be set by environment variable (shown in its default).

Quick start:
  airlockd --data-dir ./data

Then submit a job:
  airlock submit --command "echo hello"

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
			"Set AIRLOCK_SIGNING_KEY to keep them valid.")
	}
	baseURL := opts.baseURL
	if baseURL == "" {
		// Derive from the listen address rather than hardcoding a port. Getting
		// this wrong is not cosmetic: every artifact link the API hands out
		// would point at a server that is not there.
		baseURL = deriveBaseURL(opts.addr)
		log.Info("base URL derived from listen address", "base_url", baseURL)
	}

	artifacts, err := artifact.NewLocal(
		filepath.Join(dataDir, "artifacts"),
		signingKey,
		artifact.WithBaseURL(baseURL),
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
	schedCfg.ProvisionTimeout = opts.provisionTimeout
	schedCfg.MaxDeadline = opts.maxDeadline
	schedCfg.ArtifactDir = artifactDirFor(drv)

	// Choose the egress posture the driver can actually deliver.
	//
	// This is a composition-root decision on purpose. Each driver enforces a
	// different subset, and the alternative — a scheduler that guesses, or a
	// driver that silently accepts what it cannot honour — is exactly the
	// dishonesty the Capabilities design exists to prevent. So the mapping is
	// written out here, in one place, where it can be read and argued with.
	schedCfg.NetworkMode, schedCfg.DenyCIDRs = egressPostureFor(drv, opts, log)

	// Anything still unenforceable is dropped loudly rather than quietly.
	schedCfg, dropped := schedCfg.RelaxedFor(drv)
	for _, control := range dropped {
		log.Warn("control not enforced by this driver",
			"driver", drv.Name(), "control", control,
			"consequence", "jobs run without this protection")
	}

	broker := logstream.NewBroker()
	limits := admission.DefaultLimits()
	if opts.submitsPerSecond > 0 {
		limits.SubmitsPerSecond = opts.submitsPerSecond
		limits.Burst = opts.submitBurst
	}
	if opts.maxConcurrentPerTenant > 0 {
		limits.MaxConcurrent = opts.maxConcurrentPerTenant
	}
	// Note the quota is NOT clamped to the worker count. They answer different
	// questions: workers bound what this host runs at once, the quota bounds
	// how much of that one tenant may take. Clamping them together would refuse
	// a single tenant's queued work rather than simply queueing it.
	ctrl := admission.New(limits, admission.WithGlobalCapacity(opts.queueDepth))
	log.Info("admission limits",
		"submits_per_second", limits.SubmitsPerSecond,
		"burst", limits.Burst,
		"max_concurrent_per_tenant", limits.MaxConcurrent,
		"queue_capacity", opts.queueDepth)

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
// buildFargateDriver assembles the AWS clients the fargate driver needs.
//
// Credentials come from the default chain, which on ECS means the task role -
// no keys in configuration, and nothing to leak. Region likewise comes from the
// environment AWS itself sets. The driver validates its own configuration, so a
// missing cluster or bucket is refused here at startup with a message naming
// the field, rather than surfacing as a mysterious failure on the first job.
func buildFargateDriver(opts options) (driver.Driver, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration: %w", err)
	}
	if awsCfg.Region == "" {
		return nil, errors.New("no AWS region configured: set AWS_REGION")
	}

	cfg := fargate.DefaultConfig()
	cfg.Cluster = opts.ecsCluster
	cfg.TaskDefinition = opts.jobTaskDefinition
	cfg.ContainerName = opts.jobContainerName
	cfg.Subnets = splitList(opts.subnets)
	cfg.SecurityGroups = splitList(opts.securityGroups)
	cfg.AssignPublicIP = opts.assignPublicIP
	cfg.ArtifactBucket = opts.artifactBucket
	cfg.LogGroup = opts.jobLogGroup
	cfg.UploadURLTTL = opts.maxLifetime + 30*time.Minute

	s3Client := s3.NewFromConfig(awsCfg)
	return fargate.New(
		cfg,
		ecs.NewFromConfig(awsCfg),
		cloudwatchlogs.NewFromConfig(awsCfg),
		fargate.NewS3Store(s3Client, cfg.ArtifactBucket),
	)
}

// parseProxyPool reads "url" or "region=url" entries.
//
// A URL contains "://" and a region label does not, which is what makes the
// two forms distinguishable without a second flag. Malformed entries are
// dropped rather than silently treated as a region named "http", because a
// proxy nobody can reach is worse than one that is absent.
func parseProxyPool(v string) []docker.EgressProxy {
	var pool []docker.EgressProxy
	for _, entry := range strings.Split(v, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		region, url := "", entry
		if name, rest, found := strings.Cut(entry, "="); found && !strings.Contains(name, "://") {
			region, url = strings.TrimSpace(name), strings.TrimSpace(rest)
		}
		if url == "" {
			continue
		}
		pool = append(pool, docker.EgressProxy{URL: url, Region: region})
	}
	return pool
}

// splitList parses a comma-separated flag, ignoring blanks so a trailing comma
// or an empty Terraform output does not become an empty-string subnet ID.
func splitList(v string) []string {
	var out []string
	for _, part := range strings.Split(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func buildDriver(opts options, dataDir string) (driver.Driver, error) {
	switch strings.ToLower(opts.driverName) {
	case "process":
		return process.New(filepath.Join(dataDir, "environments"))

	case "docker":
		cfg := docker.DefaultConfig()
		cfg.Host = opts.dockerHost
		cfg.Network = opts.dockerNetwork
		cfg.ProxyURL = opts.egressProxy
		cfg.ProxyPool = parseProxyPool(opts.egressProxyPool)
		cfg.PullPolicy = opts.pullPolicy
		// Keep artifacts under the data directory rather than the OS temp dir,
		// so everything this daemon writes has one root to inspect and clean.
		cfg.ArtifactRoot = filepath.Join(dataDir, "environments")
		return docker.New(cfg)

	case "fargate":
		return buildFargateDriver(opts)

	default:
		return nil, fmt.Errorf("unknown driver %q (available: process, docker, fargate)", opts.driverName)
	}
}

// artifactDirFor returns the path inside an environment where agents write.
//
// The process driver runs on the host, so it hands the agent an absolute path
// through AIRLOCK_ARTIFACT_DIR and this value is unused. Container drivers
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

// deriveBaseURL turns a listen address into a URL a client can actually reach.
//
// A bare or wildcard host becomes localhost: "0.0.0.0" and "" are things a
// server binds to, not things a browser can resolve, and an artifact link
// pointing at http://0.0.0.0:8080 is a link to nowhere.
func deriveBaseURL(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "http://localhost:8080"
	}
	switch host {
	case "", "0.0.0.0", "::", "[::]":
		host = "localhost"
	}
	return "http://" + net.JoinHostPort(host, port)
}

// egressPostureFor picks the network policy this driver can genuinely enforce,
// and says plainly what the operator is getting.
//
// The three outcomes, worst to best:
//
//   - process driver: no network control at all. The agent shares the host's
//     network, so it can reach cloud metadata and anything else the host can.
//     Acceptable for local development with code you wrote; not for untrusted
//     payloads, and the log says so.
//
//   - docker driver with no internal network: a bridge network with internet
//     access. Docker cannot apply a CIDR deny list without host firewall rules
//     it does not own, so no deny list is claimed.
//
//   - docker driver with an internal network: proxied. The container has no
//     route off the network, so instance metadata is unreachable by
//     construction rather than by a rule that could be mis-specified, and the
//     proxy is the only way out.
func egressPostureFor(d driver.Driver, opts options, log *slog.Logger) (driver.NetworkMode, []string) {
	if !d.Capabilities().EnforcesNetworkPolicy {
		log.Warn("this driver enforces no network isolation",
			"driver", d.Name(),
			"consequence", "agents can reach cloud metadata and host services",
			"remedy", "use --driver docker with --docker-network for isolation")
		return driver.NetworkEgress, nil
	}

	if opts.dockerNetwork == "" {
		log.Warn("no internal network configured; containers get ordinary bridge networking",
			"consequence", "egress is unrestricted, including to instance metadata",
			"remedy", "set --docker-network (and --egress-proxy) to isolate egress")
		return driver.NetworkEgress, nil
	}

	if opts.egressProxy == "" {
		// The internal network still blocks egress structurally, but without a
		// proxy the agent has no way out at all. That is a legitimate choice
		// for offline work, and a surprise for anything that needs the web.
		log.Warn("internal network configured without an egress proxy",
			"network", opts.dockerNetwork,
			"consequence", "agents have no internet access at all",
			"remedy", "set --egress-proxy to allow controlled outbound traffic")
	} else {
		log.Info("egress isolated by internal network with a proxy",
			"network", opts.dockerNetwork,
			"proxy", opts.egressProxy,
			"metadata_reachable", false)
	}
	return driver.NetworkProxied, nil
}
