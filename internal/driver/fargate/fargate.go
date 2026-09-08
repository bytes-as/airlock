// Package fargate runs each job as an ECS Fargate task.
//
// This is the AWS path. It satisfies the same Driver contract as the docker
// driver, which is the point of having a contract at all: the scheduler, the
// reaper, the failure taxonomy and the API are identical on both, and only this
// file knows what a task ARN is.
//
// # Read this before trusting it
//
// This driver has never been executed against AWS. It compiles, it is unit
// tested against a fake ECS/S3/logs layer, and it was written carefully - and
// none of that is the same as having run. The project's own history is the
// argument for saying so plainly: the docker driver was also "written and
// compiles" for a while, and the first contact with a real daemon found that
// artifacts could never be collected at all. Assume this file has bugs of the
// same character until someone has watched a job run in a real account. The
// runbook says exactly how to do that.
//
// # Three places this differs from Docker, and why
//
//  1. There is no create-without-start. ECS RunTask launches the task; there is
//     no API to build one and hold it. So Create calls RunTask and Start waits
//     for the task to reach RUNNING. The split is preserved because the failure
//     taxonomy depends on it - a task that never places is a provisioning
//     failure, a task that places and immediately dies is a start failure - but
//     the agent may begin work fractionally before Start is called. Pretending
//     otherwise would be a lie in the shape of an abstraction.
//
//  2. Artifacts come out through S3, not a filesystem. There is no `docker cp`
//     for a Fargate task. The agent tars its artifact directory and PUTs it to
//     a presigned URL this driver generates per job; Collect downloads and
//     extracts it. The presigned URL matters: it means the job task needs no
//     AWS credentials and no S3 permission, so the deliberately powerless task
//     role in the Terraform stays powerless. The cost is honest and real - an
//     agent that dies hard never uploads, so a crashed job yields no artifacts,
//     which is exactly when they would have been most useful. A sidecar that
//     uploads on exit is the fix, and it is named in the README rather than
//     pretended away.
//
//  3. Identity lives in tags, not memory. Every task is tagged with the job,
//     tenant and expiry, and started with a constant startedBy. That is what
//     lets List find tasks this process did not create, which is what makes
//     reaping survive a control-plane crash.
package fargate

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"

	"github.com/bytes-as/ephemera/internal/driver"
	"github.com/bytes-as/ephemera/internal/driver/archive"
)

// Tag keys applied to every task, so environments stay identifiable as ours
// after the control plane that created them is gone. ECS tags, unlike Docker
// labels, are also what the cost explorer groups by.
const (
	TagManaged = "ephemera:managed"
	TagJob     = "ephemera:job"
	TagTenant  = "ephemera:tenant"
	TagExpires = "ephemera:expires-at"
)

// startedBy is constant across every job task. ECS lets ListTasks filter on it
// exactly (not by prefix), so a single shared value is what makes "list only
// our job tasks" one API call rather than a scan. Per-job identity is carried
// in tags. Max 36 characters, per the ECS API.
const startedBy = "ephemera"

// artifactObjectKey is where the agent uploads and Collect reads.
func artifactObjectKey(jobID string) string { return "jobs/" + jobID + "/artifacts.tar" }

// ECSAPI is the slice of ECS this driver uses. Narrow on purpose: it is the
// seam the tests fake, and a small interface is one that can be faked honestly.
type ECSAPI interface {
	RunTask(context.Context, *ecs.RunTaskInput, ...func(*ecs.Options)) (*ecs.RunTaskOutput, error)
	StopTask(context.Context, *ecs.StopTaskInput, ...func(*ecs.Options)) (*ecs.StopTaskOutput, error)
	DescribeTasks(context.Context, *ecs.DescribeTasksInput, ...func(*ecs.Options)) (*ecs.DescribeTasksOutput, error)
	ListTasks(context.Context, *ecs.ListTasksInput, ...func(*ecs.Options)) (*ecs.ListTasksOutput, error)
	DescribeTaskDefinition(context.Context, *ecs.DescribeTaskDefinitionInput, ...func(*ecs.Options)) (*ecs.DescribeTaskDefinitionOutput, error)
}

// LogsAPI is the slice of CloudWatch Logs this driver uses.
type LogsAPI interface {
	GetLogEvents(context.Context, *cloudwatchlogs.GetLogEventsInput, ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.GetLogEventsOutput, error)
}

// ObjectStore is the slice of S3 this driver uses: one presigned upload URL per
// job, and one download to collect. Kept as an interface so the driver can be
// tested without S3 and so the presigning strategy can change without touching
// the lifecycle code.
type ObjectStore interface {
	// PresignPut returns a URL the agent can PUT to with no credentials.
	PresignPut(ctx context.Context, key string, expires time.Duration) (string, error)
	// Get returns the object body, or ErrNoArtifacts if it was never uploaded.
	Get(ctx context.Context, key string) (body []byte, err error)
}

// ErrNoArtifacts means the agent never uploaded. Not a failure: a job that
// produced nothing and a job that crashed before uploading look the same from
// here, and neither is an error in Collect.
var ErrNoArtifacts = errors.New("fargate driver: no artifact object")

// Config tunes the driver. Everything here comes from the Terraform outputs;
// the runbook maps them one to one.
type Config struct {
	// Cluster is the ECS cluster name or ARN.
	Cluster string

	// TaskDefinition is the job task definition family (or family:revision).
	// Passing a family alone means ECS uses the latest ACTIVE revision, which
	// is what makes a Terraform change take effect without redeploying us.
	TaskDefinition string

	// ContainerName must match the container in that task definition; ECS
	// requires it to address environment overrides at it.
	ContainerName string

	// Subnets and SecurityGroups place the task's ENI. awsvpc networking is
	// mandatory on Fargate, so these are required rather than optional.
	Subnets        []string
	SecurityGroups []string

	// AssignPublicIP is false for private subnets behind a NAT gateway, which
	// is what the Terraform builds. True only if you place tasks in public
	// subnets, which costs you the "no inbound path" property.
	AssignPublicIP bool

	// LogGroup is the CloudWatch Logs group the task definition writes to, and
	// LogStreamPrefix is its awslogs-stream-prefix. Together with the task ID
	// they give the stream name; getting either wrong yields a job that runs
	// fine and streams nothing.
	LogGroup        string
	LogStreamPrefix string

	// ArtifactBucket receives the agent's uploaded tarball.
	ArtifactBucket string

	// UploadURLTTL bounds how long the presigned upload URL stays valid. It
	// must outlive the job's deadline or a long job cannot upload at the end;
	// it should not outlive it by much, because the URL is a bearer token for
	// writing to that key.
	UploadURLTTL time.Duration

	// PollInterval is how often Wait and Logs poll. ECS has no push
	// notification for task state, so this is the price of knowing.
	PollInterval time.Duration
}

// DefaultConfig returns settings suitable for the Terraform in deploy/.
func DefaultConfig() Config {
	return Config{
		ContainerName:   "agent",
		LogStreamPrefix: "job",
		UploadURLTTL:    2 * time.Hour,
		PollInterval:    2 * time.Second,
	}
}

// Validate refuses a configuration that would fail later, in a place where the
// error would be harder to read.
func (c Config) Validate() error {
	switch {
	case c.Cluster == "":
		return errors.New("fargate driver: Cluster is required")
	case c.TaskDefinition == "":
		return errors.New("fargate driver: TaskDefinition is required")
	case c.ContainerName == "":
		return errors.New("fargate driver: ContainerName is required")
	case len(c.Subnets) == 0:
		return errors.New("fargate driver: at least one subnet is required (awsvpc networking is mandatory on Fargate)")
	case c.LogGroup == "":
		return errors.New("fargate driver: LogGroup is required, or jobs would run with no way to see inside them")
	case c.ArtifactBucket == "":
		return errors.New("fargate driver: ArtifactBucket is required, or artifacts have nowhere to go")
	case c.PollInterval <= 0:
		return errors.New("fargate driver: PollInterval must be positive")
	}
	return nil
}

// Driver runs each job as a Fargate task.
type Driver struct {
	cfg     Config
	ecs     ECSAPI
	logs    LogsAPI
	objects ObjectStore
	now     func() time.Time

	// mu guards the caches below.
	//
	// uploadURLs holds the presigned URL between Create and Start so the same
	// job does not get two different upload destinations. logCfg caches the log
	// configuration read from the task definition, which does not change for a
	// given revision and is not worth an API call per log stream.
	mu         sync.Mutex
	uploadURLs map[string]string
	logCfg     *logConfig
}

// logConfig is where a job's output actually goes, as declared by the task
// definition rather than as assumed by our configuration.
type logConfig struct {
	group     string
	prefix    string
	container string
}

// Option adjusts a Driver, used by tests to inject fakes and a clock.
type Option func(*Driver)

// WithClock replaces time.Now, so deadline behaviour is testable without
// sleeping.
func WithClock(now func() time.Time) Option {
	return func(d *Driver) { d.now = now }
}

// New builds a driver from an already-configured set of AWS clients.
func New(cfg Config, ecsAPI ECSAPI, logsAPI LogsAPI, objects ObjectStore, opts ...Option) (*Driver, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	d := &Driver{
		cfg:        cfg,
		ecs:        ecsAPI,
		logs:       logsAPI,
		objects:    objects,
		now:        time.Now,
		uploadURLs: map[string]string{},
	}
	for _, opt := range opts {
		opt(d)
	}
	return d, nil
}

func (d *Driver) Name() string { return "fargate" }

// Capabilities reports what this driver enforces.
//
// Isolation is microVM: a Fargate task gets its own kernel, which is a stronger
// boundary than the docker driver's shared-kernel namespaces and the one place
// the AWS path is genuinely better than the local one.
//
// EnforcesNetworkPolicy is true, and Create is strict about which policies it
// accepts rather than accepting all of them and silently enforcing none.
func (d *Driver) Capabilities() driver.Capabilities {
	return driver.Capabilities{
		Isolation:              driver.IsolationMicroVM,
		EnforcesResourceLimits: true,
		EnforcesNetworkPolicy:  true,
		// List reads from ECS, not from memory, so a new control-plane process
		// finds tasks an old one started. This is what lets the reaper recover
		// orphans after a crash.
		SurvivesControlPlaneRestart: true,
	}
}

// checkNetwork refuses policies Fargate cannot actually deliver.
//
// The honesty rule, applied to a different runtime:
//
//   - NetworkNone is refused. awsvpc always attaches an ENI; there is no
//     "no network" task. A caller asking for no network would otherwise get a
//     task with full egress and believe it was isolated, which is the worst
//     possible outcome. The docker driver can honour this; say so, and let the
//     caller choose the driver that can.
//
//   - NetworkEgress is accepted, with a caveat stated here rather than hidden:
//     the deny list is enforced by the task's security group, which is
//     infrastructure this driver does not own and cannot verify at runtime.
//     Worse, security groups cannot filter link-local at all, so the Fargate
//     task metadata endpoint at 169.254.170.2 is reachable from inside the
//     task no matter what the deny list says. That endpoint serves the task
//     role's credentials. The mitigation is therefore not a network control at
//     all: it is that the Terraform gives job tasks a role with no permissions,
//     so the credentials it hands out can do nothing. See iam.tf.
//
//   - NetworkProxied is accepted and implemented by injecting proxy variables,
//     the same as the docker driver.
func (d *Driver) checkNetwork(policy driver.NetworkPolicy) error {
	switch policy.Mode {
	case driver.NetworkNone:
		return &driver.UnsupportedError{
			Driver:  d.Name(),
			Feature: "network isolation (awsvpc always attaches an ENI; use the docker driver for a truly network-less environment)",
		}
	case driver.NetworkEgress, driver.NetworkProxied, "":
		return nil
	default:
		return &driver.UnsupportedError{Driver: d.Name(), Feature: string(policy.Mode)}
	}
}

// Create launches the task.
//
// Named Create rather than Start because of what a failure here means: the task
// could not be placed at all - no capacity, a bad task definition, a subnet
// with no route to pull the image - and none of that ran the agent, so a retry
// is safe. Start's failures are the ones where something did run.
func (d *Driver) Create(ctx context.Context, spec driver.EnvSpec) (driver.Env, error) {
	if spec.Image != "" && spec.Image != "-" {
		// The image is baked into the task definition, which is Terraform's to
		// own. Accepting a per-job image here would silently ignore it.
		//
		// This is a real limitation of the current shape rather than a
		// principle: supporting per-job images means registering a task
		// definition revision per image, which is a bounded piece of work
		// named in the README as the next step.
		return driver.Env{}, &driver.UnsupportedError{
			Driver:  d.Name(),
			Feature: fmt.Sprintf("per-job image %q (the task definition %q fixes the image; see the README)", spec.Image, d.cfg.TaskDefinition),
		}
	}
	if err := d.checkNetwork(spec.Network); err != nil {
		return driver.Env{}, err
	}

	now := d.now()
	var expires time.Time
	if spec.Deadline > 0 {
		expires = now.Add(spec.Deadline)
	}

	// Presign before launching: if this fails the job has not started, which is
	// a cleaner failure than a running agent with nowhere to put its output.
	uploadURL, err := d.presignUpload(ctx, spec.JobID, spec.Deadline)
	if err != nil {
		return driver.Env{}, err
	}

	env, err := d.buildEnvironment(spec, uploadURL)
	if err != nil {
		return driver.Env{}, err
	}

	assign := ecstypes.AssignPublicIpDisabled
	if d.cfg.AssignPublicIP {
		assign = ecstypes.AssignPublicIpEnabled
	}

	tags := []ecstypes.Tag{
		{Key: aws.String(TagManaged), Value: aws.String("true")},
		{Key: aws.String(TagJob), Value: aws.String(spec.JobID)},
		{Key: aws.String(TagTenant), Value: aws.String(spec.TenantID)},
	}
	if !expires.IsZero() {
		// Written on the task itself so the reaper can enforce a deadline for a
		// job whose record it cannot find.
		tags = append(tags, ecstypes.Tag{
			Key:   aws.String(TagExpires),
			Value: aws.String(expires.UTC().Format(time.RFC3339)),
		})
	}

	out, err := d.ecs.RunTask(ctx, &ecs.RunTaskInput{
		Cluster:        aws.String(d.cfg.Cluster),
		TaskDefinition: aws.String(d.cfg.TaskDefinition),
		LaunchType:     ecstypes.LaunchTypeFargate,
		Count:          aws.Int32(1),
		StartedBy:      aws.String(startedBy),
		// Tags must propagate nowhere else: PropagateTags from the task
		// definition would overwrite the per-job tags set here.
		Tags: tags,
		NetworkConfiguration: &ecstypes.NetworkConfiguration{
			AwsvpcConfiguration: &ecstypes.AwsVpcConfiguration{
				Subnets:        d.cfg.Subnets,
				SecurityGroups: d.cfg.SecurityGroups,
				AssignPublicIp: assign,
			},
		},
		Overrides: &ecstypes.TaskOverride{
			ContainerOverrides: []ecstypes.ContainerOverride{{
				Name:        aws.String(d.cfg.ContainerName),
				Command:     spec.Command,
				Environment: env,
			}},
		},
	})
	if err != nil {
		return driver.Env{}, fmt.Errorf("fargate driver: run task: %w", err)
	}

	// A RunTask that returns 200 with failures is the case that looks like
	// success and is not: no capacity, no matching container instance, a
	// subnet that cannot be used. Surfacing it as an error is what makes it a
	// provisioning failure rather than a job that mysteriously never runs.
	if len(out.Failures) > 0 {
		f := out.Failures[0]
		return driver.Env{}, fmt.Errorf("fargate driver: run task refused: %s (%s)",
			aws.ToString(f.Reason), aws.ToString(f.Detail))
	}
	if len(out.Tasks) == 0 {
		return driver.Env{}, errors.New("fargate driver: run task returned neither a task nor a failure")
	}

	return driver.Env{
		ID:        aws.ToString(out.Tasks[0].TaskArn),
		Driver:    d.Name(),
		JobID:     spec.JobID,
		TenantID:  spec.TenantID,
		CreatedAt: now,
		ExpiresAt: expires,
	}, nil
}

// presignUpload produces the URL the agent PUTs its artifact tarball to, and
// remembers it so Start and Create agree.
func (d *Driver) presignUpload(ctx context.Context, jobID string, deadline time.Duration) (string, error) {
	ttl := d.cfg.UploadURLTTL
	// The URL must outlive the job, or a job that runs to its deadline finds a
	// dead URL at the exact moment it has something to upload.
	if deadline > 0 && deadline+10*time.Minute > ttl {
		ttl = deadline + 10*time.Minute
	}
	url, err := d.objects.PresignPut(ctx, artifactObjectKey(jobID), ttl)
	if err != nil {
		return "", fmt.Errorf("fargate driver: presign artifact upload: %w", err)
	}
	d.mu.Lock()
	d.uploadURLs[jobID] = url
	d.mu.Unlock()
	return url, nil
}

// buildEnvironment assembles the container's environment overrides.
//
// Secrets are injected here, and that carries a caveat worth stating in the
// code rather than only in a document: container overrides are visible to
// anyone holding ecs:DescribeTasks on this cluster. The docker driver's
// equivalent exposure is `docker inspect`, which is local; this one is an IAM
// grant. The mitigation is that the Terraform grants DescribeTasks only to the
// control plane's own role - but a reader deploying this should know the
// property they are relying on. The stronger answer is Secrets Manager
// references in the task definition, which only works for values known before
// the job exists, and ephemera resolves secrets per job at submit time.
func (d *Driver) buildEnvironment(spec driver.EnvSpec, uploadURL string) ([]ecstypes.KeyValuePair, error) {
	merged := map[string]string{}
	for k, v := range spec.Env {
		merged[k] = v
	}

	artifactDir := spec.ArtifactDir
	if artifactDir == "" {
		artifactDir = "/artifacts"
	}
	merged["EPHEMERA_ARTIFACT_DIR"] = artifactDir
	merged["EPHEMERA_JOB_ID"] = spec.JobID
	merged["EPHEMERA_TENANT_ID"] = spec.TenantID
	// The agent uploads here on exit. Its presence is also how the agent knows
	// it is running somewhere without a filesystem the control plane can read.
	merged["EPHEMERA_ARTIFACT_UPLOAD_URL"] = uploadURL

	if spec.Network.Mode == driver.NetworkProxied && spec.Network.ProxyURL != "" {
		for _, k := range []string{"HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"} {
			merged[k] = spec.Network.ProxyURL
		}
	}

	for name, secret := range spec.Secrets {
		if _, clash := merged[name]; clash {
			// Refusing beats silently letting a plain variable shadow a secret
			// or the reverse; either way the job would run with something other
			// than what was asked for.
			return nil, fmt.Errorf("fargate driver: %q is both an environment variable and a secret", name)
		}
		// Reveal is the only way out of job.Secret, which redacts itself through
		// String, GoString and MarshalJSON. Calling it here, at the last possible
		// moment, is deliberate: the value exists as a plain string only inside
		// this call and is never stored on the Driver or logged.
		merged[name] = secret.Reveal()
	}

	// Sorted so a task definition diff is stable and reviewable.
	names := make([]string, 0, len(merged))
	for k := range merged {
		names = append(names, k)
	}
	sort.Strings(names)

	pairs := make([]ecstypes.KeyValuePair, 0, len(names))
	for _, k := range names {
		pairs = append(pairs, ecstypes.KeyValuePair{Name: aws.String(k), Value: aws.String(merged[k])})
	}
	return pairs, nil
}

// Start waits for the task to be running.
//
// ECS has no create-then-start, so by the time this is called the task is
// already on its way up. What Start adds is the distinction the failure
// taxonomy needs: a task that reaches RUNNING started, and a task that goes
// straight to STOPPED did not.
func (d *Driver) Start(ctx context.Context, env driver.Env, spec driver.EnvSpec) error {
	ticker := time.NewTicker(d.cfg.PollInterval)
	defer ticker.Stop()

	for {
		task, err := d.describeTask(ctx, env.ID)
		if err != nil {
			return err
		}
		if task == nil {
			return fmt.Errorf("fargate driver: task %s disappeared before it started", env.ID)
		}

		switch aws.ToString(task.LastStatus) {
		case "RUNNING":
			return nil
		case "STOPPED", "DEACTIVATING", "STOPPING", "DEPROVISIONING":
			// It stopped without ever running. The reason is the useful part:
			// CannotPullContainerError and friends live here, and they are the
			// difference between "your image is wrong" and "we are broken".
			return fmt.Errorf("fargate driver: task stopped before running: %s", taskFailureReason(task))
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Wait blocks until the task stops and reports how it went.
func (d *Driver) Wait(ctx context.Context, env driver.Env) (driver.Status, error) {
	ticker := time.NewTicker(d.cfg.PollInterval)
	defer ticker.Stop()

	for {
		status, done, err := d.pollStatus(ctx, env)
		if err != nil {
			return driver.Status{}, err
		}
		if done {
			return status, nil
		}
		select {
		case <-ctx.Done():
			return driver.Status{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

// Describe reports the current state, including of a task this process did not
// create.
func (d *Driver) Describe(ctx context.Context, env driver.Env) (driver.Status, error) {
	status, _, err := d.pollStatus(ctx, env)
	return status, err
}

func (d *Driver) pollStatus(ctx context.Context, env driver.Env) (driver.Status, bool, error) {
	task, err := d.describeTask(ctx, env.ID)
	if err != nil {
		return driver.Status{}, false, err
	}
	if task == nil {
		// A task ECS no longer knows about is gone, not an error. Stopped tasks
		// are only retained for about an hour, so this is the ordinary end
		// state for anything the control plane looks at late - and an
		// environment that is gone costs nothing, which is the property the
		// reaper cares about.
		return driver.Status{Phase: driver.PhaseGone}, true, nil
	}
	return statusOf(task), aws.ToString(task.LastStatus) == "STOPPED", nil
}

// statusOf translates an ECS task into the driver's vocabulary.
func statusOf(task *ecstypes.Task) driver.Status {
	s := driver.Status{Reason: taskFailureReason(task)}
	if task.StartedAt != nil {
		s.StartedAt = *task.StartedAt
	}
	if task.StoppedAt != nil {
		s.ExitedAt = *task.StoppedAt
	}

	switch aws.ToString(task.LastStatus) {
	case "PROVISIONING", "PENDING", "ACTIVATING":
		s.Phase = driver.PhaseCreating
		return s
	case "RUNNING":
		s.Phase = driver.PhaseRunning
		return s
	case "STOPPED":
		s.Phase = driver.PhaseExited
	default:
		s.Phase = driver.PhaseRunning
		return s
	}

	container := agentContainer(task)
	if container != nil && container.ExitCode != nil {
		s.ExitCode = int(*container.ExitCode)
	} else {
		// No exit code means it never ran the entrypoint - it failed to pull,
		// or was killed before start. Reporting 0 here would read as success.
		s.ExitCode = -1
	}

	// OOM is a distinct outcome, not a crash: it means the spec asked for too
	// little memory, which is a different bug with a different owner.
	reason := strings.ToLower(taskFailureReason(task))
	if strings.Contains(reason, "outofmemory") || strings.Contains(reason, "out of memory") {
		s.OOMKilled = true
	}
	// A task stopped by us or by the infrastructure backstop carries a stopped
	// reason naming a timeout rather than an agent exit.
	if strings.Contains(reason, "deadline") || strings.Contains(reason, "timed out") {
		s.DeadlineExceeded = true
	}
	return s
}

// agentContainer finds the container the agent runs in. Fargate tasks may carry
// sidecars, and the essential container's exit code is the task's outcome.
func agentContainer(task *ecstypes.Task) *ecstypes.Container {
	for i := range task.Containers {
		c := &task.Containers[i]
		if c.ExitCode != nil || aws.ToString(c.Reason) != "" {
			return c
		}
	}
	if len(task.Containers) > 0 {
		return &task.Containers[0]
	}
	return nil
}

// taskFailureReason assembles the most useful explanation ECS offers. The
// container's reason is more specific than the task's when both exist.
func taskFailureReason(task *ecstypes.Task) string {
	var parts []string
	if v := aws.ToString(task.StoppedReason); v != "" {
		parts = append(parts, v)
	}
	if v := string(task.StopCode); v != "" {
		parts = append(parts, v)
	}
	for i := range task.Containers {
		if v := aws.ToString(task.Containers[i].Reason); v != "" {
			parts = append(parts, v)
		}
	}
	return strings.Join(parts, "; ")
}

func (d *Driver) describeTask(ctx context.Context, taskARN string) (*ecstypes.Task, error) {
	out, err := d.ecs.DescribeTasks(ctx, &ecs.DescribeTasksInput{
		Cluster: aws.String(d.cfg.Cluster),
		Tasks:   []string{taskARN},
		Include: []ecstypes.TaskField{ecstypes.TaskFieldTags},
	})
	if err != nil {
		return nil, fmt.Errorf("fargate driver: describe task: %w", err)
	}
	if len(out.Tasks) == 0 {
		return nil, nil
	}
	return &out.Tasks[0], nil
}

// Logs streams the task's output from CloudWatch Logs.
//
// awslogs is the only log driver Fargate offers, so this is a poll of
// GetLogEvents rather than a stream: there is no long-poll and no push. The
// loop ends when the task stops and the log stream stops producing, because a
// stream that ends the moment the task stops would truncate the last and most
// interesting lines.
func (d *Driver) Logs(ctx context.Context, env driver.Env) (<-chan driver.LogLine, error) {
	logCfg := d.resolveLogConfig(ctx)
	stream, err := logStreamName(logCfg, env.ID)
	if err != nil {
		return nil, err
	}

	out := make(chan driver.LogLine, 128)
	go func() {
		defer close(out)

		var token *string
		stoppedSeen := false

		for {
			resp, err := d.logs.GetLogEvents(ctx, &cloudwatchlogs.GetLogEventsInput{
				LogGroupName:  aws.String(logCfg.group),
				LogStreamName: aws.String(stream),
				NextToken:     token,
				StartFromHead: aws.Bool(true),
			})
			if err != nil {
				var missing *cwtypes.ResourceNotFoundException
				if !errors.As(err, &missing) {
					// Report rather than swallow: a log stream that cannot be
					// read is something an operator needs to know about, and
					// silence would look identical to a quiet agent.
					select {
					case out <- driver.LogLine{At: time.Now().UTC(), Stream: driver.StreamStderr,
						Text: "ephemera: log stream unreadable: " + err.Error()}:
					case <-ctx.Done():
					}
					return
				}
				// Not created yet. Normal for the first seconds of a task.
			} else {
				for _, ev := range resp.Events {
					line := driver.LogLine{Stream: driver.StreamStdout, Text: aws.ToString(ev.Message)}
					if ev.Timestamp != nil {
						line.At = time.UnixMilli(*ev.Timestamp).UTC()
					}
					select {
					case out <- line:
					case <-ctx.Done():
						return
					}
				}
				// GetLogEvents returns the same forward token at the end of the
				// stream; following it is how the next poll picks up new events.
				if resp.NextForwardToken != nil {
					token = resp.NextForwardToken
				}
			}

			// Stop one poll *after* the task stops, so the final lines that
			// arrive between the last poll and the exit are not lost.
			if stoppedSeen {
				return
			}
			task, derr := d.describeTask(ctx, env.ID)
			if derr != nil || task == nil || aws.ToString(task.LastStatus) == "STOPPED" {
				stoppedSeen = true
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(d.cfg.PollInterval):
			}
		}
	}()
	return out, nil
}

// resolveLogConfig reads where job output actually goes, from the task
// definition itself.
//
// This is deliberately not taken from our own configuration. The awslogs stream
// name is "<awslogs-stream-prefix>/<container-name>/<task-id>", and all three
// parts are decided by the task definition - which Terraform owns and can
// change without telling us. Assuming them means a system where jobs run
// perfectly and stream nothing, with no error anywhere to explain it, because
// we are reading a log stream that does not exist.
//
// Falls back to the configured values if the task definition cannot be read, so
// a missing ecs:DescribeTaskDefinition permission degrades to the old
// assumption rather than breaking logs entirely.
func (d *Driver) resolveLogConfig(ctx context.Context) logConfig {
	d.mu.Lock()
	cached := d.logCfg
	d.mu.Unlock()
	if cached != nil {
		return *cached
	}

	resolved := logConfig{
		group:     d.cfg.LogGroup,
		prefix:    d.cfg.LogStreamPrefix,
		container: d.cfg.ContainerName,
	}

	out, err := d.ecs.DescribeTaskDefinition(ctx, &ecs.DescribeTaskDefinitionInput{
		TaskDefinition: aws.String(d.cfg.TaskDefinition),
	})
	if err == nil && out.TaskDefinition != nil {
		for _, c := range out.TaskDefinition.ContainerDefinitions {
			if c.LogConfiguration == nil || c.LogConfiguration.LogDriver != ecstypes.LogDriverAwslogs {
				continue
			}
			opts := c.LogConfiguration.Options
			if g := opts["awslogs-group"]; g != "" {
				resolved.group = g
			}
			if p := opts["awslogs-stream-prefix"]; p != "" {
				resolved.prefix = p
			}
			if n := aws.ToString(c.Name); n != "" {
				resolved.container = n
			}
			break
		}
	}

	d.mu.Lock()
	d.logCfg = &resolved
	d.mu.Unlock()
	return resolved
}

// logStreamName builds the awslogs stream name for a task.
//
// The format "<prefix>/<container>/<task-id>" is fixed by ECS; the task id is
// the last segment of the ARN.
func logStreamName(cfg logConfig, taskARN string) (string, error) {
	idx := strings.LastIndex(taskARN, "/")
	if idx < 0 || idx == len(taskARN)-1 {
		return "", fmt.Errorf("fargate driver: cannot derive task id from %q", taskARN)
	}
	return cfg.prefix + "/" + cfg.container + "/" + taskARN[idx+1:], nil
}

// Collect downloads and unpacks what the agent uploaded.
//
// The tarball is untrusted: it was written by the payload, so it goes through
// the same extractor the docker driver uses, which refuses absolute paths,
// traversal and symlinks.
func (d *Driver) Collect(ctx context.Context, env driver.Env, dest string) ([]driver.Artifact, error) {
	body, err := d.objects.Get(ctx, artifactObjectKey(env.JobID))
	if err != nil {
		if errors.Is(err, ErrNoArtifacts) {
			// The agent uploaded nothing. Either it produced nothing, or it
			// died before it could upload. Both are ordinary; neither is an
			// error in Collect. The second is the known cost of uploading from
			// inside the agent - see the package comment.
			return nil, nil
		}
		return nil, fmt.Errorf("fargate driver: fetch artifacts: %w", err)
	}
	return archive.Extract(ctx, strings.NewReader(string(body)), dest, "")
}

// Destroy stops the task. Idempotent, and stopping something already stopped is
// success - the reaper calls this speculatively.
//
// The artifact object is deliberately not deleted: it is the job's output, it
// outlives the environment that produced it, and the bucket's lifecycle rule in
// the Terraform is what eventually reclaims it.
func (d *Driver) Destroy(ctx context.Context, env driver.Env) error {
	d.mu.Lock()
	delete(d.uploadURLs, env.JobID)
	d.mu.Unlock()

	_, err := d.ecs.StopTask(ctx, &ecs.StopTaskInput{
		Cluster: aws.String(d.cfg.Cluster),
		Task:    aws.String(env.ID),
		Reason:  aws.String("ephemera: environment destroyed"),
	})
	if err != nil {
		// A task ECS has forgotten is a task that costs nothing, which is the
		// outcome Destroy exists to guarantee.
		if isTaskGone(err) {
			return nil
		}
		return fmt.Errorf("fargate driver: stop task: %w", err)
	}
	return nil
}

// isTaskGone reports whether an error means "already not there".
func isTaskGone(err error) bool {
	var notFound *ecstypes.ResourceNotFoundException
	if errors.As(err, &notFound) {
		return true
	}
	var invalid *ecstypes.InvalidParameterException
	if errors.As(err, &invalid) {
		// ECS reports an unknown or already-stopped task this way rather than
		// with a not-found. Matching on the message is unpleasant but the
		// alternative is treating a clean outcome as a failure.
		msg := strings.ToLower(aws.ToString(invalid.Message))
		return strings.Contains(msg, "not found") || strings.Contains(msg, "does not exist")
	}
	return false
}

// List returns every job task in the cluster, including orphans from a previous
// control-plane process.
//
// Derived from ECS rather than memory, which is what the reaper needs: the
// environments that matter most are precisely the ones we have forgotten.
func (d *Driver) List(ctx context.Context) ([]driver.Env, error) {
	var envs []driver.Env
	var next *string

	for {
		listed, err := d.ecs.ListTasks(ctx, &ecs.ListTasksInput{
			Cluster:   aws.String(d.cfg.Cluster),
			StartedBy: aws.String(startedBy),
			NextToken: next,
		})
		if err != nil {
			return nil, fmt.Errorf("fargate driver: list tasks: %w", err)
		}
		if len(listed.TaskArns) > 0 {
			batch, err := d.describeBatch(ctx, listed.TaskArns)
			if err != nil {
				return nil, err
			}
			envs = append(envs, batch...)
		}
		if listed.NextToken == nil || aws.ToString(listed.NextToken) == "" {
			break
		}
		next = listed.NextToken
	}
	return envs, nil
}

// describeBatch resolves task ARNs into environments. DescribeTasks accepts at
// most 100 per call.
func (d *Driver) describeBatch(ctx context.Context, arns []string) ([]driver.Env, error) {
	const maxPerCall = 100
	var envs []driver.Env

	for start := 0; start < len(arns); start += maxPerCall {
		end := start + maxPerCall
		if end > len(arns) {
			end = len(arns)
		}
		out, err := d.ecs.DescribeTasks(ctx, &ecs.DescribeTasksInput{
			Cluster: aws.String(d.cfg.Cluster),
			Tasks:   arns[start:end],
			Include: []ecstypes.TaskField{ecstypes.TaskFieldTags},
		})
		if err != nil {
			return nil, fmt.Errorf("fargate driver: describe tasks: %w", err)
		}
		for i := range out.Tasks {
			if env, ok := envFromTask(&out.Tasks[i]); ok {
				envs = append(envs, env)
			}
		}
	}
	return envs, nil
}

// envFromTask rebuilds an environment from a task's tags. A task without our
// managed tag is not ours and is left alone: destroying something in a shared
// cluster because it happened to be listed would be unforgivable.
func envFromTask(task *ecstypes.Task) (driver.Env, bool) {
	tags := map[string]string{}
	for _, t := range task.Tags {
		tags[aws.ToString(t.Key)] = aws.ToString(t.Value)
	}
	if tags[TagManaged] != "true" {
		return driver.Env{}, false
	}

	env := driver.Env{
		ID:       aws.ToString(task.TaskArn),
		Driver:   "fargate",
		JobID:    tags[TagJob],
		TenantID: tags[TagTenant],
	}
	if task.CreatedAt != nil {
		env.CreatedAt = *task.CreatedAt
	}
	if v := tags[TagExpires]; v != "" {
		if ts, err := time.Parse(time.RFC3339, v); err == nil {
			env.ExpiresAt = ts
		}
	}
	return env, true
}
