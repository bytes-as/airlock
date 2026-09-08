package fargate

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"

	"github.com/bytes-as/ephemera/internal/driver"
	"github.com/bytes-as/ephemera/internal/job"
)

// These tests exercise the driver against fakes. They prove the logic that does
// not need AWS - the spec translation, the tag round-trip, the status mapping,
// the refusals - which is most of what this file gets wrong when it is wrong.
//
// They do NOT prove the driver works against ECS. Nothing here has spoken to
// AWS. A fake returns what the author believed the API returns, so these tests
// are worth exactly as much as that belief: they catch a regression, not a
// misunderstanding. See the package comment.

type fakeECS struct {
	taskDef    *ecs.DescribeTaskDefinitionOutput
	taskDefErr error
	runInput   *ecs.RunTaskInput
	runOut     *ecs.RunTaskOutput
	runErr     error
	tasks      map[string]*ecstypes.Task
	listPages  [][]string
	listCall   int
	stopped    []string
	stopErr    error
}

func (f *fakeECS) RunTask(_ context.Context, in *ecs.RunTaskInput, _ ...func(*ecs.Options)) (*ecs.RunTaskOutput, error) {
	f.runInput = in
	return f.runOut, f.runErr
}

func (f *fakeECS) StopTask(_ context.Context, in *ecs.StopTaskInput, _ ...func(*ecs.Options)) (*ecs.StopTaskOutput, error) {
	f.stopped = append(f.stopped, aws.ToString(in.Task))
	return &ecs.StopTaskOutput{}, f.stopErr
}

func (f *fakeECS) DescribeTasks(_ context.Context, in *ecs.DescribeTasksInput, _ ...func(*ecs.Options)) (*ecs.DescribeTasksOutput, error) {
	var out ecs.DescribeTasksOutput
	for _, arn := range in.Tasks {
		if t, ok := f.tasks[arn]; ok {
			out.Tasks = append(out.Tasks, *t)
		}
	}
	return &out, nil
}

func (f *fakeECS) ListTasks(_ context.Context, _ *ecs.ListTasksInput, _ ...func(*ecs.Options)) (*ecs.ListTasksOutput, error) {
	if f.listCall >= len(f.listPages) {
		return &ecs.ListTasksOutput{}, nil
	}
	page := f.listPages[f.listCall]
	f.listCall++
	var next *string
	if f.listCall < len(f.listPages) {
		next = aws.String("more")
	}
	return &ecs.ListTasksOutput{TaskArns: page, NextToken: next}, nil
}

func (f *fakeECS) DescribeTaskDefinition(_ context.Context, _ *ecs.DescribeTaskDefinitionInput, _ ...func(*ecs.Options)) (*ecs.DescribeTaskDefinitionOutput, error) {
	if f.taskDefErr != nil {
		return nil, f.taskDefErr
	}
	if f.taskDef != nil {
		return f.taskDef, nil
	}
	return &ecs.DescribeTaskDefinitionOutput{}, nil
}

type fakeLogs struct {
	events []cwtypes.OutputLogEvent
	err    error
}

func (f *fakeLogs) GetLogEvents(_ context.Context, _ *cloudwatchlogs.GetLogEventsInput, _ ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.GetLogEventsOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &cloudwatchlogs.GetLogEventsOutput{
		Events:           f.events,
		NextForwardToken: aws.String("t"),
	}, nil
}

type fakeObjects struct {
	presigned string
	body      []byte
	getErr    error
}

func (f *fakeObjects) PresignPut(_ context.Context, key string, _ time.Duration) (string, error) {
	if f.presigned == "" {
		f.presigned = "https://s3.example/" + key + "?sig=x"
	}
	return f.presigned, nil
}

func (f *fakeObjects) Get(_ context.Context, _ string) ([]byte, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.body, nil
}

func testDriver(t *testing.T, e *fakeECS, l *fakeLogs, o *fakeObjects) *Driver {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Cluster = "eph"
	cfg.TaskDefinition = "eph-job"
	cfg.Subnets = []string{"subnet-1"}
	cfg.SecurityGroups = []string{"sg-1"}
	cfg.LogGroup = "/ephemera/dev/jobs"
	cfg.ArtifactBucket = "bucket"
	cfg.PollInterval = time.Millisecond

	d, err := New(cfg, e, l, o)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return d
}

func specFor() driver.EnvSpec {
	return driver.EnvSpec{
		JobID:    "job_1",
		TenantID: "tenant_a",
		Deadline: time.Minute,
		Network:  driver.NetworkPolicy{Mode: driver.NetworkEgress},
	}
}

func TestConfigValidationNamesTheMissingField(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"cluster", func(c *Config) { c.Cluster = "" }, "Cluster"},
		{"task definition", func(c *Config) { c.TaskDefinition = "" }, "TaskDefinition"},
		{"subnets", func(c *Config) { c.Subnets = nil }, "subnet"},
		{"log group", func(c *Config) { c.LogGroup = "" }, "LogGroup"},
		{"bucket", func(c *Config) { c.ArtifactBucket = "" }, "ArtifactBucket"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Cluster = "c"
			cfg.TaskDefinition = "td"
			cfg.Subnets = []string{"s"}
			cfg.LogGroup = "lg"
			cfg.ArtifactBucket = "b"
			tc.mutate(&cfg)

			err := cfg.Validate()
			if err == nil {
				t.Fatal("want an error naming the missing field")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not name %q", err, tc.want)
			}
		})
	}
}

func TestCreateRefusesNetworkNone(t *testing.T) {
	// awsvpc always attaches an ENI. Accepting this would hand the caller a
	// fully networked task while they believed they had none.
	d := testDriver(t, &fakeECS{}, &fakeLogs{}, &fakeObjects{})
	spec := specFor()
	spec.Network = driver.NetworkPolicy{Mode: driver.NetworkNone}

	_, err := d.Create(context.Background(), spec)
	var unsupported *driver.UnsupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("want UnsupportedError, got %v", err)
	}
	if !strings.Contains(err.Error(), "docker") {
		t.Errorf("refusal should name the driver that can do this: %v", err)
	}
}

func TestCreateRefusesPerJobImage(t *testing.T) {
	// The task definition fixes the image. Silently ignoring a requested one
	// would run the wrong code with no indication.
	d := testDriver(t, &fakeECS{}, &fakeLogs{}, &fakeObjects{})
	spec := specFor()
	spec.Image = "someone/else:latest"

	if _, err := d.Create(context.Background(), spec); err == nil {
		t.Fatal("want a refusal for a per-job image")
	}
}

func TestCreateTagsTaskForTheReaper(t *testing.T) {
	e := &fakeECS{runOut: &ecs.RunTaskOutput{
		Tasks: []ecstypes.Task{{TaskArn: aws.String("arn:aws:ecs:r:1:task/eph/abc")}},
	}}
	d := testDriver(t, e, &fakeLogs{}, &fakeObjects{})

	env, err := d.Create(context.Background(), specFor())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if env.ID != "arn:aws:ecs:r:1:task/eph/abc" {
		t.Errorf("env.ID = %q", env.ID)
	}

	tags := map[string]string{}
	for _, tag := range e.runInput.Tags {
		tags[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	if tags[TagManaged] != "true" || tags[TagJob] != "job_1" || tags[TagTenant] != "tenant_a" {
		t.Errorf("tags = %v", tags)
	}
	if tags[TagExpires] == "" {
		t.Error("a deadline must be written on the task, or the reaper cannot enforce it for a job it has lost")
	}
	if aws.ToString(e.runInput.StartedBy) != startedBy {
		t.Errorf("startedBy = %q, want %q; List filters on it", aws.ToString(e.runInput.StartedBy), startedBy)
	}
}

func TestCreateSurfacesRunTaskFailures(t *testing.T) {
	// RunTask returns 200 with a failure list when it cannot place the task.
	// Treating that as success is how a job silently never runs.
	e := &fakeECS{runOut: &ecs.RunTaskOutput{
		Failures: []ecstypes.Failure{{
			Reason: aws.String("RESOURCE:MEMORY"),
			Detail: aws.String("no capacity"),
		}},
	}}
	d := testDriver(t, e, &fakeLogs{}, &fakeObjects{})

	_, err := d.Create(context.Background(), specFor())
	if err == nil {
		t.Fatal("a RunTask failure must not read as success")
	}
	if !strings.Contains(err.Error(), "RESOURCE:MEMORY") {
		t.Errorf("error should carry the reason: %v", err)
	}
}

func TestCreateInjectsUploadURLAndSecrets(t *testing.T) {
	e := &fakeECS{runOut: &ecs.RunTaskOutput{
		Tasks: []ecstypes.Task{{TaskArn: aws.String("arn:task/eph/abc")}},
	}}
	d := testDriver(t, e, &fakeLogs{}, &fakeObjects{})

	spec := specFor()
	spec.Env = map[string]string{"PLAIN": "value"}
	spec.Secrets = map[string]job.Secret{"TOKEN": job.Secret("s3cr3t")}

	if _, err := d.Create(context.Background(), spec); err != nil {
		t.Fatalf("Create: %v", err)
	}

	env := map[string]string{}
	for _, kv := range e.runInput.Overrides.ContainerOverrides[0].Environment {
		env[aws.ToString(kv.Name)] = aws.ToString(kv.Value)
	}
	if env["EPHEMERA_ARTIFACT_UPLOAD_URL"] == "" {
		t.Error("the agent has no way to return artifacts without an upload URL")
	}
	if env["TOKEN"] != "s3cr3t" {
		t.Errorf("secret not injected: %v", env["TOKEN"])
	}
	if env["PLAIN"] != "value" {
		t.Errorf("plain env not injected: %v", env["PLAIN"])
	}
}

func TestCreateRefusesSecretShadowingAnEnvVar(t *testing.T) {
	d := testDriver(t, &fakeECS{runOut: &ecs.RunTaskOutput{
		Tasks: []ecstypes.Task{{TaskArn: aws.String("arn:task/eph/abc")}},
	}}, &fakeLogs{}, &fakeObjects{})

	spec := specFor()
	spec.Env = map[string]string{"TOKEN": "plain"}
	spec.Secrets = map[string]job.Secret{"TOKEN": job.Secret("secret")}

	if _, err := d.Create(context.Background(), spec); err == nil {
		t.Fatal("a name that is both a secret and a plain variable must be refused, not resolved silently")
	}
}

func TestStatusMapping(t *testing.T) {
	for _, tc := range []struct {
		name  string
		task  ecstypes.Task
		phase driver.Phase
		check func(*testing.T, driver.Status)
	}{
		{
			name:  "pending is creating",
			task:  ecstypes.Task{LastStatus: aws.String("PENDING")},
			phase: driver.PhaseCreating,
		},
		{
			name:  "running",
			task:  ecstypes.Task{LastStatus: aws.String("RUNNING")},
			phase: driver.PhaseRunning,
		},
		{
			name: "clean exit",
			task: ecstypes.Task{
				LastStatus: aws.String("STOPPED"),
				Containers: []ecstypes.Container{{ExitCode: aws.Int32(0)}},
			},
			phase: driver.PhaseExited,
			check: func(t *testing.T, s driver.Status) {
				if !s.Succeeded() {
					t.Error("exit 0 should read as success")
				}
			},
		},
		{
			name: "oom is not a crash",
			task: ecstypes.Task{
				LastStatus:    aws.String("STOPPED"),
				StoppedReason: aws.String("OutOfMemoryError: container killed due to memory usage"),
				Containers:    []ecstypes.Container{{ExitCode: aws.Int32(137)}},
			},
			phase: driver.PhaseExited,
			check: func(t *testing.T, s driver.Status) {
				if !s.OOMKilled {
					t.Error("OOM must be distinguishable from an agent crash: different bug, different owner")
				}
			},
		},
		{
			name: "never started has no exit code",
			task: ecstypes.Task{
				LastStatus:    aws.String("STOPPED"),
				StoppedReason: aws.String("CannotPullContainerError"),
				Containers:    []ecstypes.Container{{Reason: aws.String("CannotPullContainerError: not found")}},
			},
			phase: driver.PhaseExited,
			check: func(t *testing.T, s driver.Status) {
				if s.ExitCode == 0 {
					t.Error("a task that never ran must not report exit 0, which reads as success")
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := statusOf(&tc.task)
			if got.Phase != tc.phase {
				t.Errorf("phase = %q, want %q", got.Phase, tc.phase)
			}
			if tc.check != nil {
				tc.check(t, got)
			}
		})
	}
}

func TestDescribeReportsGoneForAForgottenTask(t *testing.T) {
	// ECS retains stopped tasks for about an hour. After that a lookup finds
	// nothing, and an environment that is gone costs nothing - which is the
	// property the reaper cares about, so it must not be an error.
	d := testDriver(t, &fakeECS{tasks: map[string]*ecstypes.Task{}}, &fakeLogs{}, &fakeObjects{})

	status, err := d.Describe(context.Background(), driver.Env{ID: "arn:task/eph/missing"})
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	if status.Phase != driver.PhaseGone {
		t.Errorf("phase = %q, want %q", status.Phase, driver.PhaseGone)
	}
}

func TestListOnlyReturnsOurTasks(t *testing.T) {
	// A shared cluster may hold tasks that are not ours. Destroying one because
	// it happened to be listed would be unforgivable.
	e := &fakeECS{
		listPages: [][]string{{"arn:task/eph/ours"}, {"arn:task/eph/theirs"}},
		tasks: map[string]*ecstypes.Task{
			"arn:task/eph/ours": {
				TaskArn: aws.String("arn:task/eph/ours"),
				Tags: []ecstypes.Tag{
					{Key: aws.String(TagManaged), Value: aws.String("true")},
					{Key: aws.String(TagJob), Value: aws.String("job_1")},
					{Key: aws.String(TagTenant), Value: aws.String("tenant_a")},
					{Key: aws.String(TagExpires), Value: aws.String("2026-01-01T00:00:00Z")},
				},
			},
			"arn:task/eph/theirs": {
				TaskArn: aws.String("arn:task/eph/theirs"),
				Tags:    []ecstypes.Tag{{Key: aws.String("someone"), Value: aws.String("else")}},
			},
		},
	}
	d := testDriver(t, e, &fakeLogs{}, &fakeObjects{})

	envs, err := d.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(envs) != 1 {
		t.Fatalf("got %d environments, want only ours: %+v", len(envs), envs)
	}
	if envs[0].JobID != "job_1" || envs[0].TenantID != "tenant_a" {
		t.Errorf("identity not recovered from tags: %+v", envs[0])
	}
	if envs[0].ExpiresAt.IsZero() {
		t.Error("expiry not recovered from tags, so the reaper cannot enforce it after a restart")
	}
}

func TestDestroyIsIdempotentWhenTheTaskIsGone(t *testing.T) {
	e := &fakeECS{stopErr: &ecstypes.InvalidParameterException{
		Message: aws.String("The referenced task was not found."),
	}}
	d := testDriver(t, e, &fakeLogs{}, &fakeObjects{})

	if err := d.Destroy(context.Background(), driver.Env{ID: "arn:task/eph/gone"}); err != nil {
		t.Fatalf("destroying an already-gone task must be success, got %v", err)
	}
}

func TestCollectWithNoUploadIsNotAnError(t *testing.T) {
	// A job that produced nothing and a job that died before uploading look the
	// same from here. Neither is a collection failure.
	d := testDriver(t, &fakeECS{}, &fakeLogs{}, &fakeObjects{getErr: ErrNoArtifacts})

	got, err := d.Collect(context.Background(), driver.Env{JobID: "job_1"}, t.TempDir())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("want no artifacts, got %+v", got)
	}
}

func TestCollectExtractsUploadedArchive(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for name, content := range map[string]string{
		"shot.png":           "png",
		"nested/result.json": "{}",
	} {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(content))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	tw.Close()

	d := testDriver(t, &fakeECS{}, &fakeLogs{}, &fakeObjects{body: buf.Bytes()})
	dest := t.TempDir()

	got, err := d.Collect(context.Background(), driver.Env{JobID: "job_1"}, dest)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d artifacts, want 2: %+v", len(got), got)
	}
	if _, err := os.Stat(filepath.Join(dest, "nested", "result.json")); err != nil {
		t.Errorf("nested artifact not written: %v", err)
	}
}

func TestCollectRefusesTraversalInAnUploadedArchive(t *testing.T) {
	// The archive is written by the payload. An entry escaping the destination
	// is the whole reason extraction goes through the shared safe extractor.
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	body := "owned"
	if err := tw.WriteHeader(&tar.Header{Name: "../../escaped", Mode: 0o644, Size: int64(len(body))}); err != nil {
		t.Fatal(err)
	}
	tw.Write([]byte(body))
	tw.Close()

	d := testDriver(t, &fakeECS{}, &fakeLogs{}, &fakeObjects{body: buf.Bytes()})
	dest := t.TempDir()

	got, _ := d.Collect(context.Background(), driver.Env{JobID: "job_1"}, dest)
	for _, a := range got {
		if strings.Contains(a.Path, "escaped") && !strings.HasPrefix(a.Path, dest) {
			t.Fatalf("traversal escaped the destination: %s", a.Path)
		}
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(filepath.Dir(dest)), "escaped")); err == nil {
		t.Fatal("an entry escaped the destination directory")
	}
}

func TestLogStreamNameMatchesTheAwslogsFormat(t *testing.T) {
	// "<prefix>/<container>/<task-id>" is fixed by ECS. Getting it wrong yields
	// a job that runs correctly and streams nothing, which is a confusing bug.
	got, err := logStreamName(logConfig{prefix: "job", container: "agent"},
		"arn:aws:ecs:eu-west-1:1:task/eph/deadbeef")
	if err != nil {
		t.Fatalf("logStreamName: %v", err)
	}
	if want := "job/agent/deadbeef"; got != want {
		t.Errorf("stream = %q, want %q", got, want)
	}
}

func TestLogConfigComesFromTheTaskDefinitionNotOurGuess(t *testing.T) {
	// Terraform owns the task definition and can change the log group, the
	// stream prefix or the container name without telling the control plane.
	// Reading it back is what stops "jobs run fine and stream nothing".
	e := &fakeECS{taskDef: &ecs.DescribeTaskDefinitionOutput{
		TaskDefinition: &ecstypes.TaskDefinition{
			ContainerDefinitions: []ecstypes.ContainerDefinition{{
				Name: aws.String("renamed-agent"),
				LogConfiguration: &ecstypes.LogConfiguration{
					LogDriver: ecstypes.LogDriverAwslogs,
					Options: map[string]string{
						"awslogs-group":         "/somewhere/else",
						"awslogs-stream-prefix": "different",
					},
				},
			}},
		},
	}}
	d := testDriver(t, e, &fakeLogs{}, &fakeObjects{})

	got := d.resolveLogConfig(context.Background())
	if got.group != "/somewhere/else" || got.prefix != "different" || got.container != "renamed-agent" {
		t.Fatalf("log config not taken from the task definition: %+v", got)
	}

	stream, err := logStreamName(got, "arn:task/eph/abc123")
	if err != nil {
		t.Fatal(err)
	}
	if want := "different/renamed-agent/abc123"; stream != want {
		t.Errorf("stream = %q, want %q", stream, want)
	}
}

func TestLogConfigFallsBackWhenTheTaskDefinitionCannotBeRead(t *testing.T) {
	// A missing ecs:DescribeTaskDefinition permission should degrade to the
	// configured values, not break log streaming outright.
	e := &fakeECS{taskDefErr: errors.New("AccessDeniedException")}
	d := testDriver(t, e, &fakeLogs{}, &fakeObjects{})

	got := d.resolveLogConfig(context.Background())
	if got.group != "/ephemera/dev/jobs" || got.prefix != "job" || got.container != "agent" {
		t.Errorf("fallback not applied: %+v", got)
	}
}

func TestLogsStreamsEventsAndEnds(t *testing.T) {
	e := &fakeECS{tasks: map[string]*ecstypes.Task{
		"arn:task/eph/abc": {
			TaskArn:    aws.String("arn:task/eph/abc"),
			LastStatus: aws.String("STOPPED"),
			Containers: []ecstypes.Container{{ExitCode: aws.Int32(0)}},
		},
	}}
	l := &fakeLogs{events: []cwtypes.OutputLogEvent{
		{Message: aws.String("step 1"), Timestamp: aws.Int64(time.Now().UnixMilli())},
		{Message: aws.String("done"), Timestamp: aws.Int64(time.Now().UnixMilli())},
	}}
	d := testDriver(t, e, l, &fakeObjects{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	lines, err := d.Logs(ctx, driver.Env{ID: "arn:task/eph/abc"})
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}

	var texts []string
	for line := range lines {
		texts = append(texts, line.Text)
	}
	if len(texts) < 2 {
		t.Fatalf("want the agent's lines, got %v", texts)
	}
	if texts[0] != "step 1" {
		t.Errorf("first line = %q", texts[0])
	}
}

func TestWaitReturnsTheFinalStatus(t *testing.T) {
	e := &fakeECS{tasks: map[string]*ecstypes.Task{
		"arn:task/eph/abc": {
			TaskArn:    aws.String("arn:task/eph/abc"),
			LastStatus: aws.String("STOPPED"),
			Containers: []ecstypes.Container{{ExitCode: aws.Int32(3)}},
		},
	}}
	d := testDriver(t, e, &fakeLogs{}, &fakeObjects{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	status, err := d.Wait(ctx, driver.Env{ID: "arn:task/eph/abc"})
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if status.ExitCode != 3 {
		t.Errorf("exit code = %d, want 3", status.ExitCode)
	}
	if failure := status.Failure(); failure == nil {
		t.Error("a non-zero exit must map to a failure in the taxonomy")
	}
}

func TestStartFailsWhenTheTaskStopsWithoutRunning(t *testing.T) {
	e := &fakeECS{tasks: map[string]*ecstypes.Task{
		"arn:task/eph/abc": {
			TaskArn:       aws.String("arn:task/eph/abc"),
			LastStatus:    aws.String("STOPPED"),
			StoppedReason: aws.String("CannotPullContainerError: image not found"),
		},
	}}
	d := testDriver(t, e, &fakeLogs{}, &fakeObjects{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := d.Start(ctx, driver.Env{ID: "arn:task/eph/abc"}, specFor())
	if err == nil {
		t.Fatal("want a start failure")
	}
	if !strings.Contains(err.Error(), "CannotPullContainerError") {
		t.Errorf("error should carry the reason an operator needs: %v", err)
	}
}

func TestCapabilitiesAreHonest(t *testing.T) {
	d := testDriver(t, &fakeECS{}, &fakeLogs{}, &fakeObjects{})
	caps := d.Capabilities()

	if caps.Isolation != driver.IsolationMicroVM {
		t.Errorf("Fargate gives each task its own kernel; isolation = %v", caps.Isolation)
	}
	if !caps.SurvivesControlPlaneRestart {
		t.Error("List reads from ECS, so orphans must be recoverable after a restart")
	}
}
