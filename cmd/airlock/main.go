// Command airlock is the command-line client for the control plane.
//
// It exists because someone with ten minutes should be able to run one command
// and watch a job happen, rather than assemble curl invocations from a README.
//
// The default `run` command does the whole loop — submit, stream logs, wait,
// then report artifacts — because that is what someone actually wants to do.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var version = "dev"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		usage()
		return errors.New("no command given")
	}

	command, rest := args[0], args[1:]
	switch command {
	case "run":
		return cmdRun(rest)
	case "submit":
		return cmdSubmit(rest)
	case "get":
		return cmdGet(rest)
	case "list":
		return cmdList(rest)
	case "logs":
		return cmdLogs(rest)
	case "artifacts":
		return cmdArtifacts(rest)
	case "stats":
		return cmdStats(rest)
	case "version":
		fmt.Printf("airlock %s\n", version)
		return nil
	case "help", "-h", "--help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown command %q", command)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `airlock - client for the airlock control plane

Usage:
  airlock <command> [flags]

Commands:
  run         Submit a job, stream its logs, and report the result
  submit      Submit a job and print its ID
  get         Show one job
  list        List recent jobs
  logs        Stream a job's logs
  artifacts   List a job's artifacts with download links
  stats       Show scheduler and queue state
  version     Print the version

Common flags:
  --server    Control plane URL (default http://localhost:8080, or AIRLOCK_SERVER)
  --token     Bearer token (or AIRLOCK_TOKEN)

Examples:
  airlock run --command /usr/local/bin/airlock-agent --query "site reliability"
  airlock run --image airlock/agent:latest --priority high --deadline 2m
  airlock list --state failed
  airlock logs job_06g83kwj5951twb2ycswewf9mc

`)
}

// --- shared flags ---

type common struct {
	server string
	token  string
}

func (c *common) bind(fs *flag.FlagSet) {
	fs.StringVar(&c.server, "server", envOr("AIRLOCK_SERVER", "http://localhost:8080"), "control plane URL")
	fs.StringVar(&c.token, "token", os.Getenv("AIRLOCK_TOKEN"), "bearer token")
}

// jobFlags are the submission options shared by `run` and `submit`.
type jobFlags struct {
	common
	image     string
	command   string
	query     string
	priority  string
	deadline  time.Duration
	attempts  int
	env       stringList
	secrets   stringList
	memoryMiB int64
	cpuMillis int64
	region    string
}

func (j *jobFlags) bind(fs *flag.FlagSet) {
	j.common.bind(fs)
	fs.StringVar(&j.image, "image", "", "container image carrying the agent")
	fs.StringVar(&j.command, "command", "", "command to run (space separated)")
	fs.StringVar(&j.query, "query", "", "convenience for AIRLOCK_QUERY, the agent's search term")
	fs.StringVar(&j.priority, "priority", "normal", "low, normal or high")
	fs.DurationVar(&j.deadline, "deadline", 0, "maximum run time (0 uses the server default)")
	fs.IntVar(&j.attempts, "attempts", 0, "maximum attempts (0 uses the server default)")
	fs.Var(&j.env, "env", "environment variable as KEY=VALUE (repeatable)")
	fs.Var(&j.secrets, "secret", "secret as NAME=source:key (repeatable)")
	fs.Int64Var(&j.memoryMiB, "memory-mib", 0, "memory limit in MiB")
	fs.Int64Var(&j.cpuMillis, "cpu-millis", 0, "CPU limit in millicores")
	fs.StringVar(&j.region, "egress-region", "", "require an egress point in this region; refused if the platform has none")
}

func (j *jobFlags) request() (map[string]any, error) {
	if j.image == "" && j.command == "" {
		return nil, errors.New("a job needs --image or --command")
	}

	env := map[string]string{}
	for _, pair := range j.env {
		key, value, ok := strings.Cut(pair, "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("--env %q must look like KEY=VALUE", pair)
		}
		env[key] = value
	}
	if j.query != "" {
		env["AIRLOCK_QUERY"] = j.query
	}

	var secrets []map[string]string
	for _, spec := range j.secrets {
		name, ref, ok := strings.Cut(spec, "=")
		if !ok || name == "" {
			return nil, fmt.Errorf("--secret %q must look like NAME=source:key", spec)
		}
		source, key, ok := strings.Cut(ref, ":")
		if !ok || source == "" || key == "" {
			return nil, fmt.Errorf("--secret %q must look like NAME=source:key", spec)
		}
		secrets = append(secrets, map[string]string{"name": name, "source": source, "key": key})
	}

	req := map[string]any{"priority": j.priority}
	if j.image != "" {
		req["image"] = j.image
	}
	if j.command != "" {
		req["command"] = strings.Fields(j.command)
	}
	if len(env) > 0 {
		req["env"] = env
	}
	if len(secrets) > 0 {
		req["secrets"] = secrets
	}
	if j.deadline > 0 {
		req["deadline_seconds"] = int(j.deadline.Seconds())
	}
	if j.region != "" {
		req["egress_region"] = j.region
	}
	if j.attempts > 0 {
		req["max_attempts"] = j.attempts
	}
	if j.memoryMiB > 0 {
		req["memory_mib"] = j.memoryMiB
	}
	if j.cpuMillis > 0 {
		req["cpu_millis"] = j.cpuMillis
	}
	return req, nil
}

// --- commands ---

// cmdRun is the whole loop, because it is what someone actually wants.
func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	var jf jobFlags
	jf.bind(fs)
	quiet := fs.Bool("quiet", false, "suppress the log stream")
	if err := fs.Parse(args); err != nil {
		return err
	}

	client := newClient(jf.common)
	req, err := jf.request()
	if err != nil {
		return err
	}

	job, err := client.submit(req)
	if err != nil {
		return err
	}
	fmt.Printf("submitted %s (priority %v)\n", job.ID, job.Priority)

	// Ctrl-C detaches the client. It deliberately does NOT cancel the job:
	// the job belongs to the server, and killing work because a viewer looked
	// away would be surprising and expensive.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if !*quiet {
		if err := client.streamLogs(ctx, job.ID, os.Stdout); err != nil && ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "warning: log stream ended early: %v\n", err)
		}
	}

	final, err := client.awaitTerminal(ctx, job.ID)
	if err != nil {
		return err
	}
	printJob(final)

	artifacts, err := client.artifacts(final.ID)
	if err == nil && len(artifacts) > 0 {
		fmt.Println("\nartifacts:")
		for _, a := range artifacts {
			fmt.Printf("  %-24s %8s  %s\n", a.Name, humanBytes(a.Size), a.URL)
		}
	}

	if final.State != "succeeded" {
		// Non-zero exit so this composes in a shell script or a CI step.
		return fmt.Errorf("job %s", final.State)
	}
	return nil
}

func cmdSubmit(args []string) error {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	var jf jobFlags
	jf.bind(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}

	req, err := jf.request()
	if err != nil {
		return err
	}
	job, err := newClient(jf.common).submit(req)
	if err != nil {
		return err
	}
	fmt.Println(job.ID)
	return nil
}

func cmdGet(args []string) error {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	var c common
	c.bind(fs)
	asJSON := fs.Bool("json", false, "print raw JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	id := fs.Arg(0)
	if id == "" {
		return errors.New("usage: airlock get <job-id>")
	}

	client := newClient(c)
	if *asJSON {
		raw, err := client.getRaw("/v1/jobs/" + id)
		if err != nil {
			return err
		}
		fmt.Println(string(raw))
		return nil
	}
	job, err := client.job(id)
	if err != nil {
		return err
	}
	printJob(job)
	return nil
}

func cmdList(args []string) error {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	var c common
	c.bind(fs)
	state := fs.String("state", "", "filter by state")
	limit := fs.Int("limit", 20, "maximum jobs to show")
	if err := fs.Parse(args); err != nil {
		return err
	}

	path := fmt.Sprintf("/v1/jobs?limit=%d", *limit)
	if *state != "" {
		path += "&state=" + *state
	}
	raw, err := newClient(c).getRaw(path)
	if err != nil {
		return err
	}

	var listing struct {
		Jobs []jobView `json:"jobs"`
	}
	if err := json.Unmarshal(raw, &listing); err != nil {
		return err
	}
	if len(listing.Jobs) == 0 {
		fmt.Println("no jobs")
		return nil
	}

	fmt.Printf("%-30s %-12s %-8s %10s  %s\n", "ID", "STATE", "PRIORITY", "DURATION", "SUBMITTED")
	for _, j := range listing.Jobs {
		duration := "-"
		if j.DurationMS > 0 {
			duration = (time.Duration(j.DurationMS) * time.Millisecond).Round(time.Millisecond).String()
		}
		fmt.Printf("%-30s %-12s %-8d %10s  %s\n",
			j.ID, j.State, j.Priority, duration, j.SubmittedAt.Format(time.RFC3339))
	}
	return nil
}

func cmdLogs(args []string) error {
	fs := flag.NewFlagSet("logs", flag.ExitOnError)
	var c common
	c.bind(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	id := fs.Arg(0)
	if id == "" {
		return errors.New("usage: airlock logs <job-id>")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	err := newClient(c).streamLogs(ctx, id, os.Stdout)
	if ctx.Err() != nil {
		return nil // The user detached; that is not a failure.
	}
	return err
}

func cmdArtifacts(args []string) error {
	fs := flag.NewFlagSet("artifacts", flag.ExitOnError)
	var c common
	c.bind(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	id := fs.Arg(0)
	if id == "" {
		return errors.New("usage: airlock artifacts <job-id>")
	}

	artifacts, err := newClient(c).artifacts(id)
	if err != nil {
		return err
	}
	if len(artifacts) == 0 {
		fmt.Println("no artifacts")
		return nil
	}
	for _, a := range artifacts {
		fmt.Printf("%-24s %8s  %-24s %s\n", a.Name, humanBytes(a.Size), a.ContentType, a.URL)
	}
	return nil
}

func cmdStats(args []string) error {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	var c common
	c.bind(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}

	raw, err := newClient(c).getRaw("/v1/stats")
	if err != nil {
		return err
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, raw, "", "  "); err != nil {
		fmt.Println(string(raw))
		return nil
	}
	fmt.Println(pretty.String())
	return nil
}

// --- client ---

type client struct {
	server string
	token  string
	http   *http.Client
}

func newClient(c common) *client {
	return &client{
		server: strings.TrimRight(c.server, "/"),
		token:  c.token,
		// No global timeout: this client streams logs for as long as a job
		// runs, and a client-side timeout would sever it mid-job.
		http: &http.Client{},
	}
}

type jobView struct {
	ID          string    `json:"id"`
	State       string    `json:"state"`
	Priority    int       `json:"priority"`
	Attempts    int       `json:"attempts"`
	SubmittedAt time.Time `json:"submitted_at"`
	DurationMS  int64     `json:"duration_ms"`
	Failure     *struct {
		Kind      string `json:"kind"`
		Fault     string `json:"fault"`
		Message   string `json:"message"`
		Retryable bool   `json:"retryable"`
	} `json:"failure"`
	LogsURL      string `json:"logs_url"`
	ArtifactsURL string `json:"artifacts_url"`
}

type artifactView struct {
	Name        string `json:"name"`
	Size        int64  `json:"size"`
	ContentType string `json:"content_type"`
	URL         string `json:"url"`
}

func (c *client) request(method, path string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, c.server+path, body)
	if err != nil {
		return nil, err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

func (c *client) submit(payload map[string]any) (jobView, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return jobView{}, err
	}
	req, err := c.request("POST", "/v1/jobs", bytes.NewReader(encoded))
	if err != nil {
		return jobView{}, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return jobView{}, fmt.Errorf("cannot reach the control plane at %s: %w", c.server, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return jobView{}, c.apiError(resp)
	}
	var job jobView
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return jobView{}, err
	}
	return job, nil
}

func (c *client) getRaw(path string) ([]byte, error) {
	req, err := c.request("GET", path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cannot reach the control plane at %s: %w", c.server, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, c.apiError(resp)
	}
	return io.ReadAll(resp.Body)
}

func (c *client) job(id string) (jobView, error) {
	raw, err := c.getRaw("/v1/jobs/" + id)
	if err != nil {
		return jobView{}, err
	}
	var job jobView
	if err := json.Unmarshal(raw, &job); err != nil {
		return jobView{}, err
	}
	return job, nil
}

func (c *client) artifacts(id string) ([]artifactView, error) {
	raw, err := c.getRaw("/v1/jobs/" + id + "/artifacts")
	if err != nil {
		return nil, err
	}
	var listing struct {
		Artifacts []artifactView `json:"artifacts"`
	}
	if err := json.Unmarshal(raw, &listing); err != nil {
		return nil, err
	}
	return listing.Artifacts, nil
}

// streamLogs consumes the SSE stream until the server signals the end.
func (c *client) streamLogs(ctx context.Context, id string, out io.Writer) error {
	req, err := c.request("GET", "/v1/jobs/"+id+"/logs", nil)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return c.apiError(resp)
	}

	scanner := bufio.NewScanner(resp.Body)
	// Agent output can be long - a stack trace, a base64 blob. The default
	// 64KiB limit would truncate mid-line.
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case line == "event: end":
			return nil
		case strings.HasPrefix(line, ": "):
			continue // keep-alive comment
		case strings.HasPrefix(line, "data: "):
			payload := strings.TrimPrefix(line, "data: ")
			var entry struct {
				At     time.Time `json:"at"`
				Stream string    `json:"stream"`
				Text   string    `json:"text"`
			}
			if err := json.Unmarshal([]byte(payload), &entry); err != nil {
				continue
			}
			if entry.Text == "" {
				continue
			}
			prefix := "  "
			switch entry.Stream {
			case "stderr":
				prefix = "! "
			case "notice":
				prefix = "* "
			}
			fmt.Fprintf(out, "%s%s\n", prefix, entry.Text)
		}
	}
	return scanner.Err()
}

// awaitTerminal polls until the job finishes.
func (c *client) awaitTerminal(ctx context.Context, id string) (jobView, error) {
	// The log stream ending is a strong signal the job is done, so the first
	// poll almost always settles it. The loop exists for the cases where it
	// does not - a dropped stream, a job that failed before producing output.
	backoff := 100 * time.Millisecond
	for {
		job, err := c.job(id)
		if err != nil {
			return jobView{}, err
		}
		switch job.State {
		case "succeeded", "failed", "cancelled":
			return job, nil
		}
		select {
		case <-time.After(backoff):
			if backoff < 2*time.Second {
				backoff *= 2
			}
		case <-ctx.Done():
			return job, nil
		}
	}
}

// apiError turns a non-2xx response into a message worth reading.
func (c *client) apiError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))

	var apiErr struct {
		Error             string `json:"error"`
		Message           string `json:"message"`
		RetryAfterSeconds int    `json:"retry_after_seconds"`
	}
	if json.Unmarshal(body, &apiErr) == nil && apiErr.Error != "" {
		msg := fmt.Sprintf("%s: %s", apiErr.Error, apiErr.Message)
		// Surface the retry hint: the server went to the trouble of computing
		// one, and a client that ignores it is the retry storm.
		if apiErr.RetryAfterSeconds > 0 {
			msg += fmt.Sprintf(" (retry after %ds)", apiErr.RetryAfterSeconds)
		}
		return errors.New(msg)
	}
	return fmt.Errorf("server returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
}

// --- output helpers ---

func printJob(j jobView) {
	fmt.Printf("\n%-12s %s\n", "job", j.ID)
	fmt.Printf("%-12s %s\n", "state", j.State)
	fmt.Printf("%-12s %d\n", "attempts", j.Attempts)
	if j.DurationMS > 0 {
		fmt.Printf("%-12s %s\n", "duration", (time.Duration(j.DurationMS) * time.Millisecond).Round(time.Millisecond))
	}
	if j.Failure != nil {
		fmt.Printf("%-12s %s (%s fault, retryable=%v)\n", "failure",
			j.Failure.Kind, j.Failure.Fault, j.Failure.Retryable)
		if j.Failure.Message != "" {
			fmt.Printf("%-12s %s\n", "", j.Failure.Message)
		}
	}
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for size := n / unit; size >= unit; size /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "KMGT"[exp])
}

// stringList collects a repeatable flag.
type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }

func (s *stringList) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func envOr(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}
