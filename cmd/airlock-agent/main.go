// Command airlock-agent is the placeholder "computer use" agent.
//
// The agent is not the interesting part of this system; the infrastructure
// wrapping it is. So this is deliberately a stand-in - it opens a "browser",
// searches, and saves a screenshot - but a stand-in that behaves like the real
// thing in every way the platform cares about:
//
//   - It reads its task from the environment the platform injected.
//   - It streams progress to stdout as it works, so log streaming has something
//     real to carry.
//   - It writes artifacts into AIRLOCK_ARTIFACT_DIR, including a PNG, so the
//     artifact pipeline handles binary content and content types honestly.
//   - It exits non-zero on failure, so the failure taxonomy is exercised.
//   - It respects its own deadline, so it is a well-behaved citizen rather than
//     something the reaper has to kill.
//
// Swapping this for a real Playwright agent means changing what happens between
// "start" and "screenshot saved". Nothing above it needs to know.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

var version = "dev"

// Where this run writes its output, and where to upload it when the control
// plane cannot read that directory itself. Package-level because every exit
// path - including the signal handler - has to be able to flush artifacts, and
// os.Exit does not run deferred functions.
var (
	artifactDir string
	uploadURL   string
)

// finish uploads whatever the run produced, then exits with code.
//
// Every exit from this program goes through here. Calling os.Exit directly
// would skip the upload, which on the Fargate path is the only way output ever
// leaves the task - the exact bug this indirection exists to prevent.
func finish(code int) {
	if err := uploadArtifacts(artifactDir, uploadURL); err != nil {
		// A failed upload must not turn a successful job into a failed one, but
		// it must be visible: the logs are the only place it can surface.
		errorf("artifact upload failed: %v", err)
	}
	os.Exit(code)
}

func main() {
	var (
		task      = flag.String("task", envOr("AIRLOCK_TASK", "search"), "task to perform")
		query     = flag.String("query", envOr("AIRLOCK_QUERY", "ephemeral environments"), "search query")
		steps     = flag.Int("steps", envIntOr("AIRLOCK_STEPS", 4), "simulated steps to perform")
		stepDelay = flag.Duration("step-delay", envDurationOr("AIRLOCK_STEP_DELAY", 300*time.Millisecond), "pause between steps")
		failAt    = flag.Int("fail-at", envIntOr("AIRLOCK_FAIL_AT", 0), "fail deliberately at this step (0 never)")
		hang      = flag.Bool("hang", os.Getenv("AIRLOCK_HANG") == "1", "hang forever, to exercise the reaper")
		browserTO = flag.Duration("browser-timeout", envDurationOr("AIRLOCK_BROWSER_TIMEOUT", 45*time.Second), "how long the browser may take to load the page and capture it")
		showVer   = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()

	if *showVer {
		fmt.Printf("airlock-agent %s\n", version)
		return
	}

	// Handle termination gracefully so a deadline kill or a Destroy produces a
	// clean last log line rather than an abrupt silence. The platform will
	// SIGKILL if this takes too long, which is correct.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signals
		logf("received termination signal, stopping")
		// Still upload: a job stopped by its deadline has usually produced
		// something worth keeping, and this is the last chance to send it.
		finish(143) // 128 + SIGTERM, the conventional shell encoding
	}()

	jobID := os.Getenv("AIRLOCK_JOB_ID")
	tenant := os.Getenv("AIRLOCK_TENANT_ID")
	artifactDir = os.Getenv("AIRLOCK_ARTIFACT_DIR")
	// Set only by drivers whose environment the control plane cannot read
	// directly, which today means Fargate. Empty on the docker and process
	// drivers, where uploadArtifacts is a no-op.
	uploadURL = os.Getenv("AIRLOCK_ARTIFACT_UPLOAD_URL")

	logf("agent %s starting", version)
	// Say which mode this run is in, every run. A real capture and a generated
	// one produce the same filename, and confusing the two is the kind of thing
	// that makes someone distrust every other artifact the system produced.
	if browser := findBrowser(); browser != "" {
		logf("browser available: %s", browser)
	} else {
		logf("no browser on PATH; screenshots will be generated images, not captures")
	}
	logf("job=%s tenant=%s task=%s", orDash(jobID), orDash(tenant), *task)

	if artifactDir == "" {
		// Not fatal: a job may legitimately produce nothing. But say so, because
		// silently producing no artifacts is indistinguishable from a bug.
		logf("warning: AIRLOCK_ARTIFACT_DIR is not set; this run will produce no artifacts")
	} else if err := os.MkdirAll(artifactDir, 0o750); err != nil {
		errorf("cannot create artifact directory: %v", err)
		finish(1)
	}

	if *hang {
		logf("hanging deliberately; the platform's deadline should stop this")
		select {}
	}

	// The "browser session": a sequence of steps, each producing progress
	// output and, at the end, a screenshot.
	for step := 1; step <= *steps; step++ {
		if *failAt > 0 && step == *failAt {
			errorf("step %d failed: could not reach the target page", step)
			finish(1)
		}
		logf("step %d/%d: %s", step, *steps, describeStep(step, *query))
		time.Sleep(*stepDelay)
	}

	if artifactDir != "" {
		if err := writeArtifacts(artifactDir, *task, *query, *steps, *browserTO); err != nil {
			errorf("could not write artifacts: %v", err)
			finish(1)
		}
	}

	logf("task complete")
	finish(0)
}

func describeStep(step int, query string) string {
	switch step {
	case 1:
		return "preparing session"
	case 2:
		return fmt.Sprintf("preparing search for %q", query)
	case 3:
		return "settling"
	default:
		return "capturing page state"
	}
}

// writeArtifacts produces what a real computer-use agent would leave behind:
// a screenshot and a structured result.
func writeArtifacts(dir, task, query string, steps int, budget time.Duration) error {
	// Real browser when one is present, generated image otherwise. Which one
	// happened is recorded below, because an artifact nobody can trace back to
	// how it was made is an artifact nobody should trust.
	method := captureScreenshot(dir, query, budget)

	result := map[string]any{
		"task":           task,
		"query":          query,
		"steps":          steps,
		"completed_at":   time.Now().UTC().Format(time.RFC3339),
		"agent":          "airlock-agent/" + version,
		"screenshot_via": method,
	}
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "result.json"), encoded, 0o640); err != nil {
		return err
	}

	logf("saved screenshot.png (via %s) and result.json", method)
	return nil
}

// writeGeneratedScreenshot renders a PNG without a browser and writes it to
// path. The fallback path, used when no browser is installed or the capture
// failed; the run still produces a real binary artifact so everything
// downstream of it is exercised.
func writeGeneratedScreenshot(path, query string) error {
	shot, err := renderScreenshot(query)
	if err != nil {
		return fmt.Errorf("render screenshot: %w", err)
	}
	return os.WriteFile(path, shot, 0o640)
}

// renderScreenshot produces a real PNG.
//
// A real image rather than a text file with a .png extension: it means the
// artifact pipeline is genuinely carrying binary content, and that content-type
// sniffing, byte-exact storage and inline rendering are all exercised for real.
func renderScreenshot(seed string) ([]byte, error) {
	const width, height = 320, 180

	img := image.NewRGBA(image.Rect(0, 0, width, height))

	// A deterministic gradient, tinted by the query, so two runs of the same
	// task produce identical bytes and a different query is visibly different.
	var sum int
	for _, r := range seed {
		sum += int(r)
	}
	rng := rand.New(rand.NewSource(int64(sum)))
	tint := uint8(rng.Intn(128))

	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			img.Set(x, y, color.RGBA{
				R: uint8(x * 255 / width),
				G: uint8(y * 255 / height),
				B: tint,
				A: 255,
			})
		}
	}

	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// logf writes a progress line. Unbuffered and flushed per line, because the
// platform tails this stream and buffered output would arrive in a clump at
// exit — which is exactly when it stops being useful.
func logf(format string, args ...any) {
	fmt.Fprintf(os.Stdout, "%s "+format+"\n",
		append([]any{time.Now().UTC().Format(time.RFC3339)}, args...)...)
}

func errorf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "%s "+format+"\n",
		append([]any{time.Now().UTC().Format(time.RFC3339)}, args...)...)
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func envOr(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func envIntOr(key string, fallback int) int {
	if v, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(v); err == nil {
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
