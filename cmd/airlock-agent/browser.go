package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// Real browser capture.
//
// The agent drives a headless Chromium when one is present and falls back to a
// generated image when it is not. Both paths are honest about which happened,
// in the logs and in result.json, because "the screenshot looks wrong" and "the
// screenshot was never a screenshot" are very different investigations.
//
// Why a fallback exists at all: the process driver runs the agent directly on
// the host, and `make test` must stay fast and dependency-free. Requiring a
// 400 MB browser to run the unit suite would be a bad trade. The container
// image ships Chromium, so the path anyone actually runs is the real one.
//
// Chromium is driven through its command line rather than a CDP library. That
// keeps the agent's dependency list at zero for this feature, and the two
// things we need - load a page, write a PNG - are exactly what the flags do.
// A richer agent (clicking, typing, multi-step flows) is where a CDP library
// such as chromedp starts earning its dependency.

// browserNames are the binaries to look for, in order. Alpine installs
// `chromium-browser`; Debian and most desktops install `chromium` or
// `google-chrome`.
var browserNames = []string{
	"chromium-browser",
	"chromium",
	"google-chrome",
	"google-chrome-stable",
}

// findBrowser locates a usable browser, honouring an explicit override first.
func findBrowser() string {
	if explicit := os.Getenv("AIRLOCK_BROWSER"); explicit != "" {
		if path, err := exec.LookPath(explicit); err == nil {
			return path
		}
		// An explicit request that cannot be satisfied is worth saying out loud
		// rather than silently falling back to a different browser.
		logf("warning: AIRLOCK_BROWSER=%q not found on PATH", explicit)
	}
	for _, name := range browserNames {
		if path, err := exec.LookPath(name); err == nil {
			return path
		}
	}
	return ""
}

// searchURL builds the page to load for a query.
func searchURL(query string) string {
	if explicit := os.Getenv("AIRLOCK_URL"); explicit != "" {
		return explicit
	}
	// example.com by default: stable, tiny, and it never bot-challenges.
	//
	// Worth recording why that last part matters. Pointed at a general search
	// engine, this agent reliably captured a CAPTCHA - a real screenshot of a
	// real page, and useless. Headless browsers arriving from unfamiliar
	// addresses get challenged, which is exactly the problem the egress
	// rotation exists to address, and a reminder that the hard half of IP
	// rotation is the reputation of the addresses, not the routing code.
	//
	// Point AIRLOCK_URL at a search engine (with a proxy pool behind it) when
	// that is what you want to exercise.
	return "https://example.com/?q=" + url.QueryEscape(query)
}

// capture drives the browser to load target and write a PNG to dest.
//
// Returns the browser's own diagnostic output on failure, because Chromium
// reports "could not connect" and "crashed on startup" the same way to an exit
// code and differently in its stderr.
func capture(ctx context.Context, browser, target, dest string) error {
	profile, err := os.MkdirTemp(scratchDir(), "chrome-profile-")
	if err != nil {
		return fmt.Errorf("create browser profile dir: %w", err)
	}
	defer os.RemoveAll(profile)

	args := []string{
		"--headless=new",

		// The container is the sandbox. Chromium's own sandbox needs user
		// namespaces and capabilities that this platform deliberately drops,
		// so it cannot start with it enabled. Dropping it here is safe
		// precisely because everything outside is already locked down: no
		// capabilities, read-only root, non-root uid, no network route except
		// the proxy.
		"--no-sandbox",
		"--disable-gpu",

		// /dev/shm is 64 MB in a default container and Chromium will exhaust it
		// on any real page, crashing with an error that names neither shared
		// memory nor the real cause.
		"--disable-dev-shm-usage",

		"--hide-scrollbars",
		"--window-size=1280,800",
		"--user-data-dir=" + profile,
		"--no-first-run",
		"--no-default-browser-check",
		"--disable-extensions",

		// Lets the page settle, then exits on its own. Without a budget,
		// headless Chromium screenshots whatever is painted the instant the
		// document loads, which on most pages is nothing.
		"--virtual-time-budget=8000",

		"--screenshot=" + dest,
	}

	// Chromium does not reliably read HTTP_PROXY, so the platform's proxy is
	// passed as a flag. This is the line that makes egress rotation real for a
	// browser: whichever proxy the control plane selected for this job is the
	// one the browser dials through, and on an internal network it is the only
	// route out at all.
	if proxy := firstNonEmpty(os.Getenv("HTTPS_PROXY"), os.Getenv("HTTP_PROXY")); proxy != "" {
		args = append(args, "--proxy-server="+proxy)
		logf("browser egressing via %s", proxy)
	}

	args = append(args, target)

	cmd := exec.CommandContext(ctx, browser, args...)
	// HOME must be writable: Chromium writes crash and preference state there
	// regardless of --user-data-dir, and the root filesystem is read-only.
	cmd.Env = append(os.Environ(), "HOME="+scratchDir())

	var diag strings.Builder
	cmd.Stdout = &diag
	cmd.Stderr = &diag

	if err := cmd.Run(); err != nil {
		detail := strings.TrimSpace(diag.String())
		if len(detail) > 800 {
			detail = detail[:800] + " ...(truncated)"
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("browser timed out loading %s: %s", target, detail)
		}
		return fmt.Errorf("browser failed: %w: %s", err, detail)
	}

	// Chromium exits 0 even when it wrote nothing, so the file is the only
	// proof the capture happened.
	info, err := os.Stat(dest)
	if err != nil {
		return fmt.Errorf("browser exited cleanly but wrote no screenshot: %w", err)
	}
	if info.Size() == 0 {
		return errors.New("browser wrote an empty screenshot")
	}
	return nil
}

// scratchDir is a writable directory. The platform guarantees /tmp is a tmpfs
// even when the root filesystem is read-only; os.TempDir honours TMPDIR for the
// process driver, where there is no container and no such guarantee.
func scratchDir() string {
	if dir := os.Getenv("TMPDIR"); dir != "" {
		return dir
	}
	if _, err := os.Stat("/tmp"); err == nil {
		return "/tmp"
	}
	return os.TempDir()
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// captureScreenshot writes screenshot.png, using a real browser when one is
// available and a generated image otherwise.
//
// Returns how the image was produced, which is recorded in result.json so an
// artifact can always be traced back to what made it.
func captureScreenshot(dir, query string, budget time.Duration) (method string) {
	dest := filepath.Join(dir, "screenshot.png")

	browser := findBrowser()
	if browser == "" {
		logf("no browser found on PATH; generating a placeholder image instead")
		if err := writeGeneratedScreenshot(dest, query); err != nil {
			errorf("could not write placeholder screenshot: %v", err)
			return "none"
		}
		return "generated"
	}

	logf("launching %s", browser)
	target := searchURL(query)
	logf("navigating to %s", target)

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	if err := capture(ctx, browser, target, dest); err != nil {
		// A browser that cannot reach the page is a real and interesting
		// outcome - on an internal network with no proxy it is the expected
		// one - so it is reported rather than swallowed. The run still
		// produces an artifact so the pipeline downstream is exercised either
		// way, and result.json records that it was a fallback.
		errorf("browser capture failed: %v", err)
		logf("falling back to a generated image")
		if err := writeGeneratedScreenshot(dest, query); err != nil {
			errorf("could not write placeholder screenshot: %v", err)
			return "none"
		}
		return "generated-after-browser-failure"
	}

	if info, err := os.Stat(dest); err == nil {
		logf("captured screenshot.png (%d bytes) from %s", info.Size(), target)
	}
	return "browser"
}
