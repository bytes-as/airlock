package main

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// Artifact upload, for environments whose filesystem the control plane cannot
// read.
//
// On the docker driver the control plane collects artifacts by reading the
// directory it bind-mounted, and none of this runs. On Fargate there is no such
// path: the task's filesystem is gone the moment the task is, and there is no
// `docker cp` for a Fargate task. So the agent packs its own output and PUTs it
// to a presigned URL the control plane generated for exactly this job.
//
// The agent therefore holds no AWS credentials and needs no S3 permission,
// which is what lets the job task role in the Terraform stay completely empty.
// A presigned URL is a bearer token for writing one key, and that key is inside
// this job's own prefix, so the worst a leaked one buys is the ability to
// overwrite this job's own artifacts.
//
// The known gap, stated plainly because it is the one that will bite: this runs
// inside the agent, so an agent killed hard - SIGKILL, an OOM kill, a wedged
// kernel - uploads nothing, and that is precisely the run whose output would
// have been most worth having. The fix is a sidecar that uploads on task exit
// regardless of how the agent died. It is named in the README as the next step
// rather than quietly omitted.

// uploadTimeout bounds the PUT. Generous, because artifacts can be large and a
// slow upload is better than a lost one, but finite: an agent wedged on a
// network write would otherwise outlive its own deadline.
const uploadTimeout = 2 * time.Minute

// maxArtifactBytes caps what the agent will pack. The control plane refuses
// oversized archives on download too; refusing here as well means a runaway job
// fails fast with a clear message instead of after a long upload.
const maxArtifactBytes = 512 << 20 // 512 MiB

// uploadArtifacts tars dir and PUTs it to url.
//
// Returns nil when there is nothing to do - no URL configured (the docker
// driver's case) or no directory - so callers can invoke it unconditionally on
// every exit path.
func uploadArtifacts(dir, url string) error {
	if url == "" || dir == "" {
		return nil
	}
	if _, err := os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("stat artifact dir: %w", err)
	}

	archive, count, err := tarDir(dir)
	if err != nil {
		return fmt.Errorf("pack artifacts: %w", err)
	}
	if count == 0 {
		// Nothing produced. Uploading an empty archive would be indistinguishable
		// from a real one on the other side, so send nothing at all and let the
		// control plane report "no artifacts".
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), uploadTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(archive))
	if err != nil {
		return fmt.Errorf("build upload request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-tar")
	// Set explicitly: S3 rejects a presigned PUT sent with chunked encoding.
	req.ContentLength = int64(len(archive))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("upload artifacts: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		// Read a little of the body: S3 explains refusals in XML, and the
		// explanation is the difference between "the URL expired" and "the
		// bucket policy denies you".
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		// The URL carries a signature; log the status and S3's reason, never
		// the URL itself.
		return fmt.Errorf("upload rejected with %s: %s", resp.Status, bytes.TrimSpace(detail))
	}

	logf("uploaded %d artifact(s), %d bytes", count, len(archive))
	return nil
}

// tarDir packs every regular file under dir, returning the archive and how many
// files went into it.
//
// Symlinks are skipped rather than followed. The control plane's extractor
// refuses them on the way in as well, but packing one here would either leak a
// host file into the archive or produce an entry the far side discards, and
// neither is worth doing.
func tarDir(dir string) ([]byte, int, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	count := 0

	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if entry.Type()&fs.ModeSymlink != 0 || !entry.Type().IsRegular() {
			return nil
		}

		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if int64(buf.Len())+info.Size() > maxArtifactBytes {
			return fmt.Errorf("artifacts exceed %d bytes", int64(maxArtifactBytes))
		}

		header := &tar.Header{
			Name:    filepath.ToSlash(rel),
			Mode:    0o644,
			Size:    info.Size(),
			ModTime: info.ModTime(),
		}
		if err := tw.WriteHeader(header); err != nil {
			return err
		}

		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		// LimitReader bounds the copy at the size declared in the header: a
		// file growing while it is read would otherwise desynchronise the
		// archive and corrupt every entry after it.
		if _, err := io.Copy(tw, io.LimitReader(f, info.Size())); err != nil {
			return err
		}
		count++
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	if err := tw.Close(); err != nil {
		return nil, 0, err
	}
	return buf.Bytes(), count, nil
}
