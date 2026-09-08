// Package archive turns an untrusted pile of files into a list of artifacts,
// safely.
//
// It exists as its own package because every driver needs it and it is where
// the security bugs live. Whatever produced the bytes - a tar stream out of a
// container, a directory an agent wrote into, an archive an agent uploaded to
// object storage - the content is written by the untrusted payload. Entries can
// carry absolute paths, "..", or symlinks pointing anywhere on the host, and a
// naive extractor writes through all three.
//
// One implementation, used by every driver, so a fix here fixes all of them and
// no driver can quietly grow a weaker copy.
package archive

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bytes-as/airlock/internal/driver"
)

// Extract unpacks a tar stream into dest.
//
// Written as an exported pure function because it is where the security bugs
// live. A tar stream from a container is untrusted input: entries can carry
// absolute paths, "..", or symlinks pointing outside the destination, and a
// naive extractor will happily write through all three. This one refuses.
func Extract(ctx context.Context, r io.Reader, dest, stripPrefix string) ([]driver.Artifact, error) {
	absDest, err := filepath.Abs(dest)
	if err != nil {
		return nil, err
	}

	var artifacts []driver.Artifact
	reader := tar.NewReader(r)

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("docker driver: read archive: %w", err)
		}

		// Docker prefixes entries with the copied directory's name; strip it so
		// artifact names are relative to the artifact directory itself.
		name := header.Name
		if stripPrefix != "" {
			name = strings.TrimPrefix(name, stripPrefix)
		}
		name = strings.TrimPrefix(name, "./")
		if name == "" || name == "." {
			continue
		}

		switch header.Typeflag {
		case tar.TypeDir:
			continue // Directories are created as needed by their entries.

		case tar.TypeSymlink, tar.TypeLink:
			// Not followed, not recreated. A symlink in an artifact set has no
			// legitimate use here and every illegitimate one: pointing it at
			// /etc/passwd turns "collect the screenshots" into host file read.
			continue

		case tar.TypeReg:
			// fall through

		default:
			continue // devices, fifos and the rest have no business here
		}

		target, err := safeJoin(absDest, name)
		if err != nil {
			// Refuse rather than skip quietly: a traversal attempt is a signal,
			// not a formatting quirk.
			return nil, fmt.Errorf("docker driver: archive entry %q: %w", header.Name, err)
		}

		if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
			return nil, err
		}
		written, err := writeFile(target, reader, header.Size)
		if err != nil {
			return nil, fmt.Errorf("docker driver: extract %s: %w", name, err)
		}

		artifacts = append(artifacts, driver.Artifact{
			Name:        filepath.ToSlash(name),
			Path:        target,
			Size:        written,
			ContentType: contentTypeOf(name),
		})
	}

	sort.Slice(artifacts, func(i, j int) bool { return artifacts[i].Name < artifacts[j].Name })
	return artifacts, nil
}

// collectDir copies regular files from src into dest.
//
// Symlinks are skipped rather than followed, for the same reason ExtractTar
// skips them: the agent writes into this directory, so a symlink here is
// attacker-controlled and pointing one at /etc/passwd would turn "collect the
// screenshots" into a host file read.
func CollectDir(ctx context.Context, src, dest string) ([]driver.Artifact, error) {
	var artifacts []driver.Artifact

	err := filepath.WalkDir(src, func(p string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		// Type() reports the mode bits without following the link, so a symlink
		// is identified as one rather than as whatever it points at.
		if entry.Type()&os.ModeSymlink != 0 || !entry.Type().IsRegular() {
			return nil
		}

		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(rel)

		target, err := safeJoin(dest, name)
		if err != nil {
			return nil // Refuses anything escaping dest.
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
			return err
		}

		info, err := entry.Info()
		if err != nil {
			return err
		}
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		written, err := writeFile(target, f, info.Size())
		f.Close()
		if err != nil {
			return fmt.Errorf("docker driver: collect %s: %w", name, err)
		}

		artifacts = append(artifacts, driver.Artifact{
			Name:        name,
			Path:        target,
			Size:        written,
			ContentType: contentTypeOf(name),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(artifacts, func(i, j int) bool { return artifacts[i].Name < artifacts[j].Name })
	return artifacts, nil
}

// safeJoin resolves name inside root, refusing anything that escapes.
func safeJoin(root, name string) (string, error) {
	if filepath.IsAbs(name) || strings.HasPrefix(name, "/") || strings.HasPrefix(name, `\`) {
		return "", errors.New("absolute paths are not permitted")
	}
	joined := filepath.Clean(filepath.Join(root, filepath.FromSlash(name)))
	if joined != root && !strings.HasPrefix(joined, root+string(os.PathSeparator)) {
		return "", errors.New("path escapes the destination directory")
	}
	return joined, nil
}

// writeFile copies exactly size bytes, refusing a header that lies about length.
func writeFile(target string, r io.Reader, size int64) (int64, error) {
	f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	// LimitReader bounds the copy by the declared size, so a stream that keeps
	// producing bytes past its header cannot fill the disk.
	written, err := io.Copy(f, io.LimitReader(r, size))
	if err != nil {
		return written, err
	}
	return written, nil
}

func contentTypeOf(name string) string {
	if t := mime.TypeByExtension(path.Ext(name)); t != "" {
		return t
	}
	return "application/octet-stream"
}
