package fargate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Store is the real ObjectStore, backed by S3.
//
// It does two things, and the split matters. Presigning produces a URL the
// agent can PUT to holding no credentials at all - that is what allows the job
// task role in the Terraform to be genuinely empty rather than "narrow", which
// is a much easier property to defend. Getting is done by the control plane
// with its own role, which does have S3 read on this bucket.
//
// The bucket is never listed and keys are never derived from anything the
// payload controls: the key comes from the job ID the control plane issued, so
// an agent cannot aim its upload at another job's prefix.
type S3Store struct {
	client  *s3.Client
	presign *s3.PresignClient
	bucket  string
}

// NewS3Store builds an ObjectStore over a bucket.
func NewS3Store(client *s3.Client, bucket string) *S3Store {
	return &S3Store{
		client:  client,
		presign: s3.NewPresignClient(client),
		bucket:  bucket,
	}
}

// PresignPut returns a URL that accepts exactly one object at key, for expires.
//
// The URL is a bearer token: anyone holding it can write that key until it
// expires. That is acceptable because it is scoped to one key inside one job's
// prefix, it is handed only to the task that owns that job, and the worst a
// leaked one buys is the ability to overwrite that job's own artifacts.
func (s *S3Store) PresignPut(ctx context.Context, key string, expires time.Duration) (string, error) {
	req, err := s.presign.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(expires))
	if err != nil {
		return "", fmt.Errorf("presign put %s: %w", key, err)
	}
	return req.URL, nil
}

// Get downloads an object, reporting ErrNoArtifacts when it was never uploaded.
//
// A missing object is the ordinary case for a job that produced nothing or
// crashed before uploading, so it is translated into a sentinel the caller can
// treat as "nothing to collect" rather than an error to alarm on.
func (s *S3Store) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var missing *s3types.NoSuchKey
		if errors.As(err, &missing) {
			return nil, ErrNoArtifacts
		}
		// A bucket that 404s the key surfaces as NotFound rather than NoSuchKey
		// when the caller lacks s3:ListBucket, which is the normal least
		// privilege posture. Both mean the same thing here.
		var notFound *s3types.NotFound
		if errors.As(err, &notFound) {
			return nil, ErrNoArtifacts
		}
		return nil, fmt.Errorf("get %s: %w", key, err)
	}
	defer out.Body.Close()

	// Bounded read. The object is written by the payload through a presigned
	// URL, so its size is attacker-influenced, and an unbounded ReadAll here
	// would turn a large upload into control-plane memory exhaustion.
	const maxArtifactBytes = 512 << 20 // 512 MiB
	body, err := io.ReadAll(io.LimitReader(out.Body, maxArtifactBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", key, err)
	}
	if int64(len(body)) > maxArtifactBytes {
		return nil, fmt.Errorf("artifact archive for %s exceeds %d bytes", key, int64(maxArtifactBytes))
	}
	return body, nil
}
