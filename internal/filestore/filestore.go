// Package filestore is the COPY build-context object store: the e2b client
// uploads a COPY context (a gzipped tar) straight to an S3/OBS bucket via a
// presigned PUT, and the build sandbox fetches it via a presigned GET. The
// orchestrator only ever PRESIGNS and HEADs — bytes never transit the control
// plane. This is the sole aws-sdk dependency in the orchestrator; everything
// else is dependency-light (the build unit fetches via plain net/http on the
// presigned GET URL, so the sdk stays out of run-builder's runtime path).
package filestore

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
)

// Store presigns and probes objects in one bucket.
type Store struct {
	cli     *s3.Client
	presign *s3.PresignClient
	bucket  string
	prefix  string        // cleaned, no surrounding slashes
	putTTL  time.Duration // upload URL lifetime (client uploads immediately)
}

// New builds a Store from the files_storage config. Static access_key/secret_key
// take precedence; empty falls back to the AWS default chain (env / instance role).
func New(c *config.FilesStorageConfig) (*Store, error) {
	if c.Bucket == "" {
		return nil, fmt.Errorf("filestore: bucket is required")
	}
	var opts []func(*awsconfig.LoadOptions) error
	if c.Region != "" {
		opts = append(opts, awsconfig.WithRegion(c.Region))
	}
	if c.AccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(c.AccessKey, c.SecretKey, "")))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("filestore: load aws config: %w", err)
	}
	cli := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if c.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.Endpoint)
		}
		o.UsePathStyle = c.ForcePathStyle
	})
	return &Store{
		cli:     cli,
		presign: s3.NewPresignClient(cli),
		bucket:  c.Bucket,
		prefix:  strings.Trim(c.Prefix, "/"),
		putTTL:  c.PresignExpiryDur(),
	}, nil
}

// key lays out the object: {prefix}/files/{aaaa}/{bb}/{uuid}/{hash}. The uuid
// is the templateID's uuidv7 part (transient- stripped); since uuidv7 leads
// with the millisecond timestamp, aaaa = uuid[0:4] buckets at ~50-day and
// bb = uuid[4:6] at ~5-hour granularity — spreading objects across prefixes
// (no hot prefix) and giving lifecycle/GC a natural time order.
func (s *Store) key(templateID, hash string) string {
	uuid := strings.TrimPrefix(templateID, "transient-")
	aaaa, bb := "0000", "00"
	if len(uuid) >= 6 {
		aaaa, bb = uuid[0:4], uuid[4:6]
	}
	return path.Join(s.prefix, "files", aaaa, bb, uuid, hash)
}

// PresignPut returns a presigned PUT URL for the client to upload the COPY
// context to (host-only signature + UNSIGNED-PAYLOAD: the client may PUT any
// body / content-type, matching the e2b SDK's raw-bytes PUT).
func (s *Store) PresignPut(ctx context.Context, templateID, hash string) (string, error) {
	req, err := s.presign.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(templateID, hash)),
	}, func(o *s3.PresignOptions) { o.Expires = s.putTTL })
	if err != nil {
		return "", fmt.Errorf("filestore: presign put: %w", err)
	}
	return req.URL, nil
}

// PresignGet returns a presigned GET URL the build sandbox fetches the context
// from. ttl must cover the whole build (it is signed when the build starts).
func (s *Store) PresignGet(ctx context.Context, templateID, hash string, ttl time.Duration) (string, error) {
	req, err := s.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(templateID, hash)),
	}, func(o *s3.PresignOptions) { o.Expires = ttl })
	if err != nil {
		return "", fmt.Errorf("filestore: presign get: %w", err)
	}
	return req.URL, nil
}

// Exists reports whether the COPY context object is already in the bucket
// (the files endpoint's `present`; lets the client skip re-upload).
func (s *Store) Exists(ctx context.Context, templateID, hash string) (bool, error) {
	_, err := s.cli.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(templateID, hash)),
	})
	if err == nil {
		return true, nil
	}
	if isNotFound(err) {
		return false, nil
	}
	return false, fmt.Errorf("filestore: head object: %w", err)
}

// isNotFound recognizes the 404 across S3 implementations: HeadObject yields no
// typed NoSuchKey (no body), so match the smithy error code ("NotFound" on AWS,
// "NoSuchKey" on some gateways including versitygw).
func isNotFound(err error) bool {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "NotFound", "NoSuchKey", "404":
			return true
		}
	}
	return false
}
