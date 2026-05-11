package tools

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// r2Client builds an S3-compatible client targeting the R2 endpoint from env.
func r2Client() (*s3.Client, error) {
	endpoint := os.Getenv("R2_ENDPOINT_URL")
	if endpoint == "" {
		return nil, fmt.Errorf("R2_ENDPOINT_URL is not set")
	}
	accessKey := os.Getenv("R2_ACCESS_KEY_ID")
	secretKey := os.Getenv("R2_SECRET_ACCESS_KEY")
	if accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("R2_ACCESS_KEY_ID or R2_SECRET_ACCESS_KEY is not set")
	}

	cfg := aws.Config{
		Region:      "auto",
		Credentials: credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	return client, nil
}

// r2ClientFromConfig builds an S3-compatible client from explicit config. Used in tests.
func r2ClientFromConfig(endpoint, accessKey, secretKey string) *s3.Client {
	cfg := aws.Config{
		Region:      "auto",
		Credentials: credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
}

// mustEnv returns the env var value or an error if it is empty.
func mustEnv(name string) (string, error) {
	v := os.Getenv(name)
	if v == "" {
		return "", fmt.Errorf("environment variable %s is not set", name)
	}
	return v, nil
}

// ETagMismatchError is returned when a conditional write fails due to an ETag conflict.
type ETagMismatchError struct {
	Expected string
}

func (e *ETagMismatchError) Error() string {
	return fmt.Sprintf("etag mismatch: expected %q", e.Expected)
}

// isETagMismatch checks whether an AWS S3 error is a precondition failure (412).
func isETagMismatch(err error) bool {
	if err == nil {
		return false
	}
	type httpStatusCoder interface {
		HTTPStatusCode() int
	}
	type unwrapper interface {
		Unwrap() error
	}
	cur := err
	for cur != nil {
		if h, ok := cur.(httpStatusCoder); ok && h.HTTPStatusCode() == 412 {
			return true
		}
		if u, ok := cur.(unwrapper); ok {
			cur = u.Unwrap()
		} else {
			break
		}
	}
	return false
}

// listBucketObjects paginates through all objects under a prefix and returns them.
func listBucketObjects(ctx context.Context, client *s3.Client, bucket, prefix string) ([]types.Object, error) {
	var results []types.Object
	var continuationToken *string

	for {
		out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, err
		}
		results = append(results, out.Contents...)
		if out.IsTruncated == nil || !*out.IsTruncated {
			break
		}
		continuationToken = out.NextContinuationToken
	}
	return results, nil
}

// derefString safely dereferences a *string, returning "" if nil.
func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// derefInt64 safely dereferences a *int64, returning 0 if nil.
func derefInt64(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}
