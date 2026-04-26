package flatfiles

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/alpacahq/marketstore/v4/contrib/massive/massiveconfig"
)

// S3Client wraps an S3 client configured for the Massive flat files endpoint.
type S3Client struct {
	client *s3.Client
	bucket string
	prefix string
}

// NewS3Client creates a new S3 client for downloading Massive flat files.
// It uses the hardcoded endpoint, bucket, and prefix defaults from massiveconfig.
func NewS3Client(accessKey, secretKey string) (*S3Client, error) {
	if accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("S3 access key and secret key are required")
	}

	cfg := aws.Config{
		Region: "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider(
			accessKey,
			secretKey,
			"",
		),
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(massiveconfig.DefaultS3Endpoint)
		o.UsePathStyle = true
	})

	return &S3Client{
		client: client,
		bucket: massiveconfig.DefaultS3Bucket,
		prefix: massiveconfig.DefaultS3Prefix,
	}, nil
}

// Download fetches the gzipped CSV for a data type and date, returning a
// decompressed reader. The caller must close the returned ReadCloser.
// It uses the client's default S3 prefix (e.g., "us_stocks_sip").
//
// dataType should be "day_aggs_v1" or "minute_aggs_v1".
func (c *S3Client) Download(ctx context.Context, dataType string, date time.Time) (io.ReadCloser, error) {
	return c.DownloadWithPrefix(ctx, c.prefix, dataType, date)
}

// DownloadWithPrefix fetches the gzipped CSV for a data type and date from
// the given S3 key prefix, returning a decompressed reader. The caller must
// close the returned ReadCloser.
//
// The entire compressed response is buffered into memory before
// decompression so that the HTTP connection is released immediately.
// This prevents mid-parse "connection reset by peer" errors on large
// files (e.g. 1Min data) where streaming would hold the TCP connection
// open for the entire CSV parse duration.
//
// prefix should be "us_stocks_sip" or "us_indices".
// dataType should be "day_aggs_v1" or "minute_aggs_v1".
func (c *S3Client) DownloadWithPrefix(ctx context.Context, prefix, dataType string, date time.Time) (io.ReadCloser, error) {
	key := c.objectKey(prefix, dataType, date)

	resp, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("download s3://%s/%s: %w", c.bucket, key, err)
	}

	// Buffer the entire compressed body into memory so the HTTP
	// connection is closed before we start decompressing and parsing.
	compressed, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("download s3://%s/%s: %w", c.bucket, key, err)
	}

	gz, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, fmt.Errorf("decompress s3://%s/%s: %w", c.bucket, key, err)
	}

	return &gzipReadCloser{gz: gz, body: io.NopCloser(bytes.NewReader(nil))}, nil
}

// objectKey builds the S3 key for a flat file.
// Example: us_stocks_sip/day_aggs_v1/2025/01/2025-01-02.csv.gz
// Example: us_indices/day_aggs_v1/2025/01/2025-01-02.csv.gz
func (c *S3Client) objectKey(prefix, dataType string, date time.Time) string {
	return fmt.Sprintf("%s/%s/%d/%02d/%s.csv.gz",
		prefix, dataType, date.Year(), date.Month(), date.Format("2006-01-02"))
}

// gzipReadCloser wraps a gzip reader and the underlying S3 response body,
// ensuring both are closed when the reader is closed.
type gzipReadCloser struct {
	gz   *gzip.Reader
	body io.ReadCloser
}

func (r *gzipReadCloser) Read(p []byte) (int, error) {
	return r.gz.Read(p)
}

func (r *gzipReadCloser) Close() error {
	gzErr := r.gz.Close()
	bodyErr := r.body.Close()
	if gzErr != nil {
		return gzErr
	}
	return bodyErr
}
