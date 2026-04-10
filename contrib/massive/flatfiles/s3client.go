package main

import (
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
//
// dataType should be "day_aggs_v1" or "minute_aggs_v1".
func (c *S3Client) Download(ctx context.Context, dataType string, date time.Time) (io.ReadCloser, error) {
	key := c.objectKey(dataType, date)

	resp, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("download s3://%s/%s: %w", c.bucket, key, err)
	}

	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		resp.Body.Close()
		return nil, fmt.Errorf("decompress s3://%s/%s: %w", c.bucket, key, err)
	}

	return &gzipReadCloser{gz: gz, body: resp.Body}, nil
}

// objectKey builds the S3 key for a flat file.
// Example: us_stocks_sip/day_aggs_v1/2025/01/2025-01-02.csv.gz
func (c *S3Client) objectKey(dataType string, date time.Time) string {
	return fmt.Sprintf("%s/%s/%d/%02d/%s.csv.gz",
		c.prefix, dataType, date.Year(), date.Month(), date.Format("2006-01-02"))
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
