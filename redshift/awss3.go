package redshift

import (
	"bytes"
	"context"
	"io/ioutil"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/golang/glog"
)

// AWS managed secret for redshift db user & password
type s3Bucket struct {
	name     string
	region   string
	copyRole string
	ctx      context.Context
	client   *s3.Client
}

var bucket *s3Bucket

// initialize or return s3 bucket
func GetS3Bucket(bucketName, profile, region, copyRole string) (*s3Bucket, error) {
	if bucket != nil {
		return bucket, nil
	}

	// S3 client with AWS profile, region, and default config/credential specified in ~/.aws
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithSharedConfigProfile(profile))
	if err != nil {
		// Handle session creation error
		glog.Errorf("Failed to get config for AWS region %s and profile %s: %s", region, profile, err.Error())
		return nil, err
	}
	bucket = &s3Bucket{
		name:     bucketName,
		region:   region,
		copyRole: copyRole,
		ctx:      ctx,
		client:   s3.NewFromConfig(cfg),
	}
	return bucket, nil
}

func deleteS3File(name string) (*s3.DeleteObjectOutput, error) {
	return bucket.client.DeleteObject(bucket.ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket.name),
		Key:    aws.String(name),
	})
}

func deleteS3Folder(name string) (*s3.DeleteObjectsOutput, error) {
	prefix := name
	if name[len(name)-1:] != "/" {
		prefix += "/"
	}
	list, err := bucket.client.ListObjectsV2(bucket.ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket.name),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return nil, err
	}
	if list.IsTruncated {
		// returned max of 1000, some still remaining
	}
	var items []types.ObjectIdentifier
	for _, v := range list.Contents {
		items = append(items, types.ObjectIdentifier{Key: v.Key})
	}
	if len(items) == 0 {
		return nil, nil
	}
	return bucket.client.DeleteObjects(bucket.ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket.name),
		Delete: &types.Delete{Objects: items},
	})
}

func writeS3File(name string, content []byte) (*s3.PutObjectOutput, error) {
	return bucket.client.PutObject(bucket.ctx, &s3.PutObjectInput{
		Bucket:             aws.String(bucket.name),
		Key:                aws.String(name),
		Body:               bytes.NewReader(content),
		ContentType:        aws.String("text/plain"),
		ContentDisposition: aws.String("attachment"),
	})
}

func readS3File(name string) ([]byte, error) {
	resp, err := bucket.client.GetObject(bucket.ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket.name),
		Key:    aws.String(name),
	})
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(resp.Body)
}
