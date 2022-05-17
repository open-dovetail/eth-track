package redshift

// Run all unit test: `go test -v`

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriteS3Files(t *testing.T) {
	profile, ok := os.LookupEnv("AWS_PROFILE")
	if !ok {
		profile = "oocto"
	}
	region, ok := os.LookupEnv("AWS_REGION")
	if !ok {
		region = "us-west-2"
	}
	bucketName, ok := os.LookupEnv("AWS_S3BUCKET")
	if !ok {
		bucketName = "dev-eth-track"
	}
	copyRole, ok := os.LookupEnv("AWS_COPY_ROLE")
	if !ok {
		copyRole = "arn:aws:iam::436486865631:role/redshift-eth-track-copy"
	}

	bucket, err := GetS3Bucket(bucketName, profile, region, copyRole)
	assert.NoError(t, err, "get S3 bucket should not throw error")
	assert.NotNil(t, bucket, "S3 bucket should not be nil")

	testData := "hello|world|123"
	testFolder := "test"
	testFile1 := testFolder + "/test1.csv"
	testFile2 := testFolder + "/test2.csv"
	out, err := writeS3File(testFile1, []byte(testData))
	assert.NoError(t, err, "write S3 file should not throw error")
	assert.NotNil(t, out, "output from write S3 file should not be nil")
	out, err = writeS3File(testFile2, []byte(testData))

	data, err := readS3File(testFile1)
	assert.NoError(t, err, "read S3 file should not throw error")
	assert.Equal(t, testData, string(data), "downloaded s3 file should match test data")

	data, err = readS3File(testFile2)
	assert.NoError(t, err, "read S3 file should not throw error")
	assert.Equal(t, testData, string(data), "downloaded s3 file should match test data")

	_, err = deleteS3Folder(testFolder)
	assert.NoError(t, err, "delete S3 folder should not throw error")
}
