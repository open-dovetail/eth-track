package redshift

// Run all unit test: `go test -v`

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAWSSecret(t *testing.T) {
	profile, ok := os.LookupEnv("AWS_PROFILE")
	if !ok {
		profile = "oocto"
	}
	region, ok := os.LookupEnv("AWS_REGION")
	if !ok {
		region = "us-west-2"
	}
	secretName, ok := os.LookupEnv("AWS_SECRET")
	if !ok {
		secretName = "dev/ethdb/Redshift"
	}
	secret, err := GetAWSSecret(secretName, profile, region)
	assert.NoError(t, err, "get AWS secret should not throw error")
	assert.NotNil(t, secret, "AWS secret should not be nil")
	assert.Equal(t, "redshift", secret.Engine, "returned secret should be for engine 'redshift'")
	//fmt.Println(secret)
}
