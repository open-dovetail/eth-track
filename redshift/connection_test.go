package redshift

// Run all unit test: `go test -v`

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func setup() error {
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
		secretName = "dev/Redshift"
	}
	secret, err := GetAWSSecret(secretName, profile, region)
	if err != nil {
		return errors.Wrapf(err, "Failed to get redshift secret for profile %s", profile)
	}
	dbName, ok := os.LookupEnv("AWS_REDSHIFT")
	if !ok {
		dbName = "dev"
	}
	if _, err := Connect(secret, dbName, 10); err != nil {
		return errors.Wrapf(err, "Failed to connect to redshift db %s", dbName)
	}
	return nil
}

func TestMain(m *testing.M) {
	if err := setup(); err != nil {
		fmt.Printf("FAILED %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Setup connected to redshift", db.url)
	status := m.Run()

	fmt.Println("Cleanup redshift connections")
	Close()
	os.Exit(status)
}

func TestRedshiftConnection(t *testing.T) {
	var sd time.Time
	rows, err := db.Query("select SYSDATE")
	assert.NoError(t, err, "query should not return error")
	ScanRow(rows, &sd)
	fmt.Println("query result", sd)
}
