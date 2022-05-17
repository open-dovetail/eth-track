package proc

// Run all unit test: `go test -v`

import (
	"fmt"
	"os"
	"testing"

	"github.com/open-dovetail/eth-track/redshift"
	"github.com/pkg/errors"
)

// initialize Ethereum node connection
func setup() error {
	// config etherscan connection
	apiKey, ok := os.LookupEnv("ETHERSCAN_APIKEY")
	if !ok {
		return fmt.Errorf("ETHERSCAN_APIKEY env must be defined")
	}
	fmt.Println("ETHERSCAN_APIKEY:", apiKey)
	ConfigEtherscan(apiKey, 350)

	// config infura etherereum node
	url, ok := os.LookupEnv("ETHEREUM_URL")
	if !ok {
		return fmt.Errorf("ETHEREUM_URL env must be defined")
	}
	fmt.Println("ETHEREUM_URL:", url)
	if _, err := NewEthereumClient(url); err != nil {
		return errors.Wrapf(err, "Failed to connect to Ethereum node at %s", url)
	}

	// configure AWS redshift connection
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
	secret, err := redshift.GetAWSSecret(secretName, profile, region)
	if err != nil {
		return errors.Wrapf(err, "Failed to get redshift secret for profile %s", profile)
	}
	dbName, ok := os.LookupEnv("AWS_REDSHIFT")
	if !ok {
		dbName = "dev"
	}
	if _, err := redshift.Connect(secret, dbName, 10); err != nil {
		return errors.Wrapf(err, "Failed to connect to redshift db %s", dbName)
	}
	s3Bucket, ok := os.LookupEnv("AWS_S3BUCKET")
	if !ok {
		s3Bucket = "dev-eth-track"
	}
	copyRole, ok := os.LookupEnv("AWS_COPY_ROLE")
	if !ok {
		copyRole = "arn:aws:iam::436486865631:role/redshift-eth-track-copy"
	}

	if _, err := redshift.GetS3Bucket(s3Bucket, profile, region, copyRole); err != nil {
		return errors.Wrapf(err, "Failed to config AWS s3 bucket %s", s3Bucket)
	}
	return nil
}

func TestMain(m *testing.M) {
	if err := setup(); err != nil {
		fmt.Printf("FAILED %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Setup successful")
	status := m.Run()
	redshift.Close()
	os.Exit(status)
}
