package redshift

import (
	"context"
	"encoding/base64"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/golang/glog"
)

// AWS managed secret for redshift db user & password
type PasswordSecret struct {
	Username    string `json:"username"`
	Password    string `json:"password"`
	Engine      string `json:"engine"`
	Host        string `json:"host"`
	Port        int    `json:"port"`
	DBClusterID string `json:"dbClusterIdentifier"`
}

func GetAWSSecret(secretName, profile, region string) (*PasswordSecret, error) {
	// Create a Secrets Manager client with AWS profile, region, and default config/credential specified in ~/.aws
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
		config.WithSharedConfigProfile(profile))
	if err != nil {
		// Handle session creation error
		glog.Errorf("Failed to get config for AWS region %s and profile %s: %s", region, profile, err.Error())
		return nil, err
	}
	svc := secretsmanager.NewFromConfig(cfg)

	// In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
	// See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
	result, err := svc.GetSecretValue(context.TODO(),
		&secretsmanager.GetSecretValueInput{
			SecretId: aws.String(secretName),
		})
	if err != nil {
		glog.Error("Failed GetSecretValue:", err.Error())
		return nil, err
	}

	// Decrypts secret using the associated KMS key.
	// Depending on whether the secret is a string or binary, one of these fields will be populated.
	var secretString string
	if result.SecretString != nil {
		secretString = *result.SecretString
	} else {
		decodedBinarySecretBytes := make([]byte, base64.StdEncoding.DecodedLen(len(result.SecretBinary)))
		len, err := base64.StdEncoding.Decode(decodedBinarySecretBytes, result.SecretBinary)
		if err != nil {
			glog.Error("Base64 Decode Error:", err)
			return nil, err
		}
		secretString = string(decodedBinarySecretBytes[:len])
	}

	//fmt.Println("secret string:", secretString)
	var secret PasswordSecret
	if err := json.Unmarshal([]byte(secretString), &secret); err != nil {
		glog.Error("Secret string is not valid JSON:", err)
		return nil, err
	}
	return &secret, nil
}
