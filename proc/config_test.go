package proc

// Run all unit test: `go test -v`

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// initialize Ethereum node connection
func setup() error {
	url, ok := os.LookupEnv("ETHEREUM_URL")
	if !ok {
		return fmt.Errorf("ETHEREUM_URL env must be defined")
	}
	fmt.Println("ETHEREUM_URL:", url)

	apiKey, ok := os.LookupEnv("ETHERSCAN_APIKEY")
	if !ok {
		return fmt.Errorf("ETHERSCAN_APIKEY env must be defined")
	}
	fmt.Println("ETHERSCAN_APIKEY:", apiKey)

	_, err := NewConfig(url, apiKey, 200)
	return err
}

func TestMain(m *testing.M) {
	if err := setup(); err != nil {
		fmt.Printf("FAILED %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Setup successful")
	status := m.Run()
	os.Exit(status)
}

func TestConfig(t *testing.T) {

	client := GetConfig().GetClient()
	assert.NotNil(t, client, "Ethereum client should not be nil")

	addrs := []string{
		"0x6b175474e89094c44da98b954eedeac495271d0f",
		"0xdac17f958d2ee523a2206206994597c13d831ec7",
		"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"}

	for _, addr := range addrs {
		ab, err := GetConfig().FetchABI(addr)
		require.NoError(t, err, "Error fetching ABI from Etherscan: %s", addr)
		assert.NotEmpty(t, ab.Events, "ABI events should not be empty: %s", addr)
		assert.NotEmpty(t, ab.Methods, "ABI methods should not be empty: %s", addr)
	}
}
