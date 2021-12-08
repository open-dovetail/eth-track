package proc

// Run all unit test: `go test -v`

import (
	"fmt"
	"os"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/umbracle/go-web3/abi"
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

	NewEtherscanAPI(apiKey, 200)
	if _, err := NewEthereumClient(url); err != nil {
		return errors.Wrapf(err, "Failed to connect to Ethereum node at %s", url)
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
	os.Exit(status)
}

func TestEtherscanAPI(t *testing.T) {

	api := GetEtherscanAPI()
	assert.NotNil(t, api, "Etherscan config should not be nil")

	addrs := []string{
		"0x6b175474e89094c44da98b954eedeac495271d0f",
		"0xdac17f958d2ee523a2206206994597c13d831ec7",
		"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"}
	expected := [][]int{{22, 3}, {32, 11}, {5, 2}}

	for i, addr := range addrs {
		abiData, err := api.FetchABI(addr)
		require.NoError(t, err, "Error fetching ABI from Etherscan: %s", addr)
		ab, err := abi.NewABI(abiData)
		require.NoError(t, err, "Invalid ABI data fetched from Etherscan: %s", addr)
		assert.Equal(t, expected[i][0], len(ab.Methods), "ABI method count does not match for contract: %s", addr)
		assert.Equal(t, expected[i][1], len(ab.Events), "ABI event count does not match for contract: %s", addr)
	}
}
