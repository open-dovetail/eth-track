package proc

// Run all unit test: `go test -v`

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/umbracle/ethgo/abi"
)

func TestFetchABI(t *testing.T) {
	addrs := []string{
		"0x6b175474e89094c44da98b954eedeac495271d0f",
		"0xdac17f958d2ee523a2206206994597c13d831ec7",
		"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
		"0xd569d3cce55b71a8a3f3c418c329a66e5f714431",
	}
	expected := [][]int{{22, 3}, {32, 11}, {5, 2}, {29, 18}}

	for i, addr := range addrs {
		abiData, err := FetchABI(addr, 0)
		require.NoError(t, err, "Error fetching ABI from Etherscan: %s", addr)
		//fmt.Println("ABI:", abiData)
		ab, err := abi.NewABI(abiData)
		assert.NoError(t, err, "Invalid ABI data fetched from Etherscan: %s", addr)
		assert.Equal(t, expected[i][0], len(ab.Methods), "ABI method count does not match for contract: %s", addr)
		assert.Equal(t, expected[i][1], len(ab.Events), "ABI event count does not match for contract: %s", addr)
	}
}
