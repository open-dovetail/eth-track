package proc

// Run all unit test: `go test -v`

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	web3 "github.com/umbracle/go-web3"
)

func TestContract(t *testing.T) {
	addrs := []string{
		"0x6b175474e89094c44da98b954eedeac495271d0f",
		"0xdac17f958d2ee523a2206206994597c13d831ec7",
		"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"}
	expected := [][]int{{22, 3}, {32, 11}, {5, 2}}

	for i, addr := range addrs {
		c, err := NewContract(addr, -1)
		assert.NoError(t, err, "Error retrieving contract: %s", addr)
		assert.Equal(t, expected[i][0], len(c.Methods), "contract %s should contain %d methods", addr, expected[i][0])
		assert.Equal(t, expected[i][1], len(c.Events), "contract %s should contain %d events", addr, expected[i][1])
	}
}

// This test gets source code of a contract from etherscan, although it returns only compiled code
//   it maybe useful if adding contract decompiling and abi generation
func TestGetCode(t *testing.T) {
	addr := "0x4fabb145d64652a948d72533023f6e7a623c7c53"
	// pass latest block
	code, err := GetEthereumClient().Eth().GetCode(web3.HexToAddress(addr), web3.EncodeBlock())
	assert.NoError(t, err, "Error retrieving contract source code: %s", addr)
	assert.True(t, strings.HasPrefix(code, "0x"), "contract source code is hex encode of bytes")
}
