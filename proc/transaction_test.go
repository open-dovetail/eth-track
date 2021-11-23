package proc

// Run all unit test: `go test -v`

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeBlock(t *testing.T) {
	blockNumber := uint64(13648265)
	block, err := DecodeBlockByNumber(blockNumber)
	require.NoError(t, err, "Failed to decode block %d", blockNumber)
	assert.Equal(t, "0x5593e9f8d436700e7826552c87be8de76b947d9619d6c8a17f2d6a5c7e7787e9", block.Hash, "Block hash does not match expected")
	assert.Equal(t, 52, len(block.Transactions), "transaction count does not match")
	fmt.Printf("block transactions: %d events: %d\n", len(block.Transactions), len(block.EventLogs))
}

func TestTransactionStatus(t *testing.T) {
	url, ok := os.LookupEnv("ETHEREUM_URL")
	require.True(t, ok, "ETHEREUM_URL env must be defined")
	apiKey, ok := os.LookupEnv("ETHERSCAN_APIKEY")
	require.True(t, ok, "ETHERSCAN_APIKEY env must be defined")

	_, err := NewConfig(url, apiKey, 200)
	require.NoError(t, err, "failed to connect Ethereum node %s", url)

	txs := []string{
		"0xc167aafc2dbed2d72940a087be6d8185f5882a79d2d38d6c1610446f9affb3ec",
		"0x190c6db99ca0cc2090592c2eda721565c952303cdc0aa35990cb8f6666a9bc89",
		"0x8f7ad82b34218081b4af055934b7220300baf9a168f911579e0120f34b09a284",
		"0xc73d688e5f50d64fdf38cde3eab1a943fae0027e7b262d472011ac363896c6fb"}
	for i, tx := range txs {
		status, err := GetTransactionStatus(tx)
		require.NoError(t, err, "failed to get transaction status %s", tx)
		fmt.Println(tx, status)
		expected := i > 1
		assert.Equal(t, expected, status, "Not expected status '%t' for transaction %s", status, tx)
	}
}
