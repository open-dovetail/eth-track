package proc

// Run all unit test: `go test -v`

import (
	"fmt"
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
