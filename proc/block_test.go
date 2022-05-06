package proc

// Run all unit test: `go test -v`

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfirmedBlock(t *testing.T) {
	lastBlock, err := LastConfirmedBlock()
	assert.NoError(t, err, "Failed retrieve last block number with 12 block delay")
	assert.True(t, lastBlock.Number > 13648265, "block number should be greater than 13648265")
	fmt.Println("last confirmed block", lastBlock.Number)
}

func TestDecodeBlock(t *testing.T) {
	blockNumber := uint64(13648277)
	block, err := DecodeBlockByNumber(blockNumber)
	assert.NoError(t, err, "Failed to decode block %d", blockNumber)
	//assert.Equal(t, "0x5593e9f8d436700e7826552c87be8de76b947d9619d6c8a17f2d6a5c7e7787e9", block.Hash, "Block hash does not match expected")
	fmt.Printf("decoded block %d with transactiions %d and event logs %d\n", blockNumber, len(block.Transactions), len(block.Logs))
}

func TestDecodeBlockRange(t *testing.T) {
	block, err := LastConfirmedBlock()
	require.NoError(t, err, "Failed retrieve last block number with 12 block delay")
	lowBlock := block.Number - 3
	lastBlock, firstBlock, err := DecodeBlockRange(block.Number, lowBlock)
	assert.NoError(t, err, "Failed to decode block range [%d, %d]", block.Number, lowBlock)
	assert.Equal(t, block.Number, lastBlock.Number, "last block number should match last confirmed block")
	assert.Equal(t, lowBlock, firstBlock.Number, "first block number should match low bound of block range")
}
