package proc

// Run all unit test: `go test -v`

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfirmedBlock(t *testing.T) {
	lastBlock, err := LastConfirmedBlock(12)
	assert.NoError(t, err, "Failed retrieve last block number with 12 block delay")
	assert.True(t, lastBlock.Number > 13648265, "block number should be greater than 13648265")
}

func TestDecodeBlock(t *testing.T) {
	blockNumber := uint64(13648265)
	block, err := DecodeBlockByNumber(blockNumber)
	assert.NoError(t, err, "Failed to decode block %d", blockNumber)
	assert.Equal(t, "0x5593e9f8d436700e7826552c87be8de76b947d9619d6c8a17f2d6a5c7e7787e9", block.Hash, "Block hash does not match expected")
}
