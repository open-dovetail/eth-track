package redshift

// Run all unit test: `go test -v`

import (
	"testing"

	"github.com/open-dovetail/eth-track/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateProgress(t *testing.T) {
	progress := &common.Progress{
		ProcessID: common.AddTransaction,
		HiBlock:   14706420,
		LowBlock:  14706410,
	}

	err := UpdateProgress(progress)
	assert.NoError(t, err, "Update progress should not throw exception")

	// query the progress
	p, err := QueryProgress(common.AddTransaction)
	require.NoError(t, err, "query progress should not throw exception")
	assert.NotNil(t, p, "query result should not be empty")

	assert.Equal(t, uint64(14706420), p.HiBlock, "query result does not match HiBlock")
	assert.Equal(t, uint64(14706410), p.LowBlock, "query result does not match LowBlock")
}
