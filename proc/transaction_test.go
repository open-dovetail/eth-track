package proc

// Run all unit test: `go test -v`

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactionStatus(t *testing.T) {
	txs := []string{
		"0xc167aafc2dbed2d72940a087be6d8185f5882a79d2d38d6c1610446f9affb3ec",
		"0x190c6db99ca0cc2090592c2eda721565c952303cdc0aa35990cb8f6666a9bc89",
		"0x8f7ad82b34218081b4af055934b7220300baf9a168f911579e0120f34b09a284",
		"0xc73d688e5f50d64fdf38cde3eab1a943fae0027e7b262d472011ac363896c6fb"}
	for i, tx := range txs {
		status, err := GetTransactionStatus(tx)
		require.NoError(t, err, "failed to get transaction status %s", tx)
		expected := i > 1
		assert.Equal(t, expected, status, "Not expected status '%t' for transaction %s", status, tx)
	}
}
