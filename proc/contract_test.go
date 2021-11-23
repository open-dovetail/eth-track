package proc

// Run all unit test: `go test -v`

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContract(t *testing.T) {
	addrs := []string{
		"0x6b175474e89094c44da98b954eedeac495271d0f",
		"0xdac17f958d2ee523a2206206994597c13d831ec7",
		"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"}

	for _, addr := range addrs {
		c, err := NewContract(addr)
		require.NoError(t, err, "Error retrieving contract: %s", addr)
		assert.NotEmpty(t, c.Events, "ABI events should not be empty: %s", addr)
		assert.NotEmpty(t, c.Methods, "ABI methods should not be empty: %s", addr)

		// data, _ := json.Marshal(c)
		// fmt.Println("ABI", string(data))
		// for k, v := range c.Methods {
		// 	arg, _ := json.Marshal(v.Inputs.TupleElems())
		// 	fmt.Println("Method input", k, v.Name, string(arg))
		// }
		// for k, v := range c.Events {
		// 	arg, _ := json.Marshal(v.Inputs.TupleElems())
		// 	fmt.Println("Event input", k, v.Name, string(arg))
		// }
	}
}
