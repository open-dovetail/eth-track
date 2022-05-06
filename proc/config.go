package proc

import (
	"github.com/umbracle/ethgo/jsonrpc"
)

// singleton
var eth *jsonrpc.Client

func NewEthereumClient(nodeURL string) (*jsonrpc.Client, error) {
	var err error
	eth, err = jsonrpc.NewClient(nodeURL)
	return eth, err
}

func GetEthereumClient() *jsonrpc.Client {
	return eth
}
