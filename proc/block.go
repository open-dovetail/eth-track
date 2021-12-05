package proc

import (
	"math/big"

	"github.com/golang/glog"
	web3 "github.com/umbracle/go-web3"
)

type Block struct {
	Hash         string
	Number       uint64
	ParentHash   web3.Hash
	Miner        string
	Difficulty   *big.Int
	GasLimit     uint64
	GasUsed      uint64
	BlockTime    int64
	Transactions []*Transaction
	EventLogs    []*EventLog
}

// return block number at the delayed height from the current block
func LastConfirmedBlock(blockDelay int) (uint64, error) {
	client := GetEthereumClient()
	lastBlock, err := client.Eth().BlockNumber()
	if err != nil {
		return 0, err
	}
	block, err := client.Eth().GetBlockByNumber(web3.BlockNumber(lastBlock), true)
	if err != nil {
		return 0, err
	}
	for i := 0; i < blockDelay; i++ {
		if block, err = client.Eth().GetBlockByHash(block.ParentHash, true); err != nil {
			return 0, err
		}
	}
	return block.Number, nil
}

func DecodeBlockByNumber(blockNumber uint64) (*Block, error) {
	block, err := GetEthereumClient().Eth().GetBlockByNumber(web3.BlockNumber(blockNumber), true)
	if err != nil {
		return nil, err
	}
	return DecodeBlock(block)
}

func DecodeBlockByHash(blockHash web3.Hash) (*Block, error) {
	block, err := GetEthereumClient().Eth().GetBlockByHash(blockHash, true)
	if err != nil {
		return nil, err
	}
	return DecodeBlock(block)
}

func DecodeBlock(block *web3.Block) (*Block, error) {
	glog.Infof("Block %d: %s @ %d", block.Number, block.Hash.String(), block.Timestamp)
	txLen := len(block.Transactions)
	result := &Block{
		Hash:         block.Hash.String(),
		Number:       block.Number,
		ParentHash:   block.Hash,
		Miner:        block.Miner.String(),
		Difficulty:   block.Difficulty,
		GasLimit:     block.GasLimit,
		GasUsed:      block.GasUsed,
		BlockTime:    int64(block.Timestamp),
		Transactions: make([]*Transaction, txLen, txLen),
	}
	for i, tx := range block.Transactions {
		txn := DecodeTransaction(tx)
		txn.BlockTime = result.BlockTime
		result.Transactions[i] = txn
	}
	err := result.DecodeEvents()
	return result, err
}

func (b *Block) DecodeEvents() error {
	// Note: client.Eth().GetLogs(&logFilter) does not work with `BlockHash` filter, so use base RPC call here
	var wlogs []*web3.Log
	if err := GetEthereumClient().Call("eth_getLogs", &wlogs, map[string]string{"BlockHash": b.Hash}); err != nil {
		return err
	}
	b.EventLogs = make([]*EventLog, len(wlogs), len(wlogs))
	for i, w := range wlogs {
		evt := DecodeEventLog(w)
		evt.BlockTime = b.BlockTime
		b.EventLogs[i] = evt
	}
	return nil
}
