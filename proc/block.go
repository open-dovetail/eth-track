package proc

import (
	"time"

	"github.com/golang/glog"
	"github.com/open-dovetail/eth-track/common"
	"github.com/open-dovetail/eth-track/store"
	"github.com/pkg/errors"
	web3 "github.com/umbracle/go-web3"
)

// return block at the delayed height from the current block
func LastConfirmedBlock(blockDelay int) (*web3.Block, error) {
	client := GetEthereumClient()
	lastBlock, err := client.Eth().BlockNumber()
	if err != nil {
		return nil, err
	}
	block, err := client.Eth().GetBlockByNumber(web3.BlockNumber(lastBlock), true)
	if err != nil {
		return nil, err
	}
	for i := 0; i < blockDelay; i++ {
		if block, err = client.Eth().GetBlockByHash(block.ParentHash, true); err != nil {
			return nil, err
		}
	}
	return block, nil
}

func GetBlockByNumber(blockNumber uint64) (*common.Block, error) {
	block, err := GetEthereumClient().Eth().GetBlockByNumber(web3.BlockNumber(blockNumber), true)
	if err != nil {
		return nil, err
	}
	return &common.Block{
		Hash:       block.Hash.String(),
		Number:     block.Number,
		ParentHash: block.ParentHash,
		BlockTime:  int64(block.Timestamp),
	}, nil
}

func DecodeBlockByNumber(blockNumber uint64) (*common.Block, error) {
	block, err := GetEthereumClient().Eth().GetBlockByNumber(web3.BlockNumber(blockNumber), true)
	if err != nil {
		return nil, err
	}
	return DecodeBlock(block)
}

func DecodeBlockByHash(blockHash web3.Hash) (*common.Block, error) {
	for retry := 1; retry <= 3; retry++ {
		if block, err := GetEthereumClient().Eth().GetBlockByHash(blockHash, true); err == nil {
			return DecodeBlock(block)
		} else {
			// Ethereum call failed, wait and retry
			glog.Warningf("Failed %d times to get block by hash %s: %+v", retry, blockHash.String(), err)
			time.Sleep(10 * time.Second)
		}
	}
	return nil, errors.Errorf("Failed to get block by hash %s", blockHash.String())
}

func DecodeBlock(block *web3.Block) (*common.Block, error) {
	glog.Infof("Block %d: %s @ %d transactions=%d", block.Number, block.Hash.String(), block.Timestamp, len(block.Transactions))
	result := &common.Block{
		Hash:       block.Hash.String(),
		Number:     block.Number,
		ParentHash: block.ParentHash,
		Miner:      block.Miner.String(),
		Difficulty: block.Difficulty,
		GasLimit:   block.GasLimit,
		GasUsed:    block.GasUsed,
		BlockTime:  int64(block.Timestamp),
		Status:     true,
	}
	if err := insertData(result); err != nil {
		glog.Warningf("Failed to insert block %d: %+v", block.Number, err)
	}

	for _, tx := range block.Transactions {
		trans := DecodeTransaction(tx, result.BlockTime)
		if err := insertData(trans); err != nil {
			glog.Warningf("Failed to insert transaction %s: %+v", trans.Hash, err)
		}
	}
	err := DecodeEvents(result)
	return result, err
}

func insertData(data interface{}) error {
	if store.GetDBConnection() == nil {
		return errors.New("Database connection is not initialized")
	}
	switch v := data.(type) {
	case *common.Block:
		return store.MustGetDBTx().InsertBlock(v)
	case *common.Transaction:
		return store.MustGetDBTx().InsertTransaction(v)
	case *common.EventLog:
		return store.MustGetDBTx().InsertLog(v)
	case *common.Contract:
		return store.MustGetDBTx().InsertContract(v)
	}
	return nil
}

func DecodeEvents(b *common.Block) error {
	// Note: client.Eth().GetLogs(&logFilter) does not work with `BlockHash` filter, so use base RPC call here
	var wlogs []*web3.Log
	for retry := 1; retry <= 5; retry++ {
		if err := GetEthereumClient().Call("eth_getLogs", &wlogs, map[string]string{"BlockHash": b.Hash}); err != nil {
			// retry 3 times on error
			glog.Warningf("Failed %d times to get logs for block %d: %+v", retry, b.Number, err)
			time.Sleep(time.Duration(10*retry) * time.Second)
		}
	}
	if wlogs == nil {
		return errors.Errorf("Failed to retrieve logs of block %d", b.Number)
	}

	// It is equivalent to use filter From=To=b.Number as follows
	// filter := &web3.LogFilter{}
	// filter.SetFromUint64(b.Number)
	// filter.SetToUint64(b.Number)
	// wlogs, err := GetEthereumClient().Eth().GetLogs(filter)

	for _, w := range wlogs {
		evt := DecodeEventLog(w, b.BlockTime)
		if err := insertData(evt); err != nil {
			glog.Warningf("Failed to insert eventlog %s %d: %s", evt.TxnHash, evt.LogIndex, err.Error())
		}
	}
	glog.Infof("Block %d: %s @ %d events=%d", b.Number, b.Hash, b.BlockTime, len(wlogs))
	return nil
}
