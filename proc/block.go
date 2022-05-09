package proc

import (
	"time"

	"github.com/golang/glog"
	"github.com/open-dovetail/eth-track/common"
	"github.com/open-dovetail/eth-track/redshift"
	"github.com/pkg/errors"
	web3 "github.com/umbracle/ethgo"
)

var blockDelay int

func SetBlockDelay(delay int) {
	blockDelay = delay
}

// return block at the delayed height from the current block
func LastConfirmedBlock() (*web3.Block, error) {
	for retry := 1; retry <= 3; retry++ {
		client := GetEthereumClient()
		lastBlock, err := client.Eth().BlockNumber()
		if err != nil {
			// Ethereum call failed, wait and retry
			glog.Warningf("Failed %d times to get last block number: %+v", retry, err)
			time.Sleep(10 * time.Second)
			continue
		}
		bn := lastBlock - uint64(12) // default to delay confirmed block by 12 blocks
		if blockDelay > 0 {
			bn = lastBlock - uint64(blockDelay)
		}
		if block, err := client.Eth().GetBlockByNumber(web3.BlockNumber(bn), true); err == nil {
			return block, nil
		} else {
			// Ethereum call failed, wait and retry
			glog.Warningf("Failed %d times to get last confirmed block %d: %+v", retry, lastBlock-uint64(blockDelay), err)
			time.Sleep(10 * time.Second)
			continue
		}
	}
	return nil, errors.Errorf("Failed to get last confirmed block")
}

// decode range of blocks and return the first and last blocks.
// if hiBlock is 0, decode from the last confirmed block;
// if lowBlock is 0, decode only a single block specified by the hiBlock.
func DecodeBlockRange(hiBlock, lowBlock uint64) (lastBlock *common.Block, firstBlock *common.Block, err error) {
	if hiBlock < lowBlock {
		// ignore wrong block range
		return nil, nil, nil
	}
	startTime := time.Now().Unix() // to print out elapsed time of the decode process
	nextBlock := hiBlock
	if hiBlock == 0 {
		// decode last confirmed block if hiBlock is not specified
		var block *web3.Block
		block, err = LastConfirmedBlock()
		if err != nil {
			return nil, nil, err
		}
		if lastBlock, err = DecodeBlock(block); err != nil {
			return nil, nil, err
		}
		firstBlock = lastBlock
		nextBlock = lastBlock.Number - 1
	}

	if lowBlock == 0 {
		// return single block if lowBlock is not specified
		if lastBlock != nil {
			return lastBlock, lastBlock, nil
		}
		lowBlock = nextBlock
	}

	for nextBlock >= lowBlock {
		var block *common.Block
		if block, err = DecodeBlockByNumber(nextBlock); err != nil {
			return lastBlock, firstBlock, err
		}
		firstBlock = block
		if nextBlock == hiBlock {
			lastBlock = block
		}
		nextBlock--
	}

	glog.Infof("Decoded block range [%d, %d] - elapsed: %ds", firstBlock.Number, lastBlock.Number, (time.Now().Unix() - startTime))
	return lastBlock, firstBlock, nil
}

func DecodeBlockByNumber(blockNumber uint64) (*common.Block, error) {
	for retry := 1; retry <= 3; retry++ {
		if block, err := GetEthereumClient().Eth().GetBlockByNumber(web3.BlockNumber(blockNumber), true); err == nil {
			return DecodeBlock(block)
		} else {
			// Ethereum call failed, wait and retry
			glog.Warningf("Failed %d times to get block by number %d: %+v", retry, blockNumber, err)
			time.Sleep(10 * time.Second)
		}
	}
	return nil, errors.Errorf("Failed to get block by number %d", blockNumber)
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
		Hash:         block.Hash.String(),
		Number:       block.Number,
		ParentHash:   block.ParentHash,
		Miner:        block.Miner.String(),
		Difficulty:   block.Difficulty,
		GasLimit:     block.GasLimit,
		GasUsed:      block.GasUsed,
		BlockTime:    int64(block.Timestamp),
		Status:       true,
		Transactions: make(map[string]*common.Transaction),
		Logs:         make(map[uint64]*common.EventLog),
	}

	for _, tx := range block.Transactions {
		txn, err := DecodeTransaction(tx, result.BlockTime)
		if err != nil {
			glog.Errorf("Failed to decode transaction: %s", err.Error())
			return nil, err
		}
		// check receipt status
		status, err := GetTransactionStatus(txn.Hash)
		if err != nil {
			glog.Errorf("Failed to get transaction status: %s", err.Error())
			return nil, err
		}
		if status {
			result.Transactions[txn.Hash] = txn
		} else {
			if glog.V(1) {
				glog.Infof("rejected transaction %s", txn.Hash)
			}
		}
	}
	if err := DecodeEvents(result); err != nil {
		return result, err
	}

	// save block and associated transactions and logs in database
	err := redshift.InsertBlock(result)
	return result, err
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
		evt, err := DecodeEventLog(w, b.BlockTime)
		if err != nil {
			// fatal system error
			glog.Errorf("Failed to decode event log: %s", err.Error())
			return err
		}
		if evt.Removed {
			if glog.V(1) {
				glog.Infof("removed event %d-%d", evt.BlockNumber, evt.LogIndex)
			}
		} else {
			b.Logs[evt.LogIndex] = evt
		}
	}
	glog.Infof("Block %d: %s @ %d events=%d", b.Number, b.Hash, b.BlockTime, len(wlogs))
	return nil
}
