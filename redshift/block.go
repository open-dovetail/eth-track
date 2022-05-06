package redshift

import (
	"context"
	"fmt"

	"github.com/golang/glog"
	"github.com/open-dovetail/eth-track/common"
)

// insert block and associated transactions and logs in a database tx
func InsertBlock(block *common.Block) error {
	if block == nil {
		return nil
	}
	tx, err := db.Begin()
	if err != nil {
		glog.Errorf("Failed to start db tx: %+v", err)
		return err
	}
	ctx := context.Background()
	sql := "INSERT INTO eth.blocks (Hash, Number, ParentHash, Miner, Difficulty, GasLimit, GasUsed, BlockTime) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
	if _, err := tx.Exec(ctx, sql,
		common.HexToFixedString(block.Hash, 64),
		block.Number,
		common.HexToFixedString(block.ParentHash.String(), 64),
		common.HexToFixedString(block.Miner, 40),
		common.BigIntToFloat(block.Difficulty),
		block.GasLimit,
		block.GasUsed,
		common.SecondsToDateTime(block.BlockTime),
	); err != nil {
		glog.Errorf("Failed to insert block %d: %+v", block.Number, err)
		tx.Rollback(ctx)
		return err
	}
	if err := InsertTransactions(block.Transactions, tx, ctx); err != nil {
		glog.Errorf("Failed to insert %d transactions of block %d: %+v", len(block.Transactions), block.Number, err)
		tx.Rollback(ctx)
		return err
	}
	if err := InsertEventLogs(block.Logs, tx, ctx); err != nil {
		glog.Errorf("Failed to insert %d events of block %d: %+v\n", len(block.Logs), block.Number, err)
		tx.Rollback(ctx)
		return err
	}
	if glog.V(2) {
		glog.Infof("inserted block %d with %d transactions and %d logs", block.Number, len(block.Transactions), len(block.Logs))
	}
	//fmt.Printf("inserted block %d with %d transactions and %d logs\n", block.Number, len(block.Transactions), len(block.Logs))
	return tx.Commit(ctx)
}

// return saved block numbers that are out of range of [lowBlock, hiBlock]
func SelectBlocks(hiBlock, lowBlock int64) ([]*int64, error) {
	var result []*int64
	sql := "select Number from eth.blocks"
	if hiBlock > 0 {
		sql += fmt.Sprintf(" where Number > %d", hiBlock)
	}
	if lowBlock > 0 {
		if hiBlock > 0 {
			sql += fmt.Sprintf(" or Number < %d", lowBlock)
		} else {
			sql += fmt.Sprintf(" where Number < %d", lowBlock)
		}
	}
	if err := db.Select(&result, sql); err != nil {
		return nil, err
	}
	return result, nil
}
