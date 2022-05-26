package redshift

import (
	"context"
	"fmt"
	"strings"

	"github.com/golang/glog"
	"github.com/open-dovetail/eth-track/common"
)

type copyFromBlocks struct {
	rows []*common.Block
	idx  int
}

// column names for batch insert or copy
func blockColumns() []string {
	return []string{"Hash", "Number", "ParentHash", "Miner", "Difficulty", "GasLimit", "GasUsed", "BlockTime"}
}

// implement pgx.CopyFromSource interface, return tuple of values in order of blockColumns()
func (c *copyFromBlocks) Values() ([]interface{}, error) {
	block := c.rows[c.idx]
	var v []interface{}
	v = append(v, common.HexToFixedString(block.Hash, 64))
	v = append(v, block.Number)
	v = append(v, common.HexToFixedString(block.ParentHash.String(), 64))
	v = append(v, common.HexToFixedString(block.Miner, 40))
	v = append(v, common.BigIntToFloat(block.Difficulty))
	v = append(v, block.GasLimit)
	v = append(v, block.GasUsed)
	v = append(v, common.SecondsToDateTime(block.BlockTime))

	//fmt.Println("Copy block", v[0], v[1])
	return v, nil
}

func (c *copyFromBlocks) Next() bool {
	c.idx++
	return c.idx < len(c.rows)
}

func (c *copyFromBlocks) Err() error {
	return nil
}

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

// write blocks to s3 as a csv file.
func writeBlocksToS3(blocks map[string]*common.Block, s3Folder string) error {
	if len(blocks) == 0 {
		return nil
	}

	var err error
	var txCount, logCount int
	if txCount, err = writeTransactionsToS3(blocks, s3Folder); err != nil {
		return err
	}
	if logCount, err = writeEventLogsToS3(blocks, s3Folder); err != nil {
		return err
	}

	// write blocks to s3
	source := &copyFromBlocks{idx: -1}
	for _, v := range blocks {
		source.rows = append(source.rows, v)
	}
	var data []byte
	if data, err = composeCSVData(source); err != nil {
		return err
	}

	//fmt.Println("Write blocks to s3:", string(data))
	glog.Infof("Write data to s3: %d blocks, %d transactions, %d event logs", len(blocks), txCount, logCount)
	s3Filename := fmt.Sprintf("%s/blocks.csv", s3Folder)
	_, err = writeS3File(s3Filename, data)

	return err
}

// write data of blocks/transactions/events to s3 as csv, then copy the result to redshift in a transaction
func StoreBlocks(blocks map[string]*common.Block, s3Folder string) error {
	if err := writeBlocksToS3(blocks, s3Folder); err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		// handle exception of redshift offline
		if err := db.Reconnect(); err != nil {
			glog.Errorf("Failed to reconnect to db: %+v", err)
			return err
		}
		if tx, err = db.Begin(); err != nil {
			glog.Errorf("Failed to start db tx: %+v", err)
			return err
		}
	}
	ctx := context.Background()

	// copy transactions
	sql := fmt.Sprintf(`COPY eth.transactions (%s) FROM 's3://%s/%s/transactions.csv' IAM_ROLE '%s' REGION '%s' TIMEFORMAT 'auto' ACCEPTINVCHARS STATUPDATE ON CSV`,
		strings.Join(transactionColumns(), ","), bucket.name, s3Folder, bucket.copyRole, bucket.region)
	glog.Info("Execute sql: ", sql)
	if _, err := tx.Exec(ctx, sql); err != nil {
		glog.Warning("rollback copy transactions")
		tx.Rollback(ctx)
		deleteS3Folder(s3Folder)
		return err
	}

	// copy event logs
	sql = fmt.Sprintf(`COPY eth.logs (%s) FROM 's3://%s/%s/logs.csv' IAM_ROLE '%s' REGION '%s' TIMEFORMAT 'auto' ACCEPTINVCHARS STATUPDATE ON CSV`,
		strings.Join(eventLogColumns(), ","), bucket.name, s3Folder, bucket.copyRole, bucket.region)
	glog.Info("Execute sql: ", sql)
	if _, err := tx.Exec(ctx, sql); err != nil {
		glog.Warning("rollback copy event logs")
		tx.Rollback(ctx)
		deleteS3Folder(s3Folder)
		return err
	}

	// copy blocks
	sql = fmt.Sprintf(`COPY eth.blocks (%s) FROM 's3://%s/%s/blocks.csv' IAM_ROLE '%s' REGION '%s' TIMEFORMAT 'auto' STATUPDATE ON CSV`,
		strings.Join(blockColumns(), ","), bucket.name, s3Folder, bucket.copyRole, bucket.region)
	glog.Info("Execute sql: ", sql)
	if _, err := tx.Exec(ctx, sql); err != nil {
		glog.Warning("rollback copy blocks")
		tx.Rollback(ctx)
		deleteS3Folder(s3Folder)
		return err
	}
	err = tx.Commit(ctx)
	deleteS3Folder(s3Folder)
	return err
}
