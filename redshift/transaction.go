package redshift

import (
	"context"
	"fmt"

	"github.com/golang/glog"
	"github.com/jackc/pgx/v4"
	"github.com/open-dovetail/eth-track/common"
)

type copyFromTransactions struct {
	rows []*common.Transaction
	idx  int
}

// column names for batch insert or copy
func transactionColumns() []string {
	return []string{"Hash", "BlockNumber", "TxnIndex", "FromAddress", "ToAddress", "GasPrice", "Gas",
		"Value", "Nonce", "BlockTime", "Input", "Method", "ArgsLen",
		"Arg_1", "S_Value_1", "F_Value_1", "Arg_2", "S_Value_2", "F_Value_2", "Arg_3", "S_Value_3", "F_Value_3",
		"Arg_4", "S_Value_4", "F_Value_4", "Arg_5", "S_Value_5", "F_Value_5"}
}

// implement pgx.CopyFromSource interface, return tuple of values in order of transactionColumns()
func (c *copyFromTransactions) Values() ([]interface{}, error) {
	transaction := c.rows[c.idx]
	var v []interface{}
	v = append(v, common.HexToFixedString(transaction.Hash, 64))
	v = append(v, transaction.BlockNumber)
	v = append(v, transaction.TxnIndex)
	v = append(v, common.HexToFixedString(transaction.From, 40))
	v = append(v, common.HexToFixedString(transaction.To, 40))
	v = append(v, transaction.GasPrice)
	v = append(v, transaction.Gas)
	v = append(v, common.BigIntToFloat(transaction.Value))
	v = append(v, transaction.Nonce)
	v = append(v, common.SecondsToDateTime(transaction.BlockTime))
	if len(transaction.Params) > 0 && len(transaction.Params) <= 5 {
		v = append(v, []byte{})
	} else {
		v = append(v, filterBytesByLength(transaction.Input, 16384))
	}
	v = append(v, truncateString(transaction.Method, 256))
	v = append(v, len(transaction.Params))
	for i := 0; i < 5; i++ {
		if i < len(transaction.Params) {
			v = append(v, truncateString(transaction.Params[i].Name, 256))
			s, f := convertNamedValue(transaction.Params[i])
			v = append(v, truncateString(s, 4096))
			v = append(v, f)
		} else {
			v = append(v, nil)
			v = append(v, nil)
			v = append(v, 0)
		}
	}
	//fmt.Println("Copy transaction", v[0])
	return v, nil
}

func (c *copyFromTransactions) Next() bool {
	c.idx++
	return c.idx < len(c.rows)
}

func (c *copyFromTransactions) Err() error {
	return nil
}

// batch insert contract values in a DB transaction.
func InsertTransactions(transactions map[string]*common.Transaction, tx pgx.Tx, ctx context.Context) error {
	if len(transactions) == 0 {
		return nil
	}

	source := &copyFromTransactions{idx: -1}
	for _, v := range transactions {
		source.rows = append(source.rows, v)
	}

	// CopyFrom does not work for redshift probably because the postgres copy protocol is not supported by redshift
	//rows, err := db.CopyFrom(pgx.Identifier{"eth", "transactions"}, columns, source)
	sql, err := composeBatchInsert("eth.transactions", transactionColumns(), source)
	if err != nil {
		return err
	}
	//fmt.Println("Insert transactions:", sql)

	if tx == nil {
		return db.Exec(sql)
	}
	if _, err = tx.Exec(ctx, sql); err != nil {
		glog.Error("Failed to insert transactions:", sql)
		return err
	}
	return err
}

// write transactions of specified blocks to s3 as a csv file.
func writeTransactionsToS3(blocks map[string]*common.Block, s3Folder string) (int, error) {
	txCount := 0
	if len(blocks) == 0 {
		return txCount, nil
	}

	source := &copyFromTransactions{idx: -1}

	for _, b := range blocks {
		txCount += len(b.Transactions)
		for _, v := range b.Transactions {
			source.rows = append(source.rows, v)
		}
	}
	data, err := composeCSVData(source)
	if err != nil {
		return txCount, err
	}

	//fmt.Println("Write transactions to s3:", string(data))
	s3Filename := fmt.Sprintf("%s/transactions.csv", s3Folder)
	_, err = writeS3File(s3Filename, data)

	return txCount, err
}
