package redshift

import (
	"context"
	"fmt"

	"github.com/golang/glog"
	"github.com/jackc/pgx/v4"
	"github.com/open-dovetail/eth-track/common"
)

type copyFromEventLogs struct {
	rows []*common.EventLog
	idx  int
}

// column names for batch insert or copy
func eventLogColumns() []string {
	return []string{"BlockNumber", "LogIndex", "TxnIndex", "TxnHash", "Address", "BlockTime", "Data", "Event", "ArgsLen",
		"Arg_1", "S_Value_1", "F_Value_1", "Arg_2", "S_Value_2", "F_Value_2", "Arg_3", "S_Value_3", "F_Value_3",
		"Arg_4", "S_Value_4", "F_Value_4", "Arg_5", "S_Value_5", "F_Value_5"}
}

// implement pgx.CopyFromSource interface, return tuple of values in order of eventLogColumns()
func (c *copyFromEventLogs) Values() ([]interface{}, error) {
	eventlog := c.rows[c.idx]
	var v []interface{}
	v = append(v, eventlog.BlockNumber)
	v = append(v, eventlog.LogIndex)
	v = append(v, eventlog.TxnIndex)
	v = append(v, common.HexToFixedString(eventlog.TxnHash, 64))
	v = append(v, common.HexToFixedString(eventlog.Address, 40))
	v = append(v, common.SecondsToDateTime(eventlog.BlockTime))
	if len(eventlog.Params) > 0 && len(eventlog.Params) <= 5 {
		v = append(v, []byte{})
	} else {
		v = append(v, filterBytesByLength(eventlog.Data, 16384))
	}
	v = append(v, truncateString(eventlog.Event, 256))
	v = append(v, len(eventlog.Params))
	for i := 0; i < 5; i++ {
		if i < len(eventlog.Params) {
			v = append(v, truncateString(eventlog.Params[i].Name, 256))
			s, f := convertNamedValue(eventlog.Params[i])
			v = append(v, truncateString(s, 4096))
			v = append(v, f)
		} else {
			v = append(v, nil)
			v = append(v, nil)
			v = append(v, 0)
		}
	}
	//fmt.Println("Copy eventlog", v[0], v[1])
	return v, nil
}

func (c *copyFromEventLogs) Next() bool {
	c.idx++
	return c.idx < len(c.rows)
}

func (c *copyFromEventLogs) Err() error {
	return nil
}

// batch insert contract values in a DB transaction.
func InsertEventLogs(logs map[uint64]*common.EventLog, tx pgx.Tx, ctx context.Context) error {
	if len(logs) == 0 {
		return nil
	}

	source := &copyFromEventLogs{idx: -1}
	for _, v := range logs {
		source.rows = append(source.rows, v)
	}
	// CopyFrom does not work for redshift probably because the postgres copy protocol is not supported by redshift
	//rows, err := db.CopyFrom(pgx.Identifier{"eth", "logs"}, columns, source)
	sql, err := composeBatchInsert("eth.logs", eventLogColumns(), source)
	//fmt.Println("Insert eventlogs:", sql)
	if err != nil {
		return err
	}
	if tx == nil {
		return db.Exec(sql)
	}
	if _, err = tx.Exec(ctx, sql); err != nil {
		glog.Error("Failed to insert event logs:", sql)
		return err
	}
	return err
}

// write event logs of specified blocks to s3 as a csv file.
func writeEventLogsToS3(blocks map[string]*common.Block, s3Folder string) error {
	if len(blocks) == 0 {
		return nil
	}

	source := &copyFromEventLogs{idx: -1}
	for _, b := range blocks {
		for _, v := range b.Logs {
			source.rows = append(source.rows, v)
		}
	}
	data, err := composeCSVData(source)
	if err != nil {
		return err
	}

	//fmt.Println("Write event logs to s3:", string(data))
	s3Filename := fmt.Sprintf("%s/logs.csv", s3Folder)
	_, err = writeS3File(s3Filename, data)

	return err
}
