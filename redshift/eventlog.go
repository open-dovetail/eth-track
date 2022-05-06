package redshift

import (
	"context"

	"github.com/golang/glog"
	"github.com/jackc/pgx/v4"
	"github.com/open-dovetail/eth-track/common"
)

type copyFromEventLogs struct {
	rows []*common.EventLog
	idx  int
}

// implement pgx.CopyFromSource interface
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
		v = append(v, nil)
	} else {
		v = append(v, eventlog.Data)
	}
	v = append(v, trunkString(eventlog.Event, 40))
	v = append(v, len(eventlog.Params))
	for i := 0; i < 5; i++ {
		if i < len(eventlog.Params) {
			v = append(v, trunkString(eventlog.Params[i].Name, 40))
			s, f := convertNamedValue(eventlog.Params[i])
			if len(s) > 4096 {
				glog.Warning("Database truncate string value to 4096 bytes")
				s = s[:4096]
			}
			v = append(v, s)
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
	columns := []string{"BlockNumber", "LogIndex", "TxnIndex", "TxnHash", "Address", "BlockTime", "Data", "Event", "ArgsLen",
		"Arg_1", "S_Value_1", "F_Value_1", "Arg_2", "S_Value_2", "F_Value_2", "Arg_3", "S_Value_3", "F_Value_3",
		"Arg_4", "S_Value_4", "F_Value_4", "Arg_5", "S_Value_5", "F_Value_5"}
	// CopyFrom does not work for redshift probably because the postgres copy protocol is not supported by redshift
	//rows, err := db.CopyFrom(pgx.Identifier{"eth", "logs"}, columns, source)
	sql, err := composeBatchInsert("eth.logs", columns, source)
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
