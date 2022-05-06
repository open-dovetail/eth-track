package redshift

import (
	"github.com/open-dovetail/eth-track/common"
)

// acquires a connection, update a progress row, then release the connection
func UpdateProgress(progress *common.Progress) error {
	if progress == nil {
		return nil
	}
	sql := "UPDATE eth.progress SET HiBlock=$1, LowBlock=$2 WHERE ProcessID=$3"
	//fmt.Println("update progress", progress)
	return db.Exec(sql,
		progress.HiBlock,
		progress.LowBlock,
		progress.ProcessID)
}

// acquires a connection, fetch a progress row by id, then release the connection
func QueryProgress(pid common.ProcessType) (*common.Progress, error) {
	sql := `SELECT HiBlock, LowBlock FROM eth.progress WHERE ProcessID = $1`
	rows, err := db.Query(sql, pid)
	if err != nil {
		return nil, err
	}
	progress := &common.Progress{ProcessID: pid}
	ok, err := ScanRow(rows,
		&progress.HiBlock,
		&progress.LowBlock,
	)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return progress, nil
}
