package redshift

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/jackc/pgx/v4"
	"github.com/open-dovetail/eth-track/common"
)

type copyFromContracts struct {
	rows []*common.Contract
	idx  int
}

// column names for batch insert or copy
func contractColumns() []string {
	return []string{"Address", "Name", "Symbol", "Decimals", "TotalSupply", "LastEventDate", "LastErrorDate", "ABI"}
}

// implement pgx.CopyFromSource interface,  return tuple of values in order of contractColumns()
func (c *copyFromContracts) Values() ([]interface{}, error) {
	contract := c.rows[c.idx]
	var v []interface{}
	v = append(v, common.HexToFixedString(contract.Address, 40))
	v = append(v, truncateString(contract.Name, 256))
	v = append(v, truncateString(contract.Symbol, 256))
	v = append(v, contract.Decimals)
	v = append(v, contract.TotalSupply)
	// format date type to include only date part for cvs load file
	v = append(v, common.SecondsToDateTime(contract.LastEventDate).Format("2006-01-02"))
	v = append(v, common.SecondsToDateTime(contract.LastErrorDate).Format("2006-01-02"))
	v = append(v, filterStringByLength(contract.ABI, 1024*31))
	//fmt.Println("Copy contract", v[0])
	return v, nil
}

func (c *copyFromContracts) Next() bool {
	c.idx++
	return c.idx < len(c.rows)
}

func (c *copyFromContracts) Err() error {
	return nil
}

// batch insert contract values in a DB transaction.
func InsertContracts(contracts map[string]*common.Contract) error {
	if len(contracts) == 0 {
		return nil
	}

	source := &copyFromContracts{idx: -1}
	for _, v := range contracts {
		source.rows = append(source.rows, v)
	}
	// CopyFrom does not work for redshift probably because the postgres copy protocol is not supported by redshift
	//rows, err := db.CopyFrom(pgx.Identifier{"eth", "contracts"}, columns, source)
	sql, err := composeBatchInsert("eth.contracts", contractColumns(), source)
	//fmt.Println("Insert contracts:", sql)
	if err != nil {
		return err
	}
	if err := db.Exec(sql); err != nil {
		glog.Errorf("Failed to store contracts %+v: %s", err, sql)
		return err
	}
	return nil
}

// write specified contracts to s3 as a csv file.
func writeContractsToS3(contracts map[string]*common.Contract, csvFile string) error {
	if len(contracts) == 0 {
		return nil
	}

	source := &copyFromContracts{idx: -1}
	for _, c := range contracts {
		source.rows = append(source.rows, c)
	}
	data, err := composeCSVData(source)
	if err != nil {
		return err
	}

	//fmt.Println("Write contracts to s3:", string(data))
	_, err = writeS3File(csvFile, data)
	return err
}

// write data of contracts to s3 as csv, then copy the result to redshift in a transaction
func StoreContracts(contracts map[string]*common.Contract) error {
	csvFile := "contracts.csv"
	if err := writeContractsToS3(contracts, csvFile); err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		glog.Errorf("Failed to start db tx: %+v", err)
		return err
	}

	// copy contracts
	ctx := context.Background()
	sql := fmt.Sprintf(`COPY eth.contracts (%s) FROM 's3://%s/%s' IAM_ROLE '%s' REGION '%s' TIMEFORMAT 'auto' STATUPDATE ON CSV`,
		strings.Join(contractColumns(), ","), bucket.name, csvFile, bucket.copyRole, bucket.region)
	glog.Info("Execute sql: ", sql)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx)
		deleteS3File(csvFile)
		return err
	}

	err = tx.Commit(ctx)
	deleteS3File(csvFile)
	return err
}

// acquires a connection, updates contract EventDate and ErrorDate, then release the connection
func UpdateContract(contract *common.Contract) error {
	if contract == nil {
		return nil
	}
	sql := "UPDATE eth.contracts SET LastEventDate = $1, LastErrorDate = $2 WHERE Address = $3"
	return db.Exec(sql, common.SecondsToDateTime(contract.LastEventDate),
		common.SecondsToDateTime(contract.LastErrorDate), common.HexToFixedString(contract.Address, 40))
}

func InsertContract(contract *common.Contract) error {
	if contract == nil {
		return nil
	}
	abi := filterStringByLength(contract.ABI, 1024*31)

	sql := "INSERT INTO eth.contracts (Address, Name, Symbol, Decimals, TotalSupply, LastEventDate, LastErrorDate, ABI) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
	return db.Exec(sql,
		common.HexToFixedString(contract.Address, 40),
		truncateString(contract.Name, 256),
		truncateString(contract.Symbol, 256),
		contract.Decimals,
		contract.TotalSupply,
		common.SecondsToDateTime(contract.LastEventDate),
		common.SecondsToDateTime(contract.LastErrorDate),
		abi)
}

// acquires a connection, fetch one contract by address, then release the connection
func QueryContract(address string) (*common.Contract, error) {
	sql := `SELECT Name, Symbol, Decimals, TotalSupply, LastEventDate, LastErrorDate, ABI 
		FROM eth.contracts WHERE Address = $1`
	rows, err := db.Query(sql, common.HexToFixedString(address, 40))
	if err != nil {
		return nil, err
	}
	contract := &common.Contract{Address: address}
	var lastEventDate, lastErrorDate time.Time
	ok, err := ScanRow(rows,
		&contract.Name,
		&contract.Symbol,
		&contract.Decimals,
		&contract.TotalSupply,
		&lastEventDate,
		&lastErrorDate,
		&contract.ABI,
	)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	contract.LastEventDate = lastEventDate.Unix()
	contract.LastErrorDate = lastErrorDate.Unix()
	if glog.V(2) {
		glog.Infoln("Query contract", contract.Address, contract.Symbol, contract.TotalSupply, contract.LastEventDate)
		glog.Infoln("contract ABI", contract.ABI)
	}
	return contract, nil
}

type contractIterator struct {
	rows pgx.Rows
}

// implements common.Iterator interface
func (r *contractIterator) Value() interface{} {
	contract := &common.Contract{}
	var lastEventDate, lastErrorDate time.Time
	r.rows.Scan(
		&contract.Address,
		&contract.Name,
		&contract.Symbol,
		&contract.Decimals,
		&contract.TotalSupply,
		&lastEventDate,
		&lastErrorDate,
		&contract.ABI)
	contract.Address = "0x" + contract.Address
	contract.LastEventDate = lastEventDate.Unix()
	contract.LastErrorDate = lastErrorDate.Unix()
	return contract
}

func (r *contractIterator) Next() bool {
	return r.rows.Next()
}

func (r *contractIterator) Close() {
	r.rows.Close()
}

// acquires a connection and query contracts that are used in recent block days.
// must scan to end of the resultset to release the connection.
func QueryContracts(days int) (common.Iterator, error) {
	evtDt := time.Now().Add(time.Duration(-days*24) * time.Hour)
	sql := `SELECT Address, Name, Symbol, Decimals, TotalSupply, LastEventDate, LastErrorDate, ABI 
		FROM eth.contracts WHERE LastEventDate > $1`
	rows, err := db.Query(sql, evtDt)
	if err != nil {
		return nil, err
	}
	return &contractIterator{rows: rows}, err
}
