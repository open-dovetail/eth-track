package store

import (
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	clickhouse "github.com/mailru/go-clickhouse"
	"github.com/open-dovetail/eth-track/common"
	"github.com/pkg/errors"
	"github.com/umbracle/go-web3"
)

type ClickHouseConnection struct {
	url        string
	connection *sql.DB
}

type ClickHouseTransaction struct {
	tx    *sql.Tx
	stmts map[string]*sql.Stmt
}

type ParamsValuer struct {
	Name        driver.Valuer
	Seq         driver.Valuer
	ValueString driver.Valuer
	ValueDouble driver.Valuer
}

// singleton
var db *ClickHouseConnection
var txn *ClickHouseTransaction
var txnLock = &sync.Mutex{}

// open a clickhouse db connection, e.g.
// NewClickHouseConnection("http://127.0.0.1:8123", "default", map[string]string{"debug": "1"})
func NewClickHouseConnection(dbURL string, dbName string, params map[string]string) (*ClickHouseConnection, error) {
	u, err := url.Parse(dbURL)
	if err != nil {
		return nil, err
	}

	u.Path = dbName
	if len(params) > 0 {
		q := u.Query()
		for k, v := range params {
			q.Set(k, v)
		}
		u.RawQuery = q.Encode()
	}

	connect := &ClickHouseConnection{url: u.String()}
	if err := connect.Open(); err != nil {
		return nil, err
	}
	db = connect
	return connect, nil
}

func GetDBConnection() *ClickHouseConnection {
	return db
}

func MustGetDBTx() *ClickHouseTransaction {
	if txn != nil {
		return txn
	}

	tx, _ := GetDBTx()
	return tx
}

func GetDBTx() (*ClickHouseTransaction, error) {
	txnLock.Lock()
	defer txnLock.Unlock()

	if txn != nil {
		return txn, nil
	}
	t, err := db.startTx()
	if err != nil {
		return nil, err
	}
	txn = t
	return t, err
}

func (c *ClickHouseConnection) Open() error {
	connect, err := sql.Open("clickhouse", c.url)
	if err != nil {
		return err
	}
	if err := connect.Ping(); err != nil {
		return err
	}

	c.connection = connect
	return nil
}

func (c *ClickHouseConnection) Close() error {
	return c.connection.Close()
}

func (c *ClickHouseConnection) Query(sql string, args ...interface{}) (*sql.Rows, error) {
	return c.connection.Query(sql, args...)
}

func QueryContract(address string) (*common.Contract, error) {
	if db == nil {
		return nil, errors.New("Database connection is not initialized")
	}

	rows, err := db.Query(`
		SELECT
			Name,
			Symbol,
			Decimals,
			TotalSupply,
			UpdatedDate,
			StartEventDate,
			LastEventDate,
			ABI
		FROM contracts
		WHERE Address = ?`, address[2:])

	if err != nil {
		return nil, errors.Wrapf(err, "Failed to query contract %s", address)
	}

	// Gets the first returned row because one result is enough for ReplacingMergeTree
	defer rows.Close()

	if rows.Next() {
		contract := &common.Contract{Address: address}

		var totalSupply float64
		// clickhouse stores date w/o timezone, and
		// go-clickhouse dataparser.go parses query result using UTC by default
		// Note: parser timezone can be overriden in request URL with parameter, e.g. location=UTC,
		//       which would set time location to time.LoadLocation(loc) - ref go-clickhouse/config.go
		var updatedTime, startEventTime, lastEventTime time.Time

		if err := rows.Scan(
			&contract.Name,
			&contract.Symbol,
			&contract.Decimals,
			&totalSupply,
			&updatedTime,
			&startEventTime,
			&lastEventTime,
			&contract.ABI,
		); err != nil {
			return nil, errors.Wrapf(err, "Failed to parse query result for %s", address)
		}

		contract.TotalSupply = floatToBigInt(totalSupply)
		contract.UpdatedTime = updatedTime.Unix()
		contract.StartEventTime = startEventTime.Unix()
		contract.LastEventTime = lastEventTime.Unix()
		if glog.V(2) {
			glog.Infoln("Query contract", contract.Address, contract.Symbol, contract.TotalSupply, contract.UpdatedTime)
			glog.Infoln("contract ABI", contract.ABI)
		}
		return contract, nil
	}
	return nil, nil
}

func (c *ClickHouseConnection) startTx() (*ClickHouseTransaction, error) {
	if txn != nil {
		return txn, errors.New("Previous transaction has not been committed or rolled back")
	}

	tx, err := c.connection.Begin()
	if err != nil {
		return nil, err
	}
	// tx.Prepare(`set profile='async_insert'`)
	txn = &ClickHouseTransaction{
		tx:    tx,
		stmts: make(map[string]*sql.Stmt),
	}
	if err := txn.prepareContractStmt(); err != nil {
		return nil, err
	}
	if err := txn.prepareBlockStmt(); err != nil {
		return nil, err
	}
	if err := txn.prepareTransactionStmt(); err != nil {
		return nil, err
	}
	if err := txn.prepareLogStmt(); err != nil {
		return nil, err
	}
	return txn, nil
}

func (t *ClickHouseTransaction) CommitTx() error {
	txnLock.Lock()
	defer txnLock.Unlock()

	err := t.tx.Commit()
	txn = nil
	return err
}

func (t *ClickHouseTransaction) RollbackTx() error {
	txnLock.Lock()
	defer txnLock.Unlock()

	err := t.tx.Rollback()
	txn = nil
	return err
}

func (t *ClickHouseTransaction) prepareContractStmt() error {
	if _, ok := t.stmts["contract"]; !ok {
		stmt, err := t.tx.Prepare(`
			INSERT INTO contracts (
				Address,
				Name,
				Symbol,
				Decimals,
				TotalSupply,
				UpdatedDate,
				StartEventDate,
				LastEventDate,
				ABI
			) VALUES (
				?, ?, ?, ?, ?, ?, ?, ?, ?
			)`)
		if err != nil {
			return err
		}
		t.stmts["contract"] = stmt
	}
	return nil
}

func (t *ClickHouseTransaction) InsertContract(contract *common.Contract) error {
	txnLock.Lock()
	defer txnLock.Unlock()

	stmt, ok := t.stmts["contract"]
	if !ok {
		return errors.New("Contract statement is not prepared for ClickHouse transaction")
	}

	_, err := stmt.Exec(
		hexToFixedString(contract.Address, 40),
		contract.Name,
		contract.Symbol,
		contract.Decimals,
		bigIntToFloat(contract.TotalSupply),
		clickhouse.Date(secondsToDateTime(contract.UpdatedTime)),
		clickhouse.Date(secondsToDateTime(contract.StartEventTime)),
		clickhouse.Date(secondsToDateTime(contract.LastEventTime)),
		contract.ABI,
	)
	return err
}

func (t *ClickHouseTransaction) prepareBlockStmt() error {
	if _, ok := t.stmts["block"]; !ok {
		stmt, err := t.tx.Prepare(`
			INSERT INTO blocks (
				Hash,
				Number,
				ParentHash,
				Miner,
				Difficulty,
				GasLimit,
				GasUsed,
				Status,
				BlockTime
			) VALUES (
				?, ?, ?, ?, ?, ?, ?, ?, ?
			)`)
		if err != nil {
			return err
		}
		t.stmts["block"] = stmt
	}
	return nil
}

func (t *ClickHouseTransaction) InsertBlock(block *common.Block) error {
	txnLock.Lock()
	defer txnLock.Unlock()

	stmt, ok := t.stmts["block"]
	if !ok {
		return errors.New("block statement is not prepared for ClickHouse transaction")
	}

	var status = int8(-1)
	if block.Status {
		status = 1
	}
	_, err := stmt.Exec(
		hexToFixedString(block.Hash, 64),
		clickhouse.UInt64(block.Number),
		hexToFixedString(block.ParentHash.String(), 64),
		hexToFixedString(block.Miner, 40),
		bigIntToFloat(block.Difficulty),
		clickhouse.UInt64(block.GasLimit),
		clickhouse.UInt64(block.GasUsed),
		status,
		secondsToDateTime(block.BlockTime),
	)
	return err
}

func (t *ClickHouseTransaction) prepareTransactionStmt() error {
	if _, ok := t.stmts["transaction"]; !ok {
		stmt, err := t.tx.Prepare(`
			INSERT INTO transactions (
				Hash,
				BlockNumber,
				TxnIndex,
				Status,
				From,
				To,
				Method,
				Params.Name,
				Params.Seq,
				Params.ValueString,
				Params.ValueDouble,
				GasPrice,
				Gas,
				Value,
				Nonce,
				BlockTime
			) VALUES (
				?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
			)`)
		if err != nil {
			return err
		}
		t.stmts["transaction"] = stmt
	}
	return nil
}

func (t *ClickHouseTransaction) InsertTransaction(transaction *common.Transaction) error {
	txnLock.Lock()
	defer txnLock.Unlock()

	stmt, ok := t.stmts["transaction"]
	if !ok {
		return errors.New("transaction statement is not prepared for ClickHouse transaction")
	}

	params := paramsToValuers(transaction.Params)
	var status = int8(-1)
	if transaction.Status {
		status = 1
	}
	_, err := stmt.Exec(
		hexToFixedString(transaction.Hash, 64),
		clickhouse.UInt64(transaction.BlockNumber),
		clickhouse.UInt64(transaction.TxnIndex),
		status,
		hexToFixedString(transaction.From, 40),
		hexToFixedString(transaction.To, 40),
		transaction.Method,
		params.Name,
		params.Seq,
		params.ValueString,
		params.ValueDouble,
		clickhouse.UInt64(transaction.GasPrice),
		clickhouse.UInt64(transaction.Gas),
		bigIntToFloat(transaction.Value),
		clickhouse.UInt64(transaction.Nonce),
		secondsToDateTime(transaction.BlockTime),
	)
	return err
}

func (t *ClickHouseTransaction) prepareLogStmt() error {
	if _, ok := t.stmts["log"]; !ok {
		stmt, err := t.tx.Prepare(`
			INSERT INTO logs (
				BlockNumber,
				LogIndex,
				Removed,
				TxnIndex,
				TxnHash,
				Address,
				Event,
				Params.Name,
				Params.Seq,
				Params.ValueString,
				Params.ValueDouble,
				BlockTime
			) VALUES (
				?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
			)`)
		if err != nil {
			return err
		}
		t.stmts["log"] = stmt
	}
	return nil
}

func (t *ClickHouseTransaction) InsertLog(eventlog *common.EventLog) error {
	txnLock.Lock()
	defer txnLock.Unlock()

	stmt, ok := t.stmts["log"]
	if !ok {
		return errors.New("eventlog statement is not prepared for ClickHouse transaction")
	}

	params := paramsToValuers(eventlog.Params)
	var removed = int8(-1)
	if eventlog.Removed {
		removed = 1
	}
	_, err := stmt.Exec(
		clickhouse.UInt64(eventlog.BlockNumber),
		clickhouse.UInt64(eventlog.LogIndex),
		removed,
		clickhouse.UInt64(eventlog.TxnIndex),
		hexToFixedString(eventlog.TxnHash, 64),
		hexToFixedString(eventlog.Address, 40),
		eventlog.Event,
		params.Name,
		params.Seq,
		params.ValueString,
		params.ValueDouble,
		secondsToDateTime(eventlog.BlockTime),
	)
	return err
}

// convert Unix seconds to UTC time
func secondsToDateTime(t int64) time.Time {
	return time.Unix(t, 0).UTC()
}

// zero out time from DateTime, then return Unix seconds
func timeToDate(t time.Time) int64 {
	d := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	return d.Unix()
}

func bigIntToFloat(i *big.Int) float64 {
	if i == nil {
		return 0
	}
	f := new(big.Float)
	f.SetInt(i)
	v, _ := f.Float64()
	return v
}

func floatToBigInt(f float64) *big.Int {
	bf := big.NewFloat(f)
	i, _ := bf.Int(nil)
	return i
}

func hexToFixedString(h string, s int) string {
	var result string
	if strings.HasPrefix(h, "0x") {
		result = h[2:]
	}
	if len(result) > s {
		glog.Warningf("hex string is more than %d characters long: %s", s, result)
		return result[:s]
	}
	return result
}

// convert nested params into array for clickhouse insert
func paramsToValuers(params []*common.NamedValue) *ParamsValuer {
	if params == nil || len(params) == 0 {
		return &ParamsValuer{
			Name:        nil,
			Seq:         nil,
			ValueString: nil,
			ValueDouble: nil,
		}
	}
	names := make([]string, len(params))
	seqs := make([]int8, len(params))
	stringValues := make([]string, len(params))
	doubleValues := make([]float64, len(params))
	for i, v := range params {
		names[i] = v.Name
		seqs[i] = int8(i)

		value := v.Value
		if v.Kind.String() != "Bytes" {
			// replace all []uint8 fields using hex encoding
			value = hexEncodeUint8Array(v.Value)
		}
		if p, err := json.Marshal(value); err == nil {
			sp := string(p)
			if glog.V(2) {
				glog.Infof("Input %s %s %T %s", v.Name, v.Kind.String(), v.Value, sp)
			}
			if sp == "true" {
				doubleValues[i] = 1
			} else if sp == "false" {
				doubleValues[i] = 0
			} else if sp == "null" {
				stringValues[i] = ""
			} else if matched, _ := regexp.MatchString(`^".*"$`, sp); matched {
				stringValues[i] = sp[1 : len(sp)-1]
			} else if matched, _ := regexp.MatchString(`^\{.*\}$`, sp); matched {
				stringValues[i] = sp
			} else if matched, _ := regexp.MatchString(`^\[.*\]$`, sp); matched {
				stringValues[i] = sp
			} else {
				f := new(big.Float)
				if f, ok := f.SetString(sp); ok {
					v, _ := f.Float64()
					doubleValues[i] = v
				} else {
					glog.Warningf("Failed to convert digits to float64: %s", sp)
					stringValues[i] = sp
				}
			}
		}
	}
	return &ParamsValuer{
		Name:        clickhouse.Array(names),
		Seq:         clickhouse.Array(seqs),
		ValueString: clickhouse.Array(stringValues),
		ValueDouble: clickhouse.Array(doubleValues),
	}
}

// replace all []uint8 with hex encoding in the input data
func hexEncodeUint8Array(data interface{}) interface{} {
	if reflect.TypeOf(data) == reflect.TypeOf(web3.Address{}) {
		// do not re-encode for address
		return data
	}
	ref := reflect.ValueOf(data)
	switch ref.Kind() {
	case reflect.Map:
		result := make(map[string]interface{})
		for k, v := range data.(map[string]interface{}) {
			result[k] = hexEncodeUint8Array(v)
		}
		return result
	case reflect.Array, reflect.Slice:
		if ref.Len() > 0 {
			if ref.Index(0).Kind() == reflect.Uint8 {
				// convert array to slice for hex encoding
				b := make([]uint8, ref.Len(), ref.Len())
				for i := 0; i < ref.Len(); i++ {
					b[i] = uint8(ref.Index(i).Uint())
				}
				return "0x" + hex.EncodeToString(b)
			} else {
				result := make([]interface{}, ref.Len(), ref.Len())
				for i := 0; i < ref.Len(); i++ {
					result[i] = hexEncodeUint8Array(ref.Index(i).Interface())
				}
				return result
			}
		}
	}
	return data
}
