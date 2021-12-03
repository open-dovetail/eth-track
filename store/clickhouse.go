package store

import (
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"time"

	clickhouse "github.com/mailru/go-clickhouse"
	"github.com/open-dovetail/eth-track/proc"
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
	return connect, nil
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

func (c *ClickHouseConnection) StartTx() (*ClickHouseTransaction, error) {
	tx, err := c.connection.Begin()
	if err != nil {
		return nil, err
	}
	// tx.Prepare(`set profile='async_insert'`)
	t := &ClickHouseTransaction{
		tx:    tx,
		stmts: make(map[string]*sql.Stmt),
	}
	if err := t.prepareContractStmt(); err != nil {
		return nil, err
	}
	if err := t.prepareBlockStmt(); err != nil {
		return nil, err
	}
	if err := t.prepareTransactionStmt(); err != nil {
		return nil, err
	}
	if err := t.prepareLogStmt(); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *ClickHouseTransaction) CommitTx() error {
	return t.tx.Commit()
}

func (t *ClickHouseTransaction) RollbackTx() error {
	return t.tx.Rollback()
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

func (t *ClickHouseTransaction) InsertContract(contract *proc.Contract, abi string) error {
	stmt, ok := t.stmts["contract"]
	if !ok {
		return fmt.Errorf("Contract statement is not prepared for ClickHouse transaction")
	}

	_, err := stmt.Exec(
		hexToFixedString(contract.Address, 40),
		contract.Name,
		contract.Symbol,
		contract.Decimals,
		bigIntToFloat(contract.TotalSupply),
		clickhouse.Date(time.Unix(contract.UpdatedTime, 0)),
		clickhouse.Date(time.Unix(contract.StartEventTime, 0)),
		clickhouse.Date(time.Unix(contract.LastEventTime, 0)),
		abi,
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
				BlockTime
			) VALUES (
				?, ?, ?, ?, ?, ?, ?, ?
			)`)
		if err != nil {
			return err
		}
		t.stmts["block"] = stmt
	}
	return nil
}

func (t *ClickHouseTransaction) InsertBlock(block *proc.Block) error {
	stmt, ok := t.stmts["block"]
	if !ok {
		return fmt.Errorf("block statement is not prepared for ClickHouse transaction")
	}

	_, err := stmt.Exec(
		hexToFixedString(block.Hash, 64),
		block.Number,
		hexToFixedString(block.ParentHash.String(), 64),
		block.Miner,
		bigIntToFloat(block.Difficulty),
		block.GasLimit,
		block.GasUsed,
		time.Unix(block.BlockTime, 0),
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

func (t *ClickHouseTransaction) InsertTransaction(transaction *proc.Transaction) error {
	stmt, ok := t.stmts["transaction"]
	if !ok {
		return fmt.Errorf("transaction statement is not prepared for ClickHouse transaction")
	}

	params := paramsToValuers(transaction.Params)
	var status int8
	if transaction.Status {
		status = 1
	}
	_, err := stmt.Exec(
		hexToFixedString(transaction.Hash, 64),
		transaction.BlockNumber,
		transaction.TxnIndex,
		status,
		hexToFixedString(transaction.From, 40),
		hexToFixedString(transaction.To, 40),
		transaction.Method,
		params.Name,
		params.Seq,
		params.ValueString,
		params.ValueDouble,
		transaction.GasPrice,
		transaction.Gas,
		bigIntToFloat(transaction.Value),
		transaction.Nonce,
		time.Unix(transaction.BlockTime, 0),
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
				ContractAddr,
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

func (t *ClickHouseTransaction) InsertLog(eventlog *proc.EventLog) error {
	stmt, ok := t.stmts["log"]
	if !ok {
		return fmt.Errorf("eventlog statement is not prepared for ClickHouse transaction")
	}

	params := paramsToValuers(eventlog.Params)
	var removed int8
	if eventlog.Removed {
		removed = 1
	}
	_, err := stmt.Exec(
		eventlog.BlockNumber,
		eventlog.LogIndex,
		removed,
		eventlog.TxnIndex,
		hexToFixedString(eventlog.TxnHash, 64),
		hexToFixedString(eventlog.ContractAddr, 40),
		eventlog.Event,
		params.Name,
		params.Seq,
		params.ValueString,
		params.ValueDouble,
		time.Unix(eventlog.BlockTime, 0),
	)
	return err
}

func bigIntToFloat(i *big.Int) float64 {
	f := new(big.Float)
	f.SetInt(i)
	v, _ := f.Float64()
	return v
}

func hexToFixedString(h string, s int) string {
	var result string
	if strings.HasPrefix(h, "0x") {
		result = h[2:]
	}
	if len(result) > s {
		log.Printf("WARN - hex string is more than %d characters long: %s", s, result)
		return result[:s]
	}
	return result
}

// convert nested params into array for clickhouse insert
func paramsToValuers(params []*proc.NamedValue) *ParamsValuer {
	if params == nil || len(params) == 0 {
		return &ParamsValuer{
			Name:        clickhouse.Array(nil),
			Seq:         clickhouse.Array(nil),
			ValueString: clickhouse.Array(nil),
			ValueDouble: clickhouse.Array(nil),
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
			fmt.Printf("Input %s %s %T %s\n", v.Name, v.Kind.String(), v.Value, sp)
			if matched, _ := regexp.MatchString(`^".*"$`, sp); matched {
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
					log.Printf("WARN - failed to convert digits to float64: %s", sp)
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
