package common

import (
	"math/big"
	"strings"
	"time"

	"github.com/golang/glog"
	web3 "github.com/umbracle/ethgo"
	"github.com/umbracle/ethgo/abi"
)

type Iterator interface {
	// points to the next item. returns true if there is another item.
	Next() bool
	// returns the current value
	Value() interface{}
	// cleanup the iterator
	Close()
}

type NamedValue struct {
	Name  string
	Kind  abi.Kind
	Value interface{}
}

type Contract struct {
	Address       string
	Name          string
	Symbol        string
	Decimals      uint8
	TotalSupply   float64
	LastEventDate int64  // last collected event date
	LastErrorDate int64  // last block time when tx/log parsing failed
	ABI           string // ABI from etherscan; blank if failed to parse
	Methods       map[string]*abi.Method
	Events        map[string]*abi.Event
}

type Block struct {
	Hash         string
	Number       uint64
	ParentHash   web3.Hash
	Miner        string
	Difficulty   *big.Int
	GasLimit     uint64
	GasUsed      uint64
	BlockTime    int64
	Status       bool // true for confirmed, false if not belong to confirmed chain
	Transactions map[string]*Transaction
	Logs         map[uint64]*EventLog
}

type Transaction struct {
	Hash        string
	BlockNumber uint64
	TxnIndex    uint64
	Status      bool // false means rejected transaction, or its block is not on confirmed chain
	From        string
	To          string
	Input       []byte
	Method      string // UNKNOWN indicates failure due to missing or bad contract ABI
	Params      []*NamedValue
	GasPrice    uint64
	Gas         uint64
	Value       *big.Int
	Nonce       uint64
	BlockTime   int64
}

type EventLog struct {
	BlockNumber uint64
	LogIndex    uint64
	Removed     bool // true means removed log, or its block is not on confirmed chain
	TxnIndex    uint64
	TxnHash     string
	Address     string
	Data        []byte
	Event       string // UNKNOWN indicates failure due to missing or bad contract ABI
	Params      []*NamedValue
	BlockTime   int64
}

type ProcessType int16

const (
	Undefined ProcessType = iota
	AddTransaction
	SetStatus
	AddEvent
)

func (p ProcessType) String() string {
	return [...]string{"unknown", "transaction", "status", "event"}[p]
}

type Progress struct {
	ProcessID    ProcessType
	HiBlock      uint64
	LowBlock     uint64
	HiBlockTime  int64
	LowBlockTime int64
}

func BigIntToFloat(i *big.Int) float64 {
	if i == nil {
		return 0
	}
	f := new(big.Float)
	f.SetInt(i)
	v, _ := f.Float64()
	return v
}

func StringToBigInt(s string) *big.Int {
	if bint, ok := new(big.Int).SetString(s, 10); ok {
		return bint
	}
	return nil
}

// convert Unix seconds to UTC time
func SecondsToDateTime(t int64) time.Time {
	return time.Unix(t, 0).UTC()
}

// round specified unix time to start of the UTC date
// if arg is 0, use current system time
func RoundToUTCDate(sec int64) int64 {
	var t time.Time
	if sec > 0 {
		t = time.Unix(sec, 0).UTC()
	} else {
		t = time.Now().UTC()
	}
	d := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	return d.Unix()
}

func HexToFixedString(h string, s int) string {
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
