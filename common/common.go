package common

import (
	"math/big"

	web3 "github.com/umbracle/go-web3"
	"github.com/umbracle/go-web3/abi"
)

type NamedValue struct {
	Name  string
	Kind  abi.Kind
	Value interface{}
}

type Contract struct {
	Address        string
	Name           string
	Symbol         string
	Decimals       uint8
	TotalSupply    *big.Int
	Methods        map[string]*abi.Method
	Events         map[string]*abi.Event
	UpdatedTime    int64  // date when ERC20 properties and ABI was updated
	StartEventTime int64  // first collected event date
	LastEventTime  int64  // last collected event date
	LastErrorTime  int64  // last block time when tx/log parsing failed
	ABI            string // ABI from etherscan; blank if failed to parse
}

type Block struct {
	Hash       string
	Number     uint64
	ParentHash web3.Hash
	Miner      string
	Difficulty *big.Int
	GasLimit   uint64
	GasUsed    uint64
	BlockTime  int64
	Status     bool // true for confirmed, false if not belong to confirmed chain
}

type Transaction struct {
	Hash        string
	BlockNumber uint64
	TxnIndex    uint64
	Status      bool // false means rejected transaction, or its block is not on confirmed chain
	From        string
	To          string
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
	Event       string // UNKNOWN indicates failure due to missing or bad contract ABI
	Params      []*NamedValue
	BlockTime   int64
}
