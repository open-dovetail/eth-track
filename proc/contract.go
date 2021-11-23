package proc

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/open-dovetail/eth-track/contract/standard/erc1155"
	"github.com/open-dovetail/eth-track/contract/standard/erc721"
	"github.com/open-dovetail/eth-track/contract/standard/erc777"
	web3 "github.com/umbracle/go-web3"
	"github.com/umbracle/go-web3/abi"
)

// standard contract methods/events with ID as key
var standardMethods = make(map[string]*abi.Method)
var standardEvents = make(map[string]*abi.Event)

func init() {
	// set methods and events of standard ERC tokens
	for _, ab := range []*abi.ABI{erc777.ERC777Abi(), erc721.ERC721Abi(), erc1155.ERC1155Abi()} {
		for _, mth := range ab.Methods {
			id := hex.EncodeToString(mth.ID())
			if _, ok := standardMethods[id]; !ok {
				standardMethods[id] = mth
			}
		}
		for _, evt := range ab.Events {
			id := evt.ID().String()
			if _, ok := standardEvents[id]; !ok {
				standardEvents[id] = evt
			}
		}
	}
}

type Contract struct {
	Address     string
	Name        string
	Symbol      string
	Decimals    uint8
	TotalSupply *big.Int
	CreateTime  int64
	Methods     map[string]*abi.Method
	Events      map[string]*abi.Event
	AccessTime  int64 // last access time, used to cleanup in-memory cache
}

// singleton contract cache
var contractCache = map[string]*Contract{}
var contractLock = &sync.Mutex{}

func NewContract(address string) (*Contract, error) {
	contractLock.Lock()
	defer contractLock.Unlock()

	contract, ok := contractCache[address]
	if ok {
		// fmt.Println("Return contract from cache", address)
		contract.AccessTime = time.Now().UnixNano() / 1000000
		return contract, nil
	}
	conf := GetConfig()
	if conf == nil {
		return nil, fmt.Errorf("Cannot find config.  Must configure the process using NewConfig(nodeURL, apiKey, etherscanDelay)")
	}

	contract = &Contract{
		Address:    address,
		Methods:    map[string]*abi.Method{},
		Events:     map[string]*abi.Event{},
		CreateTime: time.Now().Unix(),
		AccessTime: time.Now().UnixNano() / 1000000,
	}

	ab, err := conf.FetchABI(address)
	if err != nil {
		// cache contract w/o ABI so won't try again
		fmt.Println("Cache unknown contract source", address, err)
		contractCache[address] = contract
		return nil, err
	}

	if len(ab.Methods) > 0 {
		for _, mth := range ab.Methods {
			contract.Methods[hex.EncodeToString(mth.ID())] = mth
		}
	}
	if len(ab.Events) > 0 {
		for _, evt := range ab.Events {
			contract.Events[evt.ID().String()] = evt
		}
	}

	if client := conf.GetClient(); client != nil {
		// try to set ERC token properties
		token := erc777.NewERC777(web3.HexToAddress(address), client)
		if dec, err := token.Decimals(); err == nil {
			contract.Decimals = dec
		}
		if name, err := token.Name(); err == nil {
			contract.Name = name
		}
		if symbol, err := token.Symbol(); err == nil {
			contract.Symbol = symbol
		}
		if totalSupply, err := token.TotalSupply(); err == nil {
			contract.TotalSupply = totalSupply
		}
	}
	contractCache[address] = contract
	return contract, nil
}

// remove cached contract last accessed earlier than minAccessTime
func CleanupContractCache(minAccessTime int64) {
	for k, v := range contractCache {
		if v.AccessTime < minAccessTime {
			delete(contractCache, k)
		}
	}
}

type NamedValue struct {
	Name  string
	Kind  abi.Kind
	Value interface{}
}

type DecodedData struct {
	Name   string // name of method or event
	ID     string // ID of method or event
	Params []*NamedValue
}

func DecodeTransactionInput(input []byte, address string) (*DecodedData, error) {
	methodID := hex.EncodeToString(input[:4])
	method, ok := standardMethods[methodID]
	if !ok {
		// find contract method
		contract, err := NewContract(address)
		if err != nil {
			return nil, err
		}
		if method, ok = contract.Methods[methodID]; !ok {
			return nil, fmt.Errorf("Unknown method 0x%s", methodID)
		}
	}

	data, err := abi.Decode(method.Inputs, input[4:])
	if err != nil {
		return nil, err
	}
	dmap, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Decoded input data %T is not a map", data)
	}
	dec := &DecodedData{
		Name:   method.Name,
		ID:     methodID,
		Params: []*NamedValue{},
	}
	for _, elem := range method.Inputs.TupleElems() {
		dec.Params = append(dec.Params, &NamedValue{
			Name:  elem.Name,
			Kind:  elem.Elem.Kind(),
			Value: dmap[elem.Name],
		})
	}
	return dec, nil
}

func DecodeEventData(wlog *web3.Log) (*DecodedData, error) {
	eventID := wlog.Topics[0].String()
	event, ok := standardEvents[eventID]
	if !ok {
		// find contract method
		contract, err := NewContract(wlog.Address.String())
		if err != nil {
			return nil, err
		}
		if event, ok = contract.Events[eventID]; !ok {
			return nil, fmt.Errorf("Unknown event %s", eventID)
		}
	}

	data, err := event.ParseLog(wlog)
	if err != nil {
		return nil, err
	}

	dec := &DecodedData{
		Name:   event.Name,
		ID:     eventID,
		Params: []*NamedValue{},
	}
	for _, elem := range event.Inputs.TupleElems() {
		dec.Params = append(dec.Params, &NamedValue{
			Name:  elem.Name,
			Kind:  elem.Elem.Kind(),
			Value: data[elem.Name],
		})
	}
	return dec, nil
}
