package proc

import (
	"encoding/hex"

	"math/big"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/open-dovetail/eth-track/contract/standard/erc1155"
	"github.com/open-dovetail/eth-track/contract/standard/erc721"
	"github.com/open-dovetail/eth-track/contract/standard/erc777"
	"github.com/pkg/errors"
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
	Address        string
	Name           string
	Symbol         string
	Decimals       uint8
	TotalSupply    *big.Int
	UpdatedTime    int64
	Methods        map[string]*abi.Method
	Events         map[string]*abi.Event
	StartEventTime int64 // first collected event time
	LastEventTime  int64 // last access time, used to cleanup in-memory cache
	ABI            string
}

// singleton contract cache
var contractCache = map[string]*Contract{}
var contractLock = &sync.Mutex{}

func GetContract(address string, blockTime int64) (*Contract, error) {
	contractLock.Lock()
	defer contractLock.Unlock()

	// find cached contract
	if contract, ok := contractCache[address]; ok {
		if glog.V(2) {
			glog.Infof("Found cached contract ABI for address %s methods=%d events=%d", address, len(contract.Methods), len(contract.Events))
		}
		if len(contract.ABI) == 0 {
			return nil, errors.Errorf("No ABI found for contract %s", address)
		}
		if blockTime > contract.LastEventTime {
			contract.LastEventTime = blockTime
		}
		if blockTime < contract.StartEventTime {
			contract.StartEventTime = blockTime
		}
		return contract, nil
	}

	// fetch contract from db
	// TODO

	contract, err := NewContract(address, blockTime)
	if err != nil {
		return nil, err
	}
	// TODO: store contract and abiData in database
	return contract, err
}

func NewContract(address string, blockTime int64) (*Contract, error) {
	// check etherscan API config
	api := GetEtherscanAPI()
	if api == nil {
		// Etherscan connection not configured, so quit immediately
		glog.Fatalln("Cannot find Etherscan config.  Must configure it using NewEtherscanAPI(apiKey, etherscanDelay)")
	}

	eventTime := blockTime
	if blockTime <= 0 {
		eventTime = time.Now().Unix()
	}
	contract := &Contract{
		Address:        address,
		UpdatedTime:    time.Now().Unix(),
		StartEventTime: eventTime,
		LastEventTime:  eventTime,
	}

	// Fetch ABI from etherscan
	if data, err := api.FetchABI(address); err == nil {
		contract.ABI = data
	} else {
		// Etherscan connection down, so quit immediately
		glog.Fatalf("Etherscan API failed to return ABI for address %s", address)
	}

	if err := contract.ParseABI(); err != nil {
		// cache contract w/o ABI so won't try again
		contract.ABI = ""
		contractCache[address] = contract
		return nil, errors.Wrapf(err, "Invalid ABI for address %s", address)
	}

	contract.updateERC20Properties()

	contractCache[address] = contract
	glog.Infof("Fetched contract %s Symbol %s methods=%d events=%d", address, contract.Symbol, len(contract.Methods), len(contract.Events))
	return contract, nil
}

func (c *Contract) ParseABI() error {
	ab, err := abi.NewABI(c.ABI)
	if err != nil {
		return errors.Wrapf(err, "'%s'", c.ABI)
	}

	if c.Methods == nil {
		c.Methods = make(map[string]*abi.Method)
	}
	if len(ab.Methods) > 0 {
		for _, mth := range ab.Methods {
			c.Methods[hex.EncodeToString(mth.ID())] = mth
		}
	}

	if c.Events == nil {
		c.Events = make(map[string]*abi.Event)
	}
	if len(ab.Events) > 0 {
		for _, evt := range ab.Events {
			c.Events[evt.ID().String()] = evt
		}
	}
	return nil
}

func (c *Contract) updateERC20Properties() {
	if client := GetEthereumClient(); client != nil {
		// try to set ERC token properties
		token := erc777.NewERC777(web3.HexToAddress(c.Address), client)
		if dec, err := token.Decimals(); err == nil {
			c.Decimals = dec
		}
		if name, err := token.Name(); err == nil {
			c.Name = name
		}
		if symbol, err := token.Symbol(); err == nil {
			c.Symbol = symbol
		}
		if totalSupply, err := token.TotalSupply(); err == nil {
			c.TotalSupply = totalSupply
		}
	}
}

// remove cached contract last accessed earlier than minAccessTime
func CleanupContractCache(minAccessTime int64) {
	for k, v := range contractCache {
		if v.LastEventTime < minAccessTime {
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

func DecodeTransactionInput(input []byte, address string, blockTime int64) (*DecodedData, error) {
	methodID := hex.EncodeToString(input[:4])
	method, ok := standardMethods[methodID]
	if !ok {
		// find contract method
		contract, err := GetContract(address, blockTime)
		if err != nil {
			return nil, err
		}
		if method, ok = contract.Methods[methodID]; !ok {
			return nil, errors.Errorf("Unknown method %s 0x%s", address, methodID)
		}
	}

	data, err := abi.Decode(method.Inputs, input[4:])
	if err != nil {
		return nil, err
	}
	dmap, ok := data.(map[string]interface{})
	if !ok {
		return nil, errors.Errorf("Decoded input data %T is not a map", data)
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

func DecodeEventData(wlog *web3.Log, blockTime int64) (*DecodedData, error) {
	eventID := wlog.Topics[0].String()
	var event *abi.Event
	var ok bool
	var data map[string]interface{}

	if event, ok = standardEvents[eventID]; ok {
		// try to parse w/ standard event
		var err error
		if data, err = event.ParseLog(wlog); err != nil {
			// not a standard event
			ok = false
		}
	}
	if !ok {
		// find contract event
		contract, err := GetContract(wlog.Address.String(), blockTime)
		if err != nil {
			return nil, err
		}
		if event, ok = contract.Events[eventID]; !ok {
			return nil, errors.Errorf("Unknown event %s %s", wlog.Address.String(), eventID)
		}
		if data, err = event.ParseLog(wlog); err != nil {
			return nil, errors.Wrapf(err, "Error parsing log %s", wlog.Address.String())
		}
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
