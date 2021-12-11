package proc

import (
	"encoding/hex"

	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/open-dovetail/eth-track/common"
	"github.com/open-dovetail/eth-track/contract/standard/erc1155"
	"github.com/open-dovetail/eth-track/contract/standard/erc721"
	"github.com/open-dovetail/eth-track/contract/standard/erc777"
	"github.com/open-dovetail/eth-track/store"
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

// singleton contract cache
var contractCache = map[string]*common.Contract{}
var contractLock = &sync.Mutex{}

// return a contract by (1) lookup in-memory cache; (2) quey database; (3) fetch from etherscan
func GetContract(address string, blockTime int64) (*common.Contract, error) {
	contractLock.Lock()
	defer contractLock.Unlock()

	// find cached contract
	if contract, ok := contractCache[address]; ok {
		if glog.V(2) {
			glog.Infof("Found cached contract ABI for address %s Symbol %s methods=%d events=%d", address, contract.Symbol, len(contract.Methods), len(contract.Events))
		}
		if len(contract.ABI) == 0 {
			setContractErrorTime(contract, blockTime)
			return nil, errors.Errorf("No ABI in cached contract %s", address)
		}
		setContractEventTime(contract, blockTime)
		return contract, nil
	}

	// fetch contract from db
	if contract, err := store.QueryContract(address); contract != nil && err == nil {
		contractCache[address] = contract
		if err := ParseABI(contract); err != nil {
			setContractErrorTime(contract, blockTime)
			return nil, errors.Wrapf(err, "Query returned")
		}
		setContractEventTime(contract, blockTime)
		if glog.V(1) {
			glog.Infof("Query returned contract for address %s Symbol %s methods=%d events=%d", address, contract.Symbol, len(contract.Methods), len(contract.Events))
		}
		return contract, nil
	}

	// create new contract
	return NewContract(address, blockTime)
}

func setContractEventTime(contract *common.Contract, blockTime int64) {
	updated := false
	eventTime := roundToUTCDate(blockTime)
	if eventTime > contract.LastEventTime {
		contract.LastEventTime = eventTime
		updated = true
	}
	if eventTime < contract.StartEventTime {
		contract.StartEventTime = eventTime
		updated = true
	}
	if updated {
		if err := insertData(contract); err != nil {
			glog.Warningf("Failed to update contract event time %s: %s", contract.Address, err.Error())
		}
	}
}

func setContractErrorTime(contract *common.Contract, blockTime int64) {
	if blockTime > contract.LastErrorTime {
		contract.LastErrorTime = blockTime
		eventTime := roundToUTCDate(blockTime)
		if eventTime > contract.LastEventTime {
			contract.LastEventTime = eventTime
		}
		if eventTime < contract.StartEventTime {
			contract.StartEventTime = eventTime
		}
		if err := insertData(contract); err != nil {
			glog.Warningf("Failed to update contract error time %s: %s", contract.Address, err.Error())
		}
	}
}

func NewContract(address string, blockTime int64) (*common.Contract, error) {
	// check etherscan API config
	api := GetEtherscanAPI()
	if api == nil {
		// Etherscan connection not configured, so quit immediately
		glog.Fatalln("Cannot find Etherscan config.  Must configure it using NewEtherscanAPI(apiKey, etherscanDelay)")
	}

	eventTime := roundToUTCDate(blockTime)
	contract := &common.Contract{
		Address:        address,
		UpdatedTime:    roundToUTCDate(0),
		StartEventTime: eventTime,
		LastEventTime:  eventTime,
	}

	// Fetch ABI from etherscan - retry 3 times on etherscan failure
	for retry := 1; retry <= 3; retry++ {
		if data, err := api.FetchABI(address); err == nil {
			contract.ABI = data
			break
		} else {
			// Etherscan connection down, wait and retry
			glog.Warningf("Etherscan API failed %d times for address %s: %+v", retry, address, err)
			time.Sleep(10 * time.Second)
		}
	}
	if len(contract.ABI) == 0 {
		glog.Fatalf("Etherscan API failed to return ABI for address %s", address)
	}

	if err := ParseABI(contract); err != nil {
		// cache contract w/o ABI so won't try again
		contract.ABI = ""
		contractCache[address] = contract
		setContractErrorTime(contract, blockTime)
		return nil, errors.Wrapf(err, "Invalid ABI for address %s", address)
	}

	updateERC20Properties(contract)
	contractCache[address] = contract
	if err := insertData(contract); err != nil {
		glog.Warningf("Failed to insert contract %s: %s", address, err.Error())
	}

	if glog.V(1) {
		glog.Infof("Created new contract %s Symbol %s methods=%d events=%d", address, contract.Symbol, len(contract.Methods), len(contract.Events))
	}
	return contract, nil
}

// round specified unix time to start of the UTC date
// if arg is 0, use current system time
func roundToUTCDate(sec int64) int64 {
	var t time.Time
	if sec > 0 {
		t = time.Unix(sec, 0).UTC()
	} else {
		t = time.Now().UTC()
	}
	d := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	return d.Unix()
}

func ParseABI(c *common.Contract) error {
	if len(c.ABI) == 0 {
		return errors.Errorf("No ABI in contract %s", c.Address)
	}

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

func updateERC20Properties(c *common.Contract) {
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

type DecodedData struct {
	Name   string // name of method or event
	ID     string // ID of method or event
	Params []*common.NamedValue
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
			setContractErrorTime(contract, blockTime)
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
		Params: []*common.NamedValue{},
	}
	for _, elem := range method.Inputs.TupleElems() {
		dec.Params = append(dec.Params, &common.NamedValue{
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
			setContractErrorTime(contract, blockTime)
			return nil, errors.Errorf("Unknown event %s %s", wlog.Address.String(), eventID)
		}
		if data, err = event.ParseLog(wlog); err != nil {
			setContractErrorTime(contract, blockTime)
			return nil, errors.Wrapf(err, "Error parsing log %s", wlog.Address.String())
		}
	}

	dec := &DecodedData{
		Name:   event.Name,
		ID:     eventID,
		Params: []*common.NamedValue{},
	}
	for _, elem := range event.Inputs.TupleElems() {
		dec.Params = append(dec.Params, &common.NamedValue{
			Name:  elem.Name,
			Kind:  elem.Elem.Kind(),
			Value: data[elem.Name],
		})
	}
	return dec, nil
}
