package proc

import (
	"encoding/hex"
	"strings"

	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/open-dovetail/eth-track/common"
	"github.com/open-dovetail/eth-track/contract/standard/erc1155"
	"github.com/open-dovetail/eth-track/contract/standard/erc721"
	"github.com/open-dovetail/eth-track/contract/standard/erc777"
	"github.com/open-dovetail/eth-track/redshift"
	"github.com/pkg/errors"
	web3 "github.com/umbracle/ethgo"
	"github.com/umbracle/ethgo/abi"
	econ "github.com/umbracle/ethgo/contract"
)

type contractMap struct {
	sync.Mutex
	stdMethods map[string]*abi.Method      // standard contract methods with ID as key
	stdEvents  map[string]*abi.Event       // standard contract events with ID as key
	contracts  map[string]*common.Contract // cached contracts by address
	created    map[string]*common.Contract // new contracts pending db persistence
}

// singleton contract cache
var contractCache *contractMap

func init() {
	contractCache = &contractMap{
		stdMethods: make(map[string]*abi.Method),
		stdEvents:  make(map[string]*abi.Event),
		contracts:  make(map[string]*common.Contract),
		created:    make(map[string]*common.Contract),
	}
	// set methods and events of standard ERC tokens
	for _, ab := range []*abi.ABI{erc777.ERC777Abi(), erc721.ERC721Abi(), erc1155.ERC1155Abi()} {
		for _, mth := range ab.Methods {
			id := hex.EncodeToString(mth.ID())
			if _, ok := contractCache.stdMethods[id]; !ok {
				contractCache.stdMethods[id] = mth
			}
		}
		for _, evt := range ab.Events {
			id := evt.ID().String()
			if _, ok := contractCache.stdEvents[id]; !ok {
				contractCache.stdEvents[id] = evt
			}
		}
	}
}

// return a contract by (1) lookup in-memory cache; (2) quey database; (3) fetch from etherscan
func GetContract(address string, blockTime int64) (*common.Contract, error) {
	contractCache.Lock()
	defer contractCache.Unlock()

	// find cached contract
	if contract, ok := contractCache.contracts[address]; ok {
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
	if contract, err := redshift.QueryContract(address); contract != nil && err == nil {
		contractCache.contracts[address] = contract
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

// query and cache contracts used in recent days -- used when restart the decode engine
func CacheContracts(days int) error {
	glog.Infof("retrieve knownn contracts from database that are active in recent %d days", days)
	rows, err := redshift.QueryContracts(days)
	if err != nil {
		return err
	}
	defer rows.Close()

	iter := 0
	for rows.Next() {
		contract := rows.Value().(*common.Contract)

		iter++
		if iter%1000 == 0 {
			glog.Infof("cache contract [%d] %s", iter, contract.Address)
		}
		contractCache.contracts[contract.Address] = contract
		if len(contract.ABI) > 0 {
			ParseABI(contract)
		}
	}
	return nil
}

func setContractEventTime(contract *common.Contract, blockTime int64) {
	eventTime := common.RoundToUTCDate(blockTime)
	if eventTime <= contract.LastEventDate {
		// update only for new event date
		return
	}
	contract.LastEventDate = eventTime
	if _, isNew := contractCache.created[contract.Address]; !isNew {
		if err := redshift.UpdateContract(contract); err != nil {
			glog.Warningf("Failed to update contract event time %s: %s", contract.Address, err.Error())
		}
	}
}

func setContractErrorTime(contract *common.Contract, blockTime int64) {
	eventTime := common.RoundToUTCDate(blockTime)
	if eventTime <= contract.LastErrorDate {
		// update only for new error date
		return
	}
	contract.LastErrorDate = eventTime
	if eventTime > contract.LastEventDate {
		contract.LastEventDate = eventTime
	}
	if _, isNew := contractCache.created[contract.Address]; !isNew {
		if err := redshift.UpdateContract(contract); err != nil {
			glog.Warningf("Failed to update contract error time %s: %s", contract.Address, err.Error())
		}
	}
}

func NewContract(address string, blockTime int64) (*common.Contract, error) {
	eventTime := common.RoundToUTCDate(blockTime)
	contract := &common.Contract{
		Address:       address,
		LastEventDate: eventTime,
	}

	// Fetch ABI from etherscan - retry 10 times on etherscan failure
	for retry := 1; retry <= 10; retry++ {
		if data, err := FetchABI(address, 0); err == nil {
			contract.ABI = data
			break
		} else {
			// Etherscan connection down, wait and retry
			glog.Warningf("Etherscan API failed %d times for address %s: %+v", retry, address, err)
			time.Sleep(time.Duration(10*retry) * time.Second)
		}
	}
	if len(contract.ABI) == 0 {
		// panic if etherscan fails
		glog.Fatalf("Etherscan API failed to return ABI for address %s", address)
	}

	updateERC20Properties(contract)
	contractCache.contracts[address] = contract

	err := ParseABI(contract)
	if err != nil {
		if glog.V(2) {
			glog.Info("Faied to parse ABI", err)
		}

		// do not store invalid ABI
		contract.ABI = ""
		contract.LastErrorDate = common.RoundToUTCDate(blockTime)
	}

	// insert contract to db
	if err := redshift.InsertContract(contract); err != nil {
		glog.Fatalf("Failed to insert contract %s to database: %+v", contract.Address, err)
	}

	if err != nil {
		return nil, errors.Wrapf(err, "Invalid ABI for address %s", address)
	}
	if glog.V(1) {
		glog.Infof("Created new contract %s Symbol %s methods=%d events=%d", address, contract.Symbol, len(contract.Methods), len(contract.Events))
	}
	return contract, nil

	/*
		// store other contracts to db in batches of 50
		contractCache.created[address] = contract
		if len(contractCache.created) >= 50 {
			//fmt.Println("Save new contracts", len(contractCache.contracts), len(contractCache.created))
			if err := redshift.InsertContracts(contractCache.created); err != nil {
				// panic if failed to save contracts
				glog.Fatalf("Failed to save %d contracts: %v", len(contractCache.created), err)
			}
			if glog.V(1) {
				glog.Infof("Saved %d contracts", len(contractCache.created))
			}
			contractCache.created = make(map[string]*common.Contract)
		}
		return contract, nil
	*/
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
		token := erc777.NewERC777(web3.HexToAddress(c.Address), econ.WithJsonRPC(client.Eth()))
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
			c.TotalSupply = common.BigIntToFloat(totalSupply)
		}
	}
}

// remove cached contract last accessed earlier than minAccessTime
func CleanupContractCache(minAccessTime int64) {
	for k, v := range contractCache.contracts {
		if v.LastEventDate < minAccessTime {
			delete(contractCache.contracts, k)
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
	method, ok := contractCache.stdMethods[methodID]
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

	if glog.V(2) {
		glog.Infof("decode contract %s method %s tx data %s", address, methodID, hex.EncodeToString(input))
		glog.Flush()
	}
	data, err := SafeAbiDecode(method.Inputs, input[4:])
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

// catch panic from abi decoder
func SafeAbiDecode(t *abi.Type, input []byte) (data interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			data = nil
			err = r.(error)
		}
	}()
	return abi.Decode(t, input)
}

func DecodeEventData(wlog *web3.Log, blockTime int64) (*DecodedData, error) {
	eventID := wlog.Topics[0].String()
	var event *abi.Event
	var ok bool
	var data map[string]interface{}

	if event, ok = contractCache.stdEvents[eventID]; ok {
		// try to parse w/ standard event
		var err error
		if data, err = event.ParseLog(wlog); err != nil {
			// not a standard event
			ok = false
		}
	}
	if !ok {
		// find contract event
		addr := strings.ToLower(wlog.Address.String())
		contract, err := GetContract(addr, blockTime)
		if err != nil {
			return nil, err
		}
		if event, ok = contract.Events[eventID]; !ok {
			setContractErrorTime(contract, blockTime)
			return nil, errors.Errorf("Unknown event %s %s", addr, eventID)
		}
		if data, err = event.ParseLog(wlog); err != nil {
			setContractErrorTime(contract, blockTime)
			return nil, errors.Wrapf(err, "Error parsing log %s", addr)
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
