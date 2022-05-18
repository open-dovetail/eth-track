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

// return a contract by (1) lookup in-memory cache; (2) quey database; (3) fetch from etherscan.
// return fatal error if failed to connect to etherscan or save batched contracts to database.
func getContract(address string, blockTime int64) (*common.Contract, error) {
	contractCache.Lock()
	defer contractCache.Unlock()

	// find cached contract
	if contract, ok := contractCache.contracts[address]; ok {
		if glog.V(2) {
			glog.Infof("Found cached contract ABI for address %s Symbol %s methods=%d events=%d", address, contract.Symbol, len(contract.Methods), len(contract.Events))
		}
		return contract, nil
	}

	// fetch contract from db
	if contract, err := redshift.QueryContract(address); contract != nil && err == nil {
		contractCache.contracts[address] = contract
		parseABI(contract)
		if glog.V(1) {
			glog.Infof("Query returned contract for address %s Symbol %s methods=%d events=%d", address, contract.Symbol, len(contract.Methods), len(contract.Events))
		}
		return contract, nil
	}

	// create new contract
	return newContract(address, blockTime)
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
			parseABI(contract)
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

// create new contract by fetching ABI from etherscan
// return fatal error if failed to connect to etherscan or save to database
func newContract(address string, blockTime int64) (*common.Contract, error) {
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
		glog.Errorf("Failed to fetch ABI from etherscan for contract %s", address)
		return nil, errors.Errorf("Failed to fetch ABI from etherscan for contract %s", address)
	}

	updateERC20Properties(contract)
	contractCache.contracts[address] = contract

	// parse ABI to set definitions of methods and events
	if err := parseABI(contract); err != nil {
		if glog.V(2) {
			glog.Info("Faied to parse ABI", err)
		}
		// do not store invalid ABI
		contract.ABI = ""
		contract.LastErrorDate = eventTime
	}
	if glog.V(1) {
		glog.Infof("Created new contract %s Symbol %s methods=%d events=%d", address, contract.Symbol, len(contract.Methods), len(contract.Events))
	}

	// store new contracts to db in batches
	contractCache.created[address] = contract
	if len(contractCache.created) >= 200 {
		//fmt.Println("Save new contracts", len(contractCache.contracts), len(contractCache.created))
		if err := redshift.StoreContracts(contractCache.created); err != nil {
			// return error if failed to save the batch
			glog.Errorf("Failed to save %d contracts: %v", len(contractCache.created), err)
			return nil, errors.Wrapf(err, "Failed to save %d contracts", len(contractCache.created))
		}
		if glog.V(1) {
			glog.Infof("Saved %d contracts", len(contractCache.created))
		}
		contractCache.created = make(map[string]*common.Contract)
	}
	return contract, nil
}

func parseABI(c *common.Contract) error {
	if len(c.ABI) == 0 {
		return errors.Errorf("No ABI in contract %s", c.Address)
	}

	ab, err := safeNewABI(c.ABI)
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

// decode transaction input of a specified contract.
// returns decoded result if decode is successful, or nil otherwise
// returns fatal error if failed to connect to etherscan or database for the operation
func DecodeTransactionInput(input []byte, address string, blockTime int64) (*DecodedData, error) {
	var contract *common.Contract
	methodID := hex.EncodeToString(input[:4])
	method, ok := contractCache.stdMethods[methodID]
	if !ok {
		// find contract method
		var err error
		contract, err = getContract(address, blockTime)
		if err != nil {
			return nil, err
		}
		if len(contract.Methods) == 0 {
			if glog.V(1) {
				glog.Infof("Contract 0x%s contains no method %s", address, methodID)
			}
			setContractErrorTime(contract, blockTime)
			return nil, nil
		}
		if method, ok = contract.Methods[methodID]; !ok {
			if glog.V(1) {
				glog.Warningf("Contract 0x%s does not contain method %s", address, methodID)
			}
			setContractErrorTime(contract, blockTime)
			return nil, nil
		}
	}

	if glog.V(2) {
		glog.Infof("decode contract %s method %s tx data %s", address, methodID, hex.EncodeToString(input))
		glog.Flush()
	}
	if len(input) <= 4 {
		if glog.V(1) {
			glog.Warningf("Transaction contains no input data for contract %s method %s", address, methodID)
		}
		return &DecodedData{
			Name:   method.Name,
			ID:     methodID,
			Params: []*common.NamedValue{},
		}, nil
	}
	data, err := safeAbiDecode(method.Inputs, input[4:])
	if err != nil {
		glog.Errorf("Failed to decode transaction for contract %s method %s: %v", address, methodID, err)
		if contract != nil {
			setContractErrorTime(contract, blockTime)
		}
		return nil, nil
	}
	dmap, ok := data.(map[string]interface{})
	if !ok {
		glog.Errorf("Decoded transaction data %T is not a map", data)
		return nil, nil
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
	if contract != nil {
		setContractEventTime(contract, blockTime)
	}
	return dec, nil
}

// catch panic from abi.NewABI(a)
func safeNewABI(a string) (ab *abi.ABI, err error) {
	defer func() {
		if r := recover(); r != nil {
			ab = nil
			err = r.(error)
		}
	}()
	return abi.NewABI(a)
}

// catch panic from abi decoder
func safeAbiDecode(t *abi.Type, input []byte) (data interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			data = nil
			err = r.(error)
		}
	}()
	return abi.Decode(t, input)
}

// decode event data of a contract.
// returns decoded result if decode is successful, or nil otherwise.
// returns fatal error if failed to connect to etherscan or database for the operation
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
		contract, err := getContract(addr, blockTime)
		if err != nil {
			return nil, err
		}
		if len(contract.Events) == 0 {
			setContractErrorTime(contract, blockTime)
			if glog.V(1) {
				glog.Infof("Contract 0x%s contains no event %s", addr, eventID)
			}
			return nil, nil
		}
		if event, ok = contract.Events[eventID]; !ok {
			setContractErrorTime(contract, blockTime)
			if glog.V(1) {
				glog.Warningf("Contract 0x%s does not contain event %s", addr, eventID)
			}
			return nil, nil
		}
		if data, err = event.ParseLog(wlog); err != nil {
			setContractErrorTime(contract, blockTime)
			if glog.V(1) {
				glog.Warningf("Failed to decode event for contract %s event %s: %v", addr, eventID, err)
			}
			return &DecodedData{
				Name:   event.Name,
				ID:     eventID,
				Params: []*common.NamedValue{},
			}, nil
		}
		setContractEventTime(contract, blockTime)
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
