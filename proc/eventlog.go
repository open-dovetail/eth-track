package proc

import (
	"encoding/hex"
	"encoding/json"
	"fmt"

	web3 "github.com/umbracle/go-web3"
)

type EventLog struct {
	BlockNumber  uint64
	LogIndex     uint64
	Removed      bool
	TxnIndex     uint64
	TxnHash      string
	ContractAddr string
	Event        string
	Params       []*NamedValue
	BlockTime    int64
}

func DecodeEventLog(wlog *web3.Log) *EventLog {
	fmt.Println("Log:", wlog.LogIndex, len(wlog.Topics), wlog.Topics[0].String(), wlog.Address.String(), hex.EncodeToString(wlog.Data))
	result := &EventLog{
		BlockNumber:  wlog.BlockNumber,
		LogIndex:     wlog.LogIndex,
		Removed:      wlog.Removed,
		TxnIndex:     wlog.TransactionIndex,
		TxnHash:      wlog.TransactionHash.String(),
		ContractAddr: wlog.Address.String(),
	}
	// decode only if event topics exist
	if len(wlog.Topics) < 1 {
		fmt.Printf("Event log %d: %s has no topics\n", wlog.LogIndex, wlog.TransactionHash.String())
		return result
	}

	if data, err := DecodeEventData(wlog); err == nil {
		result.Event = data.Name
		result.Params = data.Params
	} else {
		fmt.Printf("Event log %d: %s Failed decode %+v\n", wlog.LogIndex, wlog.TransactionHash.String(), err)
		result.Event = "UNKNOWN"
	}
	fmt.Printf("Event log %d: %s Event %s\n", wlog.LogIndex, wlog.TransactionHash.String(), result.Event)
	if result.Params != nil {
		for _, v := range result.Params {
			value := v.Value
			if v.Kind.String() != "Bytes" {
				// replace all []uint8 fields using hex encoding
				value = HexEncodeUint8Array(v.Value)
			}
			if p, err := json.Marshal(value); err == nil {
				fmt.Printf("Input %s %s %T %s\n", v.Name, v.Kind.String(), v.Value, string(p))
			}
		}
	}
	return result
}
