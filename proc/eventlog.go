package proc

import (
	"encoding/hex"

	"github.com/golang/glog"
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

func DecodeEventLog(wlog *web3.Log, blockTime int64) *EventLog {
	if glog.V(2) {
		glog.Infoln("Log:", wlog.LogIndex, len(wlog.Topics), wlog.Topics[0].String(), wlog.Address.String(), hex.EncodeToString(wlog.Data))
	}
	result := &EventLog{
		BlockNumber:  wlog.BlockNumber,
		LogIndex:     wlog.LogIndex,
		Removed:      wlog.Removed,
		TxnIndex:     wlog.TransactionIndex,
		TxnHash:      wlog.TransactionHash.String(),
		ContractAddr: wlog.Address.String(),
		BlockTime:    blockTime,
	}
	// decode only if event topics exist
	if len(wlog.Topics) < 1 {
		glog.Warningf("Event log %d: %s No topics for contract %s", wlog.LogIndex, wlog.TransactionHash.String(), wlog.Address.String())
		return result
	}

	if data, err := DecodeEventData(wlog, blockTime); err == nil {
		result.Event = data.Name
		result.Params = data.Params
	} else {
		glog.Warningf("Event log %d: %s Failed decode - %s", wlog.LogIndex, wlog.TransactionHash.String(), err.Error())
		result.Event = "UNKNOWN"
	}
	glog.Infof("Event log %d: %s Event %s", wlog.LogIndex, wlog.TransactionHash.String(), result.Event)

	return result
}
