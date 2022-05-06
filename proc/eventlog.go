package proc

import (
	"github.com/golang/glog"
	"github.com/open-dovetail/eth-track/common"
	web3 "github.com/umbracle/ethgo"
)

func DecodeEventLog(wlog *web3.Log, blockTime int64) *common.EventLog {
	if glog.V(2) {
		glog.Infoln("Log:", wlog.LogIndex, len(wlog.Topics), wlog.Address.String())
	}
	result := &common.EventLog{
		BlockNumber: wlog.BlockNumber,
		LogIndex:    wlog.LogIndex,
		Removed:     wlog.Removed,
		TxnIndex:    wlog.TransactionIndex,
		TxnHash:     wlog.TransactionHash.String(),
		Address:     wlog.Address.String(),
		Data:        wlog.Data,
		BlockTime:   blockTime,
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
		if glog.V(1) {
			glog.Warningf("Event log %d: %s Failed decode - %s", wlog.LogIndex, wlog.TransactionHash.String(), err.Error())
		}
		result.Event = "UNKNOWN"
	}
	if glog.V(1) {
		glog.Infof("Event log %d: %s Event %s", wlog.LogIndex, wlog.TransactionHash.String(), result.Event)
	}

	return result
}
