package proc

import (
	"strings"

	"github.com/golang/glog"
	"github.com/open-dovetail/eth-track/common"
	web3 "github.com/umbracle/ethgo"
)

// decode event log
// returns decoded event-log if decode is successful, or raw event data otherwise
// returns fatal error if failed to connect to etherscan or database
func DecodeEventLog(wlog *web3.Log, blockTime int64) (*common.EventLog, error) {
	if glog.V(2) {
		glog.Infoln("Log:", wlog.LogIndex, len(wlog.Topics), wlog.Address.String())
	}
	result := &common.EventLog{
		BlockNumber: wlog.BlockNumber,
		LogIndex:    wlog.LogIndex,
		Removed:     wlog.Removed,
		TxnIndex:    wlog.TransactionIndex,
		TxnHash:     wlog.TransactionHash.String(),
		Address:     strings.ToLower(wlog.Address.String()),
		Data:        wlog.Data,
		BlockTime:   blockTime,
	}
	// decode only if event topics exist
	if len(wlog.Topics) < 1 {
		glog.Warningf("Event log %d: %s No topics for contract %s", wlog.LogIndex, wlog.TransactionHash.String(), result.Address)
		return result, nil
	}

	data, err := DecodeEventData(wlog, blockTime)
	if err != nil {
		// fatal error
		return result, err
	}
	if data != nil {
		// data decoded successfully
		result.Event = data.Name
		result.Params = data.Params
	} else {
		// failed to decode event data
		result.Event = "UNKNOWN"
	}
	if glog.V(1) {
		glog.Infof("Event log %d: %s Event %s", wlog.LogIndex, wlog.TransactionHash.String(), result.Event)
	}
	return result, nil
}
