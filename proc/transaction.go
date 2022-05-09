package proc

import (
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/open-dovetail/eth-track/common"
	"github.com/pkg/errors"
	web3 "github.com/umbracle/ethgo"
)

// decode transaction input data
// returns decoded transaction if decode is successful, or raw input data otherwise
// returns fatal error if failed to connect to etherscan or database
func DecodeTransaction(tx *web3.Transaction, blockTime int64) (*common.Transaction, error) {
	if glog.V(2) {
		glog.Infoln("Decode transaction:", tx.BlockNumber, tx.TxnIndex, tx.From.String(), tx.Value, tx.Hash.String())
	}
	result := &common.Transaction{
		Hash:        tx.Hash.String(),
		BlockNumber: tx.BlockNumber,
		TxnIndex:    tx.TxnIndex,
		Status:      true,
		From:        strings.ToLower(tx.From.String()),
		Input:       tx.Input,
		GasPrice:    tx.GasPrice,
		Gas:         tx.Gas,
		Value:       tx.Value,
		Nonce:       tx.Nonce,
		BlockTime:   blockTime,
	}
	if tx.To != nil {
		result.To = strings.ToLower(tx.To.String())
	}

	// decode only if method is specified in the input data
	if len(tx.Input) < 4 {
		if glog.V(1) {
			glog.Infof("Transaction %d: %s No data to decode", tx.TxnIndex, tx.Hash.String())
		}
		return result, nil
	}

	if tx.To == nil {
		glog.Warningf("Transaction %d: %s No contract address", tx.TxnIndex, tx.Hash.String())
		return result, nil
	}

	data, err := DecodeTransactionInput(tx.Input, result.To, blockTime)
	if err != nil {
		// fatal error
		return result, err
	}
	if data != nil {
		// data decoded successfully
		result.Method = data.Name
		result.Params = data.Params
	} else {
		// failed to decode data
		result.Method = "UNKNOWN"
	}
	if glog.V(1) {
		glog.Infof("Transaction %d: %s Method %s", tx.TxnIndex, tx.Hash.String(), result.Method)
	}
	return result, nil
}

// Return false if transaction failed, true if succeeded
func GetTransactionStatus(txHash string) (bool, error) {
	for retry := 1; retry <= 3; retry++ {
		var data map[string]interface{}
		if err := GetEthereumClient().Call("eth_getTransactionReceipt", &data, txHash); err == nil {
			return data["status"] == "0x1", nil
		} else {
			// Ethereum call failed, wait and retry
			glog.Warningf("Failed %d times to get status for TxHash %s: %+v", retry, txHash, err)
			time.Sleep(10 * time.Second)
		}
	}
	return false, errors.Errorf("Failed to retrieve status for transaction %s", txHash)
}
