package proc

import (
	"fmt"
	"math/big"

	web3 "github.com/umbracle/go-web3"
)

type Transaction struct {
	Hash        string
	BlockNumber uint64
	TxnIndex    uint64
	Status      bool
	From        string
	To          string
	Method      string
	Params      []*NamedValue
	GasPrice    uint64
	Gas         uint64
	Value       *big.Int
	Nonce       uint64
	BlockTime   int64
}

func DecodeTransaction(tx *web3.Transaction) *Transaction {
	// fmt.Println("Transaction:", tx.TxnIndex, tx.From.String(), tx.To.String(), tx.Value, tx.Hash.String(), hex.EncodeToString(tx.Input))
	result := &Transaction{
		Hash:        tx.Hash.String(),
		BlockNumber: tx.BlockNumber,
		TxnIndex:    tx.TxnIndex,
		Status:      true,
		From:        tx.From.String(),
		GasPrice:    tx.GasPrice,
		Gas:         tx.Gas,
		Value:       tx.Value,
		Nonce:       tx.Nonce,
	}
	if tx.To != nil {
		result.To = tx.To.String()
	}

	// decode only if method is specified in the input data
	if len(tx.Input) < 4 {
		fmt.Printf("Transaction %d: %s No decode\n", tx.TxnIndex, tx.Hash.String())
		return result
	}

	if data, err := DecodeTransactionInput(tx.Input, tx.To.String()); err == nil {
		result.Method = data.Name
		result.Params = data.Params
	} else {
		fmt.Printf("Transaction %d: %s Failed decode %+v\n", tx.TxnIndex, tx.Hash.String(), err)
		result.Method = "UNKNOWN"
	}
	fmt.Printf("Transaction %d: %s Method %s\n", tx.TxnIndex, tx.Hash.String(), result.Method)
	return result
}

// Return false if transaction failed, true if succeeded
func GetTransactionStatus(txHash string) (bool, error) {
	var data map[string]interface{}
	if err := GetConfig().GetClient().Call("eth_getTransactionReceipt", &data, txHash); err != nil {
		return false, err
	}
	return data["status"] == "0x1", nil
}
