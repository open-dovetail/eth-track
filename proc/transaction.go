package proc

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"

	web3 "github.com/umbracle/go-web3"
)

type Transaction struct {
	Hash        string
	BlockNumber uint64
	TxnIndex    uint64
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
		From:        tx.From.String(),
		To:          tx.To.String(),
		GasPrice:    tx.GasPrice,
		Gas:         tx.Gas,
		Value:       tx.Value,
		Nonce:       tx.Nonce,
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

// Return false if transaction failed, true if succeeded
func GetTransactionStatus(txHash string) (bool, error) {
	var data map[string]interface{}
	if err := GetConfig().GetClient().Call("eth_getTransactionReceipt", &data, txHash); err != nil {
		return false, err
	}
	return data["status"] == "0x1", nil
}

// replace all []uint8 with hex encoding in the input data
func HexEncodeUint8Array(data interface{}) interface{} {
	if reflect.TypeOf(data) == reflect.TypeOf(web3.Address{}) {
		// do not re-encode for address
		return data
	}
	ref := reflect.ValueOf(data)
	switch ref.Kind() {
	case reflect.Map:
		result := make(map[string]interface{})
		for k, v := range data.(map[string]interface{}) {
			result[k] = HexEncodeUint8Array(v)
		}
		return result
	case reflect.Array, reflect.Slice:
		if ref.Len() > 0 {
			if ref.Index(0).Kind() == reflect.Uint8 {
				// convert array to slice for hex encoding
				b := make([]uint8, ref.Len(), ref.Len())
				for i := 0; i < ref.Len(); i++ {
					b[i] = uint8(ref.Index(i).Uint())
				}
				return "0x" + hex.EncodeToString(b)
			} else {
				result := make([]interface{}, ref.Len(), ref.Len())
				for i := 0; i < ref.Len(); i++ {
					result[i] = HexEncodeUint8Array(ref.Index(i).Interface())
				}
				return result
			}
		}
	}
	return data
}
