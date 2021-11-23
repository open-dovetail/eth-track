package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"os"

	"github.com/open-dovetail/eth-track/proc"
)

func main() {
	url, ok := os.LookupEnv("ETHEREUM_URL")
	if !ok {
		log.Fatalln("ETHEREUM_URL env must be defined")
	}
	fmt.Println("ETHEREUM_URL:", url)

	apiKey, ok := os.LookupEnv("ETHERSCAN_APIKEY")
	if !ok {
		log.Fatalln("ETHERSCAN_APIKEY env must be defined")
	}
	fmt.Println("ETHERSCAN_APIKEY:", apiKey)

	// initialize Ethereum connection
	etherscanDelay := 200 // control etherscan call rate at < 5/s
	if _, err := proc.NewConfig(url, apiKey, etherscanDelay); err != nil {
		log.Fatalf("Failed to connect to Ethereum: %+v", err)
	}

	// get contract defs
	dai := "0x6b175474e89094c44da98b954eedeac495271d0f"
	c, err := proc.NewContract(dai)
	if err != nil {
		panic(err)
	}
	fmt.Println("DAI token properties:", c.Name, c.Symbol, c.Decimals, c.TotalSupply, len(c.Methods), (c.Events))
	for i, mth := range c.Methods {
		fmt.Println("Method:", i, mth.Name, hex.EncodeToString(mth.ID()), mth.Inputs.TupleElems(), mth.Inputs.Kind())
	}

	// get last confirmed block, assume confirmed at 12 height before last known block
	lastBlock, err := proc.LastConfirmedBlock(12)
	if err != nil {
		panic(err)
	}
	fmt.Println("Last block number:", lastBlock)

	block, err := proc.DecodeBlockByNumber(lastBlock)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Block %d: %s @ %d; Transactions: %d; Events: %d\n", block.Number, block.Hash, block.BlockTime, len(block.Transactions), len(block.EventLogs))

	// get latest code of a contract -- go-web3/jsonrpc/eth.go does not work if block is not known
	// so use direct client call -- this call returns only binary, not source code, so not useful
	// var res string
	// if err := client.Call("eth_getCode", &res, "0x4fabb145d64652a948d72533023f6e7a623c7c53", "latest"); err != nil {
	// 	panic(err)
	// }
	// fmt.Println("Contract:", res)

	// fmt.Println(block)
}
