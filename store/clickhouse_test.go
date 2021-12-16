package store

// Run all unit test: `go test -v`

import (
	"fmt"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/open-dovetail/eth-track/common"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setup() error {
	// initialize clickhouse db connection
	dbURL, ok := os.LookupEnv("CLICKHOUSE_URL")
	if !ok {
		dbURL = "http://127.0.0.1:8123"
	}
	dbName, ok := os.LookupEnv("CLICKHOUSE_DB")
	if !ok {
		dbName = "ethdb"
	}
	dbUser, ok := os.LookupEnv("CLICKHOUSE_USER")
	if !ok {
		dbUser = "default"
	}
	dbPassword, ok := os.LookupEnv("CLICKHOUSE_PASSWORD")
	if !ok {
		dbPassword = "clickhouse"
	}

	params := make(map[string]string)
	if dbUser != "default" {
		params["user"] = dbUser
	}
	if len(dbPassword) > 0 {
		params["password"] = dbPassword
	}
	// params["debug"] = "1"
	if _, err := NewClickHouseConnection(dbURL, dbName, params); err != nil {
		return errors.Wrapf(err, "Failed to connect to clickhouse db at %s/%s", dbURL, dbName)
	}
	return nil
}

func TestMain(m *testing.M) {
	if err := setup(); err != nil {
		fmt.Printf("FAILED %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Setup successful")
	status := m.Run()
	os.Exit(status)
}

func TestContractStore(t *testing.T) {
	// test bigInt to float64 conversion
	bint, ok := new(big.Int).SetString("8933985513688138313511470486", 10)
	require.True(t, ok, "big int should be created successfully")

	// convert big int to float64
	f := bigIntToFloat(bint)
	require.Greater(t, f, 1e27, "converted float value should be greater than 1e27")

	// convert back to big int
	i := floatToBigInt(f)
	// fmt.Println("int", bint.String(), "float", f, "converted", i.String())
	assert.Equal(t, bint.String()[:15], i.String()[:15], "converted number does not match first 15 digits")

	address := "0x6b175474e89094c44da98b954eedeac495271d0f"
	abiCode := `[{"inputs":[{"internalType":"uint256","name":"chainId_","type":"uint256"}],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"src","type":"address"},{"indexed":true,"internalType":"address","name":"guy","type":"address"},{"indexed":false,"internalType":"uint256","name":"wad","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":true,"inputs":[{"indexed":true,"internalType":"bytes4","name":"sig","type":"bytes4"},{"indexed":true,"internalType":"address","name":"usr","type":"address"},{"indexed":true,"internalType":"bytes32","name":"arg1","type":"bytes32"},{"indexed":true,"internalType":"bytes32","name":"arg2","type":"bytes32"},{"indexed":false,"internalType":"bytes","name":"data","type":"bytes"}],"name":"LogNote","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"src","type":"address"},{"indexed":true,"internalType":"address","name":"dst","type":"address"},{"indexed":false,"internalType":"uint256","name":"wad","type":"uint256"}],"name":"Transfer","type":"event"},{"constant":true,"inputs":[],"name":"DOMAIN_SEPARATOR","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"PERMIT_TYPEHASH","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"usr","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"usr","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"burn","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"guy","type":"address"}],"name":"deny","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"usr","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"mint","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"src","type":"address"},{"internalType":"address","name":"dst","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"move","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"nonces","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"holder","type":"address"},{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"nonce","type":"uint256"},{"internalType":"uint256","name":"expiry","type":"uint256"},{"internalType":"bool","name":"allowed","type":"bool"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"permit","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"usr","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"pull","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"usr","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"push","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"guy","type":"address"}],"name":"rely","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"dst","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"src","type":"address"},{"internalType":"address","name":"dst","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"version","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"wards","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"}]`

	// store a sample contract
	contract := &common.Contract{
		Address:        address,
		Name:           "Dai Stablecoin",
		Symbol:         "DAI",
		Decimals:       18,
		TotalSupply:    bint,
		UpdatedTime:    1638850281,
		StartEventTime: 1638850281,
		LastEventTime:  0,
		LastErrorTime:  0,
		ABI:            abiCode,
	}
	tx, err := GetDBTx()
	require.NoError(t, err, "Start DB Tx should not throw exception")
	assert.NotNil(t, tx, "DB Tx should not be nil")

	err = tx.InsertContract(contract)
	assert.NoError(t, err, "Insert contract should not throw exception")

	err = tx.CommitTx()
	assert.NoError(t, err, "Commit Tx should not through exception")

	// query the contract
	c, err := QueryContract(address)
	assert.NoError(t, err, "query contract should not throw exception")
	assert.NotNil(t, c, "query result should not be empty")

	assert.Equal(t, address, c.Address, "query result does not match address")
	assert.Equal(t, "Dai Stablecoin", c.Name, "query result does not match name")
	assert.Equal(t, "DAI", c.Symbol, "query result does not match symbol")
	assert.Equal(t, uint8(18), c.Decimals, "query result does not match decimals")
	assert.Equal(t, bint.String()[:15], c.TotalSupply.String()[:15], "query result does not match first 15 digits of totalSupply")
	// check UTC date conversion
	utcTime := secondsToDateTime(1638850281)
	assert.Equal(t, timeToDate(utcTime), c.UpdatedTime, "query result does not match updatedTime")
	assert.NotEmpty(t, c.ABI, "query result ABI should not be empty")
}

func TestProgressStore(t *testing.T) {
	// setup test data
	hit, _ := time.ParseInLocation("2006-01-02 15:04:05", "2021-12-11 07:37:04", time.UTC)
	lowt, _ := time.ParseInLocation("2006-01-02 15:04:05", "2021-12-10 16:13:33", time.UTC)

	progress := &common.Progress{
		ProcessID:    common.AddTransaction,
		HiBlock:      13782538,
		LowBlock:     13778418,
		HiBlockTime:  hit.Unix(),
		LowBlockTime: lowt.Unix(),
	}

	tx, err := GetDBTx()
	require.NoError(t, err, "Start DB Tx should not throw exception")
	assert.NotNil(t, tx, "DB Tx should not be nil")

	err = tx.InsertProgress(progress)
	require.NoError(t, err, "Insert progress should not throw exception")

	err = tx.CommitTx()
	assert.NoError(t, err, "Commit Tx should not through exception")

	// query the progress
	p, err := QueryProgress(common.AddTransaction, true)
	assert.NoError(t, err, "query progress should not throw exception")
	assert.NotNil(t, p, "query result should not be empty")

	assert.Equal(t, progress.HiBlock, p.HiBlock, "query result does not match high block")
	assert.Equal(t, progress.LowBlock, p.LowBlock, "query result does not match low block")
	assert.Equal(t, progress.HiBlockTime, p.HiBlockTime, "query result does not match high block time")
	assert.Equal(t, progress.LowBlockTime, p.LowBlockTime, "query result does not match low block time")
}

func TestBlockQuery(t *testing.T) {
	latestBlock, err := QueryBlock(0, true)
	assert.NoError(t, err, "query latest block should not throw exception")
	earliestBlock, err := QueryBlock(0, false)
	assert.NoError(t, err, "query earliest block should not throw exception")
	if earliestBlock != nil {
		block, err := QueryBlock(earliestBlock.Number, false)
		assert.NoError(t, err, "query non-existing block should not throw exception")
		assert.Nil(t, block, "should return nil if block does not exist")
		block, err = QueryBlock(earliestBlock.Number, true)
		assert.NoError(t, err, "next to earliest block should not throw exception")
		if block != nil {
			// fmt.Println("next to earliest block", block.Number, block.Hash, block.BlockTime, secondsToDateTime(block.BlockTime))
			assert.Equal(t, earliestBlock.Number+1, block.Number, "block number difference should be 1")
		}
	}
	if latestBlock != nil {
		assert.NotNil(t, earliestBlock, "earliest block should not be nil")
		// fmt.Println("earliest block", earliestBlock.Number, earliestBlock.Hash, earliestBlock.BlockTime, secondsToDateTime(earliestBlock.BlockTime))
		assert.GreaterOrEqual(t, latestBlock.Number, earliestBlock.Number, "latest block number should be greater than earliest block number")
		assert.GreaterOrEqual(t, latestBlock.BlockTime, earliestBlock.BlockTime, "latest block time should be greater than earliest block time")
		assert.Equal(t, 66, len(latestBlock.Hash), "block hash should be 66 characters long")
		assert.Equal(t, 66, len(earliestBlock.Hash), "block hash should be 66 characters long")
	}
}
