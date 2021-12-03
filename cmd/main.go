package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/mailru/dbr"
	clickhouse "github.com/mailru/go-clickhouse"

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

	// testClickHouse()
	// testDBR()
	testDecode()
}

func testDecode() {
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

func testClickHouse() {
	connect, err := sql.Open("clickhouse", "http://127.0.0.1:8123/default?password=clickhouse&debug=1")
	if err != nil {
		log.Fatal(err)
	}
	if err := connect.Ping(); err != nil {
		log.Fatal(err)
	}

	_, err = connect.Exec(`
		CREATE TABLE IF NOT EXISTS example (
			country_code FixedString(2),
			os_id        UInt8,
			browser_id   UInt8,
			categories   Array(Int16),
			action_day   Date,
			action_time  DateTime
		) engine=Memory`)

	if err != nil {
		log.Fatal(err)
	}

	tx, err := connect.Begin()
	if err != nil {
		log.Fatal(err)
	}
	tx.Prepare(`set profile='async_insert'`)
	stmt, err := tx.Prepare(`
		INSERT INTO example (
			country_code,
			os_id,
			browser_id,
			categories,
			action_day,
			action_time
		) VALUES (
			?, ?, ?, ?, ?, ?
		)`)

	if err != nil {
		log.Fatal(err)
	}

	for i := 100; i < 200; i++ {
		if _, err := stmt.Exec(
			"RU",
			10+i,
			100+i,
			clickhouse.Array([]int16{1, 2, 3}),
			clickhouse.Date(time.Now()),
			time.Now(),
		); err != nil {
			log.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	rows, err := connect.Query(`
			SELECT
				country_code,
				os_id,
				browser_id,
				categories,
				action_day,
				action_time
			FROM
				example`)

	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var (
			country               string
			os, browser           uint8
			categories            []int16
			actionDay, actionTime time.Time
		)
		if err := rows.Scan(
			&country,
			&os,
			&browser,
			&categories,
			&actionDay,
			&actionTime,
		); err != nil {
			log.Fatal(err)
		}
		log.Printf("country: %s, os: %d, browser: %d, categories: %v, action_day: %s, action_time: %s",
			country, os, browser, categories, actionDay, actionTime,
		)
	}

	ctx := context.Background()
	rows, err = connect.QueryContext(context.WithValue(ctx, clickhouse.QueryID, "dummy-query-id"), `
			SELECT
				country_code,
				os_id,
				browser_id,
				categories,
				action_day,
				action_time
			FROM
				example`)

	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var (
			country               string
			os, browser           uint8
			categories            []int16
			actionDay, actionTime time.Time
		)
		if err := rows.Scan(
			&country,
			&os,
			&browser,
			&categories,
			&actionDay,
			&actionTime,
		); err != nil {
			log.Fatal(err)
		}
		log.Printf("country: %s, os: %d, browser: %d, categories: %v, action_day: %s, action_time: %s",
			country, os, browser, categories, actionDay, actionTime,
		)
	}
}

func testDBR() {
	connect, err := dbr.Open("clickhouse", "http://127.0.0.1:8123/default?password=clickhouse", nil)
	if err != nil {
		log.Fatal(err)
	}
	var items []struct {
		CountryCode string    `db:"country_code"`
		OsID        uint8     `db:"os_id"`
		BrowserID   uint8     `db:"browser_id"`
		Categories  []int16   `db:"categories"`
		ActionTime  time.Time `db:"action_time"`
	}
	sess := connect.NewSession(nil)
	query := sess.Select("country_code", "os_id", "browser_id", "categories", "action_time").From("example")
	query.Where(dbr.Eq("country_code", "RU"))
	if _, err := query.Load(&items); err != nil {
		log.Fatal(err)
	}

	for _, item := range items {
		log.Printf("country: %s, os: %d, browser: %d, categories: %v, action_time: %s",
			item.CountryCode, item.OsID, item.BrowserID, item.Categories, item.ActionTime,
		)
	}
}
