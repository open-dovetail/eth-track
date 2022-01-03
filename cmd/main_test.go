package main

// Run all unit test: `go test -v`

import (
	"fmt"
	"os"
	"testing"

	"github.com/open-dovetail/eth-track/store"
	"github.com/pkg/errors"
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
	if _, err := store.NewClickHouseConnection(dbURL, dbName, params); err != nil {
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

func TestStatusInterval(t *testing.T) {
	intervals, progress := txStatusIntervals()
	fmt.Println("progress:", progress)
	fmt.Printf("intervals: %d\n", len(intervals))
	for i, v := range intervals {
		fmt.Printf("[%d] %s %s\n", i, v.startTime, v.endTime)
	}
}
