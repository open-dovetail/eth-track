package main

import (
	"flag"
	"os"
	"time"

	"github.com/golang/glog"

	"github.com/pkg/errors"

	"github.com/open-dovetail/eth-track/common"
	"github.com/open-dovetail/eth-track/proc"
	"github.com/open-dovetail/eth-track/store"
)

type Config struct {
	nodeURL        string // Ethereum node URL
	apiKey         string // etherscan API key
	etherscanDelay int    // delay of consecutive etherscan API invocation in ms
	blockDelay     int    // blockchain height delay for last confirmed block
	blockBatchSize int    // number of blocks to process per db commit
	dbURL          string // clickhouse URL
	dbName         string // clickhouse database name
	dbUser         string // clickhouse user name
	dbPassword     string // clickhouse user password
}

var config = &Config{}

// Initial values of the command-line args
func init() {
	flag.StringVar(&config.nodeURL, "nodeURL", "http://localhost:8545", "Ethereum node URL")
	flag.StringVar(&config.apiKey, "apiKey", "", "Etherscan API key")
	flag.IntVar(&config.etherscanDelay, "etherscanDelay", 350, "delay in millis between etherscan API calls")
	flag.IntVar(&config.blockDelay, "blockDelay", 12, "blockchain height delay for last confirmed block")
	flag.IntVar(&config.blockBatchSize, "blockBatchSize", 10, "number of blocks to process per db commit")
	flag.StringVar(&config.dbURL, "dbURL", "http://127.0.0.1:8123", "Etherscan API key")
	flag.StringVar(&config.dbName, "dbName", "ethdb", "Etherscan API key")
	flag.StringVar(&config.dbUser, "dbUser", "default", "Etherscan API key")
	flag.StringVar(&config.dbPassword, "dbPassword", "clickhouse", "Etherscan API key")
}

// check env variables, which overrides the commandline input
func envOverride() {
	if v, ok := os.LookupEnv("ETHEREUM_URL"); ok && v != "" {
		config.nodeURL = v
	}
	if v, ok := os.LookupEnv("ETHERSCAN_APIKEY"); ok && v != "" {
		config.apiKey = v
	}
	if v, ok := os.LookupEnv("CLICKHOUSE_URL"); ok && v != "" {
		config.dbURL = v
	}
	if v, ok := os.LookupEnv("CLICKHOUSE_DB"); ok && v != "" {
		config.dbName = v
	}
	if v, ok := os.LookupEnv("CLICKHOUSE_USER"); ok && v != "" {
		config.dbUser = v
	}
	if v, ok := os.LookupEnv("CLICKHOUSE_PASSWORD"); ok && v != "" {
		config.dbPassword = v
	}

	// Google log setting
	if v, ok := os.LookupEnv("GLOG_logtostderr"); ok && v != "" {
		if v == "false" || v == "0" {
			flag.Lookup("logtostderr").Value.Set("false")
		} else {
			flag.Lookup("logtostderr").Value.Set("true")
		}
	}

	if flag.Lookup("logtostderr").Value.String() != "true" {
		// Set folder for log files
		if flag.Lookup("log_dir").Value.String() == "" {
			flag.Lookup("log_dir").Value.Set("./log")
		}
		if err := os.MkdirAll(flag.Lookup("log_dir").Value.String(), 0777); err != nil {
			glog.Errorf("Failed to create log folder %s: %+v\n", flag.Lookup("log_dir").Value.String(), err)
			flag.Lookup("logtostderr").Value.Set("true")
		}
	}

	// default clickhouse config
	if len(config.dbName) == 0 {
		config.dbName = "default"
	}
	if len(config.dbUser) == 0 {
		config.dbUser = "default"
	}
}

// initialize connections
func connect() error {
	// override config with env vars
	envOverride()

	// initialize ethereum node client
	if _, err := proc.NewEthereumClient(config.nodeURL); err != nil {
		return errors.Wrapf(err, "Failed to connect to ethereum node %s", config.nodeURL)
	}

	// initialize etherscan api connection
	proc.NewEtherscanAPI(config.apiKey, config.etherscanDelay)
	dai := "0x6b175474e89094c44da98b954eedeac495271d0f"
	if _, err := proc.GetEtherscanAPI().FetchABI(dai); err != nil {
		return errors.Wrapf(err, "Failed to invoke etherscan API with key %s", config.apiKey)
	}

	// initialize clickhouse db connection
	params := make(map[string]string)
	if config.dbUser != "default" {
		params["user"] = config.dbUser
	}
	if len(config.dbPassword) > 0 {
		params["password"] = config.dbPassword
	}
	if glog.V(2) {
		params["debug"] = "1"
	}
	if _, err := store.NewClickHouseConnection(config.dbURL, config.dbName, params); err != nil {
		return errors.Wrapf(err, "Failed to connect to clickhouse db at %s/%s", config.dbURL, config.dbName)
	}
	return nil
}

// Turn on verbose logging using option -v 2
// Log to stderr using option -logtostderr or set env GLOG_logtostderr=true
// or log to specified folder using option -log_dir="mylogdir"
func main() {
	// parse command-line args
	flag.Parse()

	// initialize connections
	if err := connect(); err != nil {
		glog.Fatalf("Failed initialization: %+v", err)
	}

	startTime := time.Now().Unix()
	var lastBlock *common.Block
	var err error
	for i := 0; i < 10; i++ {
		loopStart := time.Now().Unix()
		lastBlock, err = decodeBlocks(lastBlock, config.blockBatchSize)
		if err != nil {
			glog.Fatalf("Failed block batch %+v", err)
		}
		loopEnd := time.Now().Unix()
		glog.Infof("[%d] Block %d - Loop elapsed: %ds; Total elapsed: %ds", i, lastBlock.Number, (loopEnd - loopStart), (loopEnd - startTime))
	}

	if lastBlock != nil {
		glog.Infof("Last block %d", lastBlock.Number)
	}

	if err := store.GetDBConnection().Close(); err != nil {
		glog.Errorf("Failed to close db connection: %4", err.Error())
	}
}

func decodeBlocks(startBlock *common.Block, batchSize int) (lastBlock *common.Block, err error) {
	if startBlock == nil {
		block, err := proc.LastConfirmedBlock(config.blockDelay)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to retrieve last confirmed block")
		}
		// decode the last confirmed block
		if lastBlock, err = proc.DecodeBlock(block); err != nil {
			return lastBlock, err
		}
	} else {
		lastBlock = startBlock
	}

	// decode batch of parent blocks
	for i := 0; i < batchSize; i++ {
		if lastBlock, err = proc.DecodeBlockByHash(lastBlock.ParentHash); err != nil {
			return lastBlock, errors.Wrapf(err, "Failed to retrieve parent block")
		}
	}

	if err := store.MustGetDBTx().CommitTx(); err != nil {
		glog.Errorf("Failed to commit db transaction: %s", err.Error())
	}
	return
}
