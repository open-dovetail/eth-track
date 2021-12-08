package main

import (
	"flag"
	"os"

	"github.com/golang/glog"

	"github.com/pkg/errors"

	"github.com/open-dovetail/eth-track/proc"
	"github.com/open-dovetail/eth-track/store"
)

type Config struct {
	nodeURL        string // Ethereum node URL
	apiKey         string // etherscan API key
	etherscanDelay int    // delay of consecutive etherscan API invocation in ms
	blockDelay     int    // blockchain height delay for last confirmed block
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

	testDecode()
}

func testDecode() {

	// get last confirmed block, assume confirmed at 12 height before last known block
	lastBlock, err := proc.LastConfirmedBlock(config.blockDelay)
	if err != nil {
		glog.Fatalf("Failed to retrieve last confirmed block: %+v", err)
	}
	glog.Infof("Last block number: %d", lastBlock)

	// lastBlock = 13742419
	block, err := proc.DecodeBlockByNumber(lastBlock)
	if err != nil {
		glog.Fatalf("Failed to decode block %d: %+v", lastBlock, err)
	}
	glog.Infof("Block %d: %s @ %d; Transactions: %d; Events: %d", block.Number, block.Hash, block.BlockTime, len(block.Transactions), len(block.EventLogs))
}
