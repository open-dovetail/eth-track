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
	flag.IntVar(&config.blockBatchSize, "blockBatchSize", 40, "number of blocks to process per db commit")
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

	latest, _ := store.QueryBlock(0, true)
	fillBlockGap(latest)

	// process new blocks on chain
	var endBlock uint64
	if latest != nil {
		endBlock = latest.Number
	}
	if block, _ := decodeConfirmedBlock(endBlock); block != nil {
		lastBlock := batchLoop(block, latest, 100)
		if latest == nil {
			progress := &common.Progress{
				ProcessID:    common.AddTransaction,
				HiBlock:      block.Number,
				HiBlockTime:  block.BlockTime,
				LowBlock:     latest.Number,
				LowBlockTime: latest.BlockTime,
			}
			store.MustGetDBTx().InsertProgress(progress)

		} else if latest.Number+1 >= lastBlock.Number {
			earliest, _ := store.QueryBlock(0, false)
			progress := &common.Progress{
				ProcessID:    common.AddTransaction,
				HiBlock:      block.Number,
				HiBlockTime:  block.BlockTime,
				LowBlock:     earliest.Number,
				LowBlockTime: earliest.BlockTime,
			}
			store.MustGetDBTx().InsertProgress(progress)
		}
	}

	store.MustGetDBTx().CommitTx()
	if err := store.GetDBConnection().Close(); err != nil {
		glog.Errorf("Failed to close db connection: %4", err.Error())
	}
}

// process missing blocks between recorded top blocks and HiBlock in grogress table
func fillBlockGap(latest *common.Block) {
	if latest == nil {
		// blocks table is empty, so no gap
		return
	}
	progress, _ := store.QueryProgress(common.AddTransaction, true)
	if progress == nil {
		// nothing recorded in progress, so no gap
		return
	}
	if latest.Number <= progress.HiBlock+1 {
		// recorded blocks is lower than progress, so no gap
		return
	}

	// fill in possible gap, then update progress
	gap, _ := store.QueryBlock(progress.HiBlock, true)
	if gap.Number > progress.HiBlock+1 {
		hiBlock, _ := proc.GetBlockByNumber(progress.HiBlock)
		lastBlock := batchLoop(gap, hiBlock, 0)
		if lastBlock != nil && progress.HiBlock+1 >= lastBlock.Number {
			// gap filled, so update progress
			progress.HiBlock = latest.Number
			progress.HiBlockTime = latest.BlockTime
			store.MustGetDBTx().InsertProgress(progress)
		} else {
			// unexpected early exit of gap-filling loop
			glog.Fatalf("gap between %d and %d is not filled", progress.HiBlock, lastBlock.Number)
		}
	} else {
		// no gap, so update progress
		progress.HiBlock = latest.Number
		progress.HiBlockTime = latest.BlockTime
		store.MustGetDBTx().InsertProgress(progress)
	}
}

// process blocks from the parent of a start block backwards up to before the end block.
// if end block is not specified, exit when maxBatch loops are complete
func batchLoop(startBlock, endBlock *common.Block, maxBatch int) *common.Block {
	if startBlock == nil {
		// start block must be specified
		return nil
	}
	if endBlock != nil && endBlock.Number+1 >= startBlock.Number {
		// nothing to process before end block
		return nil
	}

	startTime := time.Now().Unix()
	lastBlock := startBlock
	var err error

	i := 0
	for {
		i++
		loopStart := time.Now().Unix()
		lastBlock, err = decodeBatch(lastBlock, endBlock, config.blockBatchSize)
		if err != nil {
			glog.Fatalf("Failed block batch %+v", err)
		}
		loopEnd := time.Now().Unix()
		glog.Infof("[%d] Block %d - Loop elapsed: %ds; Total elapsed: %ds", i, lastBlock.Number, (loopEnd - loopStart), (loopEnd - startTime))
		if endBlock == nil && i > maxBatch {
			// end block is not specified, so complete max number of batches
			break
		}
		if lastBlock == nil {
			// no block is processed, so quit here
			break
		}
		if endBlock != nil && (lastBlock.Number <= endBlock.Number+1 || lastBlock.ParentHash.String() == endBlock.Hash) {
			// reached highest blocks already in database, so quit here
			break
		}
	}
	glog.Flush()
	return lastBlock
}

// decode latest confirmed block on chain.
// do nothing if refBlock is later than the latest confirmed block.
func decodeConfirmedBlock(refBlock uint64) (lastBlock *common.Block, err error) {
	block, err := proc.LastConfirmedBlock(config.blockDelay)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to retrieve last confirmed block")
	}
	if block.Number <= refBlock {
		glog.Infof("confirmed block %d is earlier than ref block %d", block.Number, refBlock)
		return nil, nil
	}
	// decode the last confirmed block
	return proc.DecodeBlock(block)
}

func decodeBatch(startBlock, endBlock *common.Block, batchSize int) (lastBlock *common.Block, err error) {

	lastBlock = startBlock

	// decode batch of parent blocks
	i := 0
	for {
		i++
		if batchSize > 0 && i > batchSize {
			glog.Infof("loop reached max batch size %s", i)
			break
		}
		if endBlock != nil && (lastBlock.Number <= endBlock.Number+1 || lastBlock.ParentHash.String() == endBlock.Hash) {
			glog.Infof("loop reached block %d %s compared to end-block %d %s", lastBlock.Number, lastBlock.ParentHash.String(), endBlock.Number, endBlock.Hash)
			break
		}
		if lastBlock, err = proc.DecodeBlockByHash(lastBlock.ParentHash); err != nil {
			return lastBlock, errors.Wrapf(err, "Failed to retrieve parent block")
		}
	}

	if err := store.MustGetDBTx().CommitTx(); err != nil {
		glog.Errorf("Failed to commit db transaction: %s", err.Error())
	}
	return
}
