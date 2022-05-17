package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"time"

	"github.com/golang/glog"
	"golang.org/x/sync/errgroup"

	"github.com/pkg/errors"

	"github.com/open-dovetail/eth-track/common"
	"github.com/open-dovetail/eth-track/proc"
	"github.com/open-dovetail/eth-track/redshift"
)

type Config struct {
	nodeURL        string // Ethereum node URL
	apiKey         string // etherscan API key
	etherscanDelay int    // delay of consecutive etherscan API invocation in ms
	blockDelay     int    // blockchain height delay for last confirmed block
	threads        int    // number of threads for processing blocks
	batchSize      int    // size of block interval per worker job
	awsProfile     string // profile name for AWS user
	awsRegion      string // AWS region for redshift server
	awsSecret      string // AWS secret alias for redshift connection
	awsRedshift    string // redshift DB name
	awsS3Bucket    string // name of AWS s3 bucket
	awsCopyRole    string // aws role for copying csv from s3 to redshift
}

var config = &Config{}

// Initial values of the command-line args
func init() {
	flag.StringVar(&config.nodeURL, "nodeURL", "http://localhost:8545", "Ethereum node URL")
	flag.StringVar(&config.apiKey, "apiKey", "", "Etherscan API key")
	flag.IntVar(&config.etherscanDelay, "etherscanDelay", 350, "delay in millis between etherscan API calls")
	flag.IntVar(&config.blockDelay, "blockDelay", 12, "blockchain height delay for last confirmed block")
	flag.IntVar(&config.threads, "threads", 5, "number of threads for processing blocks")
	flag.IntVar(&config.batchSize, "batchSize", 40, "size of block interval per worker job")
	flag.StringVar(&config.awsProfile, "profile", "default", "profile name for AWS user")
	flag.StringVar(&config.awsRegion, "region", "us-west-2", "AWS region for redshift server")
	flag.StringVar(&config.awsSecret, "secret", "dev/ethdb/Redshift", "AWS secret alias for redshift connection")
	flag.StringVar(&config.awsRedshift, "redshift", "ethdb", "Redshift database name")
	flag.StringVar(&config.awsS3Bucket, "s3Bucket", "dev-eth-track", "AWS s3 bucket name")
	flag.StringVar(&config.awsCopyRole, "copyRole", "", "AWS role to copy csv from s3 to redshift")
}

// check env variables, which overrides the commandline input
func envOverride() {
	if v, ok := os.LookupEnv("ETHEREUM_URL"); ok && v != "" {
		config.nodeURL = v
	}
	if v, ok := os.LookupEnv("ETHERSCAN_APIKEY"); ok && v != "" {
		config.apiKey = v
	}
	if v, ok := os.LookupEnv("AWS_PROFILE"); ok && v != "" {
		config.awsProfile = v
	}
	if v, ok := os.LookupEnv("AWS_REGION"); ok && v != "" {
		config.awsRegion = v
	}
	if v, ok := os.LookupEnv("AWS_SECRET"); ok && v != "" {
		config.awsSecret = v
	}
	if v, ok := os.LookupEnv("AWS_REDSHIFT"); ok && v != "" {
		config.awsRedshift = v
	}
	if v, ok := os.LookupEnv("AWS_S3BUCKET"); ok && v != "" {
		config.awsS3Bucket = v
	}
	if v, ok := os.LookupEnv("AWS_COPY_ROLE"); ok && v != "" {
		config.awsCopyRole = v
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
}

// Turn on verbose logging using option -v 2
// Log to stderr using option -logtostderr or set env GLOG_logtostderr=true
// or log to specified folder using option -log_dir="mylogdir"
func main() {
	// parse command-line args
	flag.Parse()
	// override config with env vars
	envOverride()

	// initialize connections
	if err := connect(); err != nil {
		glog.Fatalf("Failed initialization of connections: %+v", err)
	}

	// initialize block progress from db
	if _, err := redshift.GetBlockCache(); err != nil {
		glog.Fatalf("Failed initialization of block cache: %+v", err)
	}

	// initialize contract cache to contain contracts invoked in the last month
	if err := proc.CacheContracts(30); err != nil {
		glog.Fatalf("Failed to fetch contracts from database: %+v", err)
	}

	// register os interrupt signal
	sig := make(chan os.Signal, config.threads)
	signal.Notify(sig, os.Interrupt, os.Kill)

	// start workers
	job := make(chan redshift.Interval, config.threads)
	g, ctx := errgroup.WithContext(context.Background())
	for i := 0; i < config.threads; i++ {
		pid := i
		g.Go(func() error {
			return work(pid, job, sig, ctx)
		})
	}

	// start scheduler
	g.Go(func() error {
		return schedule(job, sig, ctx)
	})

	// wait for scheduler and all workers to exit
	if err := g.Wait(); err != nil {
		glog.Infof("Failed from a processing thread: %v", err)
	}
	glog.Flush()
}

// initialize connections of Ethereum, etherscan and redshift
func connect() error {
	// initialize ethereum node client
	if _, err := proc.NewEthereumClient(config.nodeURL); err != nil {
		return errors.Wrapf(err, "Failed to connect to ethereum node %s", config.nodeURL)
	}
	proc.SetBlockDelay(config.blockDelay)

	// initialize etherscan api connection
	proc.ConfigEtherscan(config.apiKey, config.etherscanDelay)
	dai := "0x6b175474e89094c44da98b954eedeac495271d0f"
	if _, err := proc.FetchABI(dai, 0); err != nil {
		return errors.Wrapf(err, "Failed to invoke etherscan API with key %s", config.apiKey)
	}

	// config AWS s3 bucket
	if _, err := redshift.GetS3Bucket(config.awsS3Bucket, config.awsProfile, config.awsRegion, config.awsCopyRole); err != nil {
		return errors.Wrapf(err, "Failed to config AWS s3 bucket %s", config.awsS3Bucket)
	}

	// initialize redshift db connection
	secret, err := redshift.GetAWSSecret(config.awsSecret, config.awsProfile, config.awsRegion)
	if err != nil {
		return errors.Wrapf(err, "Failed to get redshift secret for profile %s", config.awsProfile)
	}
	poolSize := 2 * config.threads
	if poolSize < 10 {
		poolSize = 10
	}
	if _, err := redshift.Connect(secret, config.awsRedshift, poolSize); err != nil {
		return errors.Wrapf(err, "Failed to connect to redshift db %s", config.awsRedshift)
	}
	return nil
}

// continuously create block processing jobs until os interrupt is received
// each job is created as a block interval on the output channel
func schedule(job chan<- redshift.Interval, sig <-chan os.Signal, ctx context.Context) error {
	glog.Info("scheduler started")
	// schedule initial block gaps from database
	blockCache, _ := redshift.GetBlockCache()
	gaps := blockCache.GetIntervalGaps()
	glog.Info("schedule to fill block gaps in database")

	var pendingJobs []redshift.Interval
	for _, gap := range gaps {
		pendingJobs = addBatchJob(gap, pendingJobs)
	}
	for {
		select {
		case <-ctx.Done():
			glog.Infof("scheduler returns %v", ctx.Err())
			return ctx.Err()
		case <-sig:
			glog.Info("scheduler received os interrupt")
			return errors.New("interrupted")
		default:
			for len(job) < cap(job) {
				if len(pendingJobs) == 0 {
					// all pending jobs have been scheduled, so prepare new jobs
					var err error
					if pendingJobs, err = prepareJobs(blockCache); err != nil {
						return err
					}
				}

				// send a job since the job channel has more capacity
				v := pendingJobs[0]
				pendingJobs = pendingJobs[1:]
				job <- v
			}
			// wait 1 second for workers to catch up before trying again
			time.Sleep(time.Second)
		}
	}
}

// prepare the next batch of jobs in a queue, including new confirmed blocks and older unprocessed blocks.
func prepareJobs(blockCache *redshift.BlockInterval) ([]redshift.Interval, error) {
	var result []redshift.Interval

	// schedule new confirmed blocks
	lastBlock, err := proc.LastConfirmedBlock()
	if err != nil {
		glog.Infof("scheduler returns error while getting last confirmed block: %+v", err)
		return nil, err
	}
	hiBlock := lastBlock.Number
	scheduled := blockCache.GetScheduledBlocks()
	if scheduled.Low == 0 || scheduled.High == 0 {
		// no block has been processed, so schedule all new blocks
		lowBlock := hiBlock - uint64(config.threads*config.batchSize-1)
		glog.Infof("schedule new blocks of range [%d, %d]", lowBlock, hiBlock)
		v := redshift.Interval{Low: lowBlock, High: hiBlock}
		result = addBatchJob(v, result)
		blockCache.SetScheduledBlocks(v)
		return result, nil
	}
	if hiBlock > scheduled.High {
		glog.Infof("schedule new blocks of range (%d, %d]", scheduled.High, hiBlock)
		result = addBatchJob(redshift.Interval{
			Low:  scheduled.High + 1,
			High: hiBlock,
		}, result)
	}
	lowBlock := scheduled.Low - uint64(config.threads*config.batchSize)
	glog.Infof("schedule old blocks of range [%d, %d)", lowBlock, scheduled.Low)
	result = addBatchJob(redshift.Interval{
		Low:  lowBlock,
		High: scheduled.Low - 1,
	}, result)
	blockCache.SetScheduledBlocks(redshift.Interval{Low: lowBlock, High: hiBlock})

	return result, nil
}

// split an interval value into batch jobs of max interval of config.batchSize,
// append batch jobs to a jobs queue, and return the result
func addBatchJob(v redshift.Interval, jobs []redshift.Interval) []redshift.Interval {
	result := jobs
	low := v.Low
	hi := low + uint64(config.batchSize)
	for hi < v.High {
		result = append(result, redshift.Interval{Low: low, High: hi})
		low = hi + 1
		hi = low + uint64(config.batchSize-1)
	}
	if low <= v.High {
		result = append(result, redshift.Interval{Low: low, High: v.High})
	}
	return result
}

// continuously receive jobs from input channel.
// returns error if process failed or ctx closed by other worker when used with sync.errgroup.
func work(gid int, job <-chan redshift.Interval, sig <-chan os.Signal, ctx context.Context) error {
	glog.Info("started worker", gid)
	blockCache, _ := redshift.GetBlockCache()
	for {
		select {
		case <-ctx.Done():
			// exit when any other worker in errgroup error out
			glog.Infof("worker %d returns %v", gid, ctx.Err())
			return ctx.Err()
		case <-sig:
			// exit when received os interrupt
			glog.Infof("worker %d received os interrupt", gid)
			return errors.New("interrupted")
		case v := <-job:
			glog.Infof("worker %d processing block interval [%d, %d]", gid, v.Low, v.High)
			lastBlock, firstBlock, err := proc.DecodeBlockRange(v.High, v.Low)
			//lastBlock, firstBlock, err := simulator(v.High, v.Low)
			if err != nil {
				glog.Infof("worker %d returns error %v", gid, err)
				return err
			}
			block := lastBlock.Number
			for firstBlock != nil && block >= firstBlock.Number {
				blockCache.AddBlock(block)
				block--
			}
			if err := blockCache.SaveNextInterval(); err != nil {
				glog.Infof("worker %d returns save block cache error %v", gid, err)
				return err
			}
		}
	}
}

func simulator(hiBlock, lowBlock uint64) (lastBlock *common.Block, firstBlock *common.Block, err error) {
	time.Sleep(time.Second * 2)
	return &common.Block{Number: hiBlock}, &common.Block{Number: lowBlock}, nil
}
