package proc

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/umbracle/go-web3/jsonrpc"
)

type EtherscanAPI struct {
	apiKey         string // etherscan API key
	etherscanDelay int    // delay of consecutive etherscan API invocation in ms
	apiTime        int64  // Unix millis of last etherscan API invocation
}

type EthereumClient struct {
	nodeURL string // Ethereum node URL
	client  *jsonrpc.Client
}

// singleton
var api *EtherscanAPI
var apiLock = &sync.Mutex{}
var eth *EthereumClient

func NewEtherscanAPI(apiKey string, etherscanDelay int) *EtherscanAPI {
	api = &EtherscanAPI{apiKey: apiKey}
	if etherscanDelay > 0 {
		api.etherscanDelay = etherscanDelay
	}
	return api
}

func GetEtherscanAPI() *EtherscanAPI {
	return api
}

func NewEthereumClient(nodeURL string) (*EthereumClient, error) {
	client, err := jsonrpc.NewClient(nodeURL)
	if err != nil {
		return nil, err
	}
	eth = &EthereumClient{
		nodeURL: nodeURL,
		client:  client,
	}
	return eth, nil
}

func CloseEthereumClient() error {
	return eth.client.Close()
}

func GetEthereumClient() *jsonrpc.Client {
	if eth == nil {
		return nil
	}
	return eth.client
}

// calls etherscan to fetch contract ABI - control the delay of calls so the rate is no more than 5 per second
func (c *EtherscanAPI) FetchABI(address string) (string, error) {
	apiLock.Lock()
	defer apiLock.Unlock()

	// fmt.Printf("Get ABI @ %d: %s\n", time.Now().UnixNano()/1000000, address)
	if c.etherscanDelay > 0 {
		// control etherscan call rate
		delay := int64(c.etherscanDelay) - (int64(time.Now().UnixNano()/1000000) - c.apiTime)
		if delay > 0 {
			if glog.V(2) {
				glog.Infof("Sleep %d ms", delay)
			}
			// fmt.Printf("Sleep %d ms\n", delay)
			time.Sleep(time.Duration(delay) * time.Millisecond)
		}
		c.apiTime = int64(time.Now().UnixNano() / 1000000)
	}

	url := fmt.Sprintf("https://api.etherscan.io/api?apikey=%s&module=contract&action=getabi&address=%s", c.apiKey, address)
	data, err := httpGet(url, 0)
	if err != nil {
		return "", err
	}
	return data.(string), nil
}

// Note: web3.etherscan.Query does not consistently return on consecutive calls, so use my own HTTP calls to etherscan
func httpGet(url string, timeout int) (interface{}, error) {
	if timeout <= 0 {
		// default time out to 5 second
		timeout = 15
	}
	client := http.Client{
		Timeout: time.Duration(timeout * int(time.Second)),
	}
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	var out map[string]interface{}
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return out["result"], nil
}
