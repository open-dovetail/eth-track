package proc

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/golang/glog"
)

type etherscan struct {
	sync.Mutex
	apiKey   string // etherscan API key
	delay    int    // delay of consecutive etherscan API invocation in ms
	lastTime int64  // Unix millis of last etherscan API invocation
}

// singleton
var api *etherscan

func ConfigEtherscan(apiKey string, delay int) {
	api = &etherscan{apiKey: apiKey}
	if delay > 0 {
		api.delay = delay
	}
}

// calls etherscan to fetch contract ABI - control the delay of calls so the rate is no more than 5 per second
func FetchABI(address string, timeout int) (string, error) {
	if api == nil || len(api.apiKey) == 0 {
		// panic if API key is not configured
		glog.Fatalln("Etherscan APIkey is not configured.  Must first call ConfigEtherscan(apiKey, delay)")
	}

	// make 1 etherscan call at a time to limit the rate of API calls
	api.Lock()
	defer api.Unlock()

	// fmt.Printf("Get ABI @ %d: %s\n", time.Now().UnixNano()/1000000, address)
	if api.delay > 0 {
		// control etherscan call rate
		delay := int64(api.delay) - (int64(time.Now().UnixNano()/1000000) - api.lastTime)
		if delay > 0 {
			if glog.V(2) {
				glog.Infof("Sleep %d ms", delay)
			}
			// fmt.Printf("Sleep %d ms\n", delay)
			time.Sleep(time.Duration(delay) * time.Millisecond)
		}
		api.lastTime = int64(time.Now().UnixNano() / 1000000)
	}

	data, err := api.httpGetABI(address, timeout)
	if err != nil {
		return "", err
	}
	return data.(string), nil
}

// Note: web3.etherscan.Query does not consistently return on consecutive calls, so use my own HTTP calls to etherscan
func (c *etherscan) httpGetABI(address string, timeout int) (interface{}, error) {
	if timeout <= 0 {
		// default time out to 10 second
		timeout = 5
	}
	url := fmt.Sprintf("https://api.etherscan.io/api?apikey=%s&module=contract&action=getabi&address=%s", c.apiKey, address)

	// We have to setup the transport timeout, otherwise, retry would not work after connection failure
	var netTransport = &http.Transport{
		Dial: (&net.Dialer{
			Timeout: time.Duration(timeout) * time.Second,
		}).Dial,
		TLSHandshakeTimeout: time.Duration(timeout) * time.Second,
	}
	client := &http.Client{
		Timeout:   time.Duration(timeout+3) * time.Second,
		Transport: netTransport,
	}
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	// ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	// defer cancel()
	// request = request.WithContext(ctx)
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
