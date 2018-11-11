package redis_timeseries

import (
	"errors"
	"log"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

// Client is an interface to time series redis commands
type Client struct {
	pool ConnPool
	name string
}

var maxConns = 500

// NewClient creates a new client connecting to the redis host, and using the given name as key prefix.
// Addr can be a single host:port pair, or a comma separated list of host:port,host:port...
// In the case of multiple hosts we create a multi-pool and select connections at random
func NewClient(addr, name string) *Client {
	addrs := strings.Split(addr, ",")
	var pool ConnPool
	if len(addrs) == 1 {
		pool = NewSingleHostPool(addrs[0])
	} else {
		pool = NewMultiHostPool(addrs)
	}
	ret := &Client{
		pool: pool,
		name: name,
	}
	return ret
}

func formatSec(dur time.Duration) int64 {
	if dur > 0 && dur < time.Second {
		log.Printf("specified duration is %s, but minimal supported value is %s", dur, time.Second)
	}
	return int64(dur / time.Second)
}

// CreateKey create a new time-series
func (client *Client) CreateKey(key string, retentionSecs time.Duration, maxSamplesPerChunk uint) (err error) {
	conn := client.pool.Get()
	defer conn.Close()
	_, err = conn.Do("TS.CREATE", key, formatSec(retentionSecs), maxSamplesPerChunk)
	return err
}

func ParseInfo(result interface{}, err error) (intMap map[string]int64, outErr error) {
	values, err := redis.Values(result, err)
	if err != nil {
		return nil, err
	}
	if len(values)%2 != 0 {
		return nil, errors.New("ParseInfo expects even number of values result")
	}

	var key string
	var value int64
	intMap = make(map[string]int64, (len(values)-1)/2)
	for i := 0; i < len(values); i += 2 {
		key, err = redis.String(values[i], nil) //string(values[i].([]byte))
		if key == "rules" {
			continue
		}
		value, err = redis.Int64(values[i+1], nil) // string(values[i].([]byte)) // strconv.ParseInt(string(values[i+1].([]byte)), 10, 0)
		if err != nil {
			return nil, err
		}
		intMap[key] = value
	}
	return intMap, nil
}

// Info create a new time-series
func (client *Client) Info(key string) (res interface{}, err error) {
	//TODO: parse rules
	conn := client.pool.Get()
	defer conn.Close()
	res, err = ParseInfo(conn.Do("TS.INFO", key))
	return res, err
}
