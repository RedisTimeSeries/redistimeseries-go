package redis_timeseries

import (
	"log"
	"strings"
	"time"
)

// Client is an interface to time series redis commands
type Client struct {
	pool ConnPool
	name string
}

// NewClient creates a new client connecting to the redis host, and using the given name as key prefix.
// Addr can be a single host:port pair, or a comma separated list of host:port,host:port...
// In the case of multiple hosts we create a multi-pool and select connections at random
func NewClient(addr, name string) *Client {
	addresses := strings.Split(addr, ",")
	var pool ConnPool
	if len(addresses) == 1 {
		pool = NewSingleHostPool(addresses[0])
	} else {
		pool = NewMultiHostPool(addresses)
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
func (client *Client) CreateKey(key string, retentionSecs time.Duration, maxSamplesPerChunk uint) error {

	args := []interface{}{key, formatSec(retentionSecs), string(maxSamplesPerChunk)}

	conn := client.pool.Get()
	defer conn.Close()
	_, err := conn.Do("TS.CREATE", args...)
	return err
}