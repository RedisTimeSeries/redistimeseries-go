package redis_timeseries_go

import (
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
)

// TODO: refactor this hard limit and revise client locking
// Client Max Connections
var maxConns = 500

// NewClient creates a new client connecting to the redis host, and using the given name as key prefix.
// Addr can be a single host:port pair, or a comma separated list of host:port,host:port...
// In the case of multiple hosts we create a multi-pool and select connections at random
func NewClient(addr, name string, authPass *string) *Client {
	addrs := strings.Split(addr, ",")
	var pool ConnPool
	if len(addrs) == 1 {
		pool = NewSingleHostPool(addrs[0], authPass)
	} else {
		pool = NewMultiHostPool(addrs, authPass)
	}
	ret := &Client{
		Pool: pool,
		Name: name,
	}
	return ret
}


// NewAutocompleter creates a new Autocompleter with the given pool and index name
func NewClientFromPool(pool *redis.Pool, name string) *Client {
	ret := &Client{
		Pool: pool,
		Name: name,
	}
	return ret
}

// CreateKey create a new time-series
// Deprecated: This function has been deprecated, use CreateKeyWithOptions instead
func (client *Client) CreateKey(key string, retentionTime time.Duration) (err error) {
	conn := client.Pool.Get()
	defer conn.Close()
	opts := DefaultCreateOptions
	opts.RetentionMSecs = retentionTime
	return client.CreateKeyWithOptions(key, opts)
}

func (client *Client) CreateKeyWithOptions(key string, options CreateOptions) (err error) {
	conn := client.Pool.Get()
	defer conn.Close()

	args := []interface{}{key}
	args, err = options.Serialize(args)
	if err != nil {
		return
	}
	_, err = conn.Do("TS.CREATE", args...)
	return err
}


// Add - Append (or create and append) a new sample to the series
// args:
// key - time series key name
// timestamp - time of value
// value - value
func (client *Client) Add(key string, timestamp int64, value float64) (storedTimestamp int64, err error) {
	conn := client.Pool.Get()
	defer conn.Close()
	return redis.Int64(conn.Do("TS.ADD", key, timestamp, floatToStr(value)))
}

// AddAutoTs - Append (or create and append) a new sample to the series, with DB automatic timestamp (using the system clock)
// args:
// key - time series key name
// value - value
func (client *Client) AddAutoTs(key string, value float64) (storedTimestamp int64, err error) {
	conn := client.Pool.Get()
	defer conn.Close()
	return redis.Int64(conn.Do("TS.ADD", key, "*", floatToStr(value)))
}

// AddWithOptions - Append (or create and append) a new sample to the series, with the specified CreateOptions
// args:
// key - time series key name
// timestamp - time of value
// value - value
// options - define options for create key on add
func (client *Client) AddWithOptions(key string, timestamp int64, value float64, options CreateOptions) (storedTimestamp int64, err error) {
	conn := client.Pool.Get()
	defer conn.Close()

	args := []interface{}{key, timestamp, floatToStr(value)}
	args, err = options.Serialize(args)
	if err != nil {
		return
	}
	return redis.Int64(conn.Do("TS.ADD", args...))
}

// AddAutoTsWithOptions - Append (or create and append) a new sample to the series, with the specified CreateOptions and DB automatic timestamp (using the system clock)
// args:
// key - time series key name
// value - value
// options - define options for create key on add
func (client *Client) AddAutoTsWithOptions(key string, value float64, options CreateOptions) (storedTimestamp int64, err error) {
	conn := client.Pool.Get()
	defer conn.Close()
	args := []interface{}{key, "*", floatToStr(value)}
	args, err = options.Serialize(args)
	if err != nil {
		return
	}
	return redis.Int64(conn.Do("TS.ADD", args...))
}

// AddWithRetention - append a new value to the series with a duration
// args:
// key - time series key name
// timestamp - time of value
// value - value
// duration - value
// Deprecated: This function has been deprecated, use AddWithOptions instead
func (client *Client) AddWithRetention(key string, timestamp int64, value float64, duration int64) (storedTimestamp int64, err error) {
	conn := client.Pool.Get()
	defer conn.Close()
	options := DefaultCreateOptions
	options.RetentionMSecs = time.Duration(duration)
	return client.AddWithOptions(key, timestamp, value, options)
}

// CreateRule - create a compaction rule
// args:
// sourceKey - key name for source time series
// aggType - AggregationType
// bucketSizeMSec - Time bucket for aggregation in milliseconds
// destinationKey - key name for destination time series
func (client *Client) CreateRule(sourceKey string, aggType AggregationType, bucketSizeMSec uint, destinationKey string) (err error) {
	conn := client.Pool.Get()
	defer conn.Close()
	_, err = conn.Do("TS.CREATERULE", sourceKey, destinationKey, "AGGREGATION", aggType, bucketSizeMSec)
	return err
}

// DeleteRule - delete a compaction rule
// args:
// sourceKey - key name for source time series
// destinationKey - key name for destination time series
func (client *Client) DeleteRule(sourceKey string, destinationKey string) (err error) {
	conn := client.Pool.Get()
	defer conn.Close()
	_, err = conn.Do("TS.DELETERULE", sourceKey, destinationKey)
	return err
}

// Range - ranged query
// args:
// key - time series key name
// fromTimestamp - start of range. You can use TimeRangeMinimum to express the minimum possible timestamp.
// toTimestamp - end of range. You can use TimeRangeFull or TimeRangeMaximum to express the maximum possible timestamp.
// Deprecated: This function has been deprecated, use RangeWithOptions instead
func (client *Client) Range(key string, fromTimestamp int64, toTimestamp int64) (dataPoints []DataPoint, err error) {
	return client.RangeWithOptions(key, fromTimestamp, toTimestamp, DefaultRangeOptions)

}

// AggRange - aggregation over a ranged query
// args:
// key - time series key name
// fromTimestamp - start of range. You can use TimeRangeMinimum to express the minimum possible timestamp.
// toTimestamp - end of range. You can use TimeRangeFull or TimeRangeMaximum to express the maximum possible timestamp.
// aggType - aggregation type
// bucketSizeSec - time bucket for aggregation
// Deprecated: This function has been deprecated, use RangeWithOptions instead
func (client *Client) AggRange(key string, fromTimestamp int64, toTimestamp int64, aggType AggregationType,
	bucketSizeSec int) (dataPoints []DataPoint, err error) {
	rangeOptions := NewRangeOptions()
	rangeOptions = rangeOptions.SetAggregation(aggType, bucketSizeSec)
	return client.RangeWithOptions(key, fromTimestamp, toTimestamp, *rangeOptions)
}

// RangeWithOptions - Query a timestamp range on a specific time-series
// args:
// key - time-series key name
// fromTimestamp - start of range. You can use TimeRangeMinimum to express the minimum possible timestamp.
// toTimestamp - end of range. You can use TimeRangeFull or TimeRangeMaximum to express the maximum possible timestamp.
// rangeOptions - RangeOptions options. You can use the default DefaultRangeOptions
func (client *Client) RangeWithOptions(key string, fromTimestamp int64, toTimestamp int64, rangeOptions RangeOptions) (dataPoints []DataPoint, err error) {
	conn := client.Pool.Get()
	defer conn.Close()
	var reply interface{}
	args := createRangeCmdArguments(key, fromTimestamp, toTimestamp, rangeOptions)
	reply, err = conn.Do("TS.RANGE", args...)
	if err != nil {
		return
	}
	dataPoints, err = ParseDataPoints(reply)
	return
}

// AggMultiRange - Query a timestamp range across multiple time-series by filters.
// args:
// fromTimestamp - start of range. You can use TimeRangeMinimum to express the minimum possible timestamp.
// toTimestamp - end of range. You can use TimeRangeFull or TimeRangeMaximum to express the maximum possible timestamp.
// aggType - aggregation type
// bucketSizeSec - time bucket for aggregation
// filters - list of filters e.g. "a=bb", "b!=aa"
// Deprecated: This function has been deprecated, use MultiRangeWithOptions instead
func (client *Client) AggMultiRange(fromTimestamp int64, toTimestamp int64, aggType AggregationType,
	bucketSizeSec int, filters ...string) (ranges []Range, err error) {
	mrangeOptions := NewMultiRangeOptions()
	mrangeOptions = mrangeOptions.SetAggregation(aggType, bucketSizeSec)
	return client.MultiRangeWithOptions(fromTimestamp, toTimestamp, *mrangeOptions, filters...)
}

// MultiRangeWithOptions - Query a timestamp range across multiple time-series by filters.
// args:
// fromTimestamp - start of range. You can use TimeRangeMinimum to express the minimum possible timestamp.
// toTimestamp - end of range. You can use TimeRangeFull or TimeRangeMaximum to express the maximum possible timestamp.
// mrangeOptions - MultiRangeOptions options. You can use the default DefaultMultiRangeOptions
// filters - list of filters e.g. "a=bb", "b!=aa"
func (client *Client) MultiRangeWithOptions(fromTimestamp int64, toTimestamp int64, mrangeOptions MultiRangeOptions, filters ...string) (ranges []Range, err error) {
	conn := client.Pool.Get()
	defer conn.Close()
	var reply interface{}
	args := createMultiRangeCmdArguments(fromTimestamp, toTimestamp, mrangeOptions, filters)
	reply, err = conn.Do("TS.MRANGE", args...)
	if err != nil {
		return
	}
	ranges, err = ParseRanges(reply)
	return
}

// Get - Get the last sample of a time-series.
// args:
// key - time-series key name
func (client *Client) Get(key string) (dataPoint *DataPoint,
	err error) {
	conn := client.Pool.Get()
	defer conn.Close()
	resp, err := conn.Do("TS.GET", key)
	if err != nil {
		return nil, err
	}
	dataPoint, err = ParseDataPoint(resp)
	return
}

// MultiGet - Get the last sample across multiple time-series, matching the specific filters.
// args:
// filters - list of filters e.g. "a=bb", "b!=aa"
func (client *Client) MultiGet(filters ...string) (ranges []Range,
	err error) {
	conn := client.Pool.Get()
	defer conn.Close()
	var reply interface{}
	if len(filters) == 0 {
		return
	}
	args := []interface{}{"FILTER"}
	for _, filter := range filters {
		args = append(args, filter)
	}

	reply, err = conn.Do("TS.MGET", args...)

	if err != nil {
		return
	}
	ranges, err = ParseRangesSingleDataPoint(reply)
	return
}

// Returns information and statistics on the time-series.
// args:
// key - time-series key name
func (client *Client) Info(key string) (res KeyInfo, err error) {
	conn := client.Pool.Get()
	defer conn.Close()
	res, err = ParseInfo(conn.Do("TS.INFO", key))
	return res, err
}
