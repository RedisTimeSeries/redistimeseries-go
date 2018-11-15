package redis_timeseries_go

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

// Client is an interface to time series redis commands
type Client struct {
	pool ConnPool
	name string
	authPass string
}

var maxConns = 500

// NewClient creates a new client connecting to the redis host, and using the given name as key prefix.
// Addr can be a single host:port pair, or a comma separated list of host:port,host:port...
// In the case of multiple hosts we create a multi-pool and select connections at random
func NewClient(addr, name string, authPass string) *Client {
	addrs := strings.Split(addr, ",")
	var pool ConnPool
	if len(addrs) == 1 {
		pool = NewSingleHostPool(addrs[0], authPass)
	} else {
		pool = NewMultiHostPool(addrs, authPass)
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

type Rule struct {
	DestKey       string
	BucketSizeSec int
	AggType       AggregationType
}

type KeyInfo struct {
	ChunkCount         int64
	MaxSamplesPerChunk int64
	LastTimestamp      int64
	RetentionSecs      int64
	Rules              []Rule
}

func ParseRules(ruleInterface interface{}, err error) (rules []Rule, retErr error) {
	if err != nil {
		return nil, err
	}
	ruleSlice, err := redis.Values(ruleInterface, nil)
	if err != nil {
		return nil, err
	}
	for _, ruleSlice := range ruleSlice {

		ruleValues, err := redis.Values(ruleSlice, nil)
		if err != nil {
			return nil, err
		}
		destKey, err := redis.String(ruleValues[0], nil)
		if err != nil {
			return nil, err
		}
		bucketSizeSec, err := redis.Int(ruleValues[1], nil)
		if err != nil {
			return nil, err
		}
		aggType, err := toAggregationType(ruleValues[2])
		if err != nil {
			return nil, err
		}
		rules = append(rules, Rule{destKey, bucketSizeSec, aggType})
	}
	return rules, nil
}

func ParseInfo(result interface{}, err error) (info KeyInfo, outErr error) {
	values, err := redis.Values(result, nil)
	if err != nil {
		return KeyInfo{}, err
	}
	if len(values)%2 != 0 {
		return KeyInfo{}, errors.New("ParseInfo expects even number of values result")
	}
	var key string
	for i := 0; i < len(values); i += 2 {
		key, err = redis.String(values[i], nil)
		switch key {
		case "rules":
			info.Rules, err = ParseRules(values[i+1], nil)
		case "retentionSecs":
			info.RetentionSecs, err = redis.Int64(values[i+1], nil)
		case "chunkCount":
			info.ChunkCount, err = redis.Int64(values[i+1], nil)
		case "maxSamplesPerChunk":
			info.MaxSamplesPerChunk, err = redis.Int64(values[i+1], nil)
		case "lastTimestamp":
			info.LastTimestamp, err = redis.Int64(values[i+1], nil)
		}
		if err != nil {
			return KeyInfo{}, err
		}
	}

	return info, nil
}

// Info create a new time-series
func (client *Client) Info(key string) (res KeyInfo, err error) {
	conn := client.pool.Get()
	defer conn.Close()
	res, err = ParseInfo(conn.Do("TS.INFO", key))
	return res, err
}

//go:generate stringer -type=AggregationType
type AggregationType int

const (
	AvgAggregation AggregationType = iota
	SumAggregation
	MinAggregation
	MaxAggregation
	CountAggregation
	FirstAggregation
	LastAggregation
)

var aggToString = map[AggregationType]string{
	AvgAggregation:   "AVG",
	SumAggregation:   "SUM",
	MinAggregation:   "MIN",
	MaxAggregation:   "MAX",
	CountAggregation: "COUNT",
	FirstAggregation: "FIRST",
	LastAggregation:  "LAST",
}

func (aggType AggregationType) String() string {
	return aggToString[aggType]
}

func toAggregationType(aggType interface{}) (AggregationType, error) {
	aggTypeStr, err := redis.String(aggType, nil)
	if err != nil {
		return 0, err
	}
	for k, v := range aggToString {
		if v == aggTypeStr {
			return k, nil
		}
	}
	return 0, fmt.Errorf("AggregationType not found %q", aggType)
}

// TS.CREATERULE create a compaction rule
// SOURCE_KEY - key name for source time series
// AGG_TYPE - AggregationType
// BUCKET_SIZE_SEC - time bucket for aggregated compaction,
// DEST_KEY - key name for destination time series
func (client *Client) CreateRule(sourceKey string, aggType AggregationType, bucketSizeSec uint, destinationKey string) (err error) {
	conn := client.pool.Get()
	defer conn.Close()
	_, err = conn.Do("TS.CREATERULE", sourceKey, aggType.String(), bucketSizeSec, destinationKey)
	return err
}

// deleterule - delete a compaction rule
// args:
// SOURCE_KEY - key name for source time series
// DEST_KEY - key name for destination time series
func (client *Client) DeleteRule(sourceKey string, destinationKey string) (err error) {
	conn := client.pool.Get()
	defer conn.Close()
	_, err = conn.Do("TS.DELETERULE", sourceKey, destinationKey)
	return err
}

func floatToStr(inputFloat float64) string {
	return strconv.FormatFloat(inputFloat, 'g', 16, 64)
}

func strToFloat(inputString string) (float64, error) {
	return strconv.ParseFloat(inputString, 64)
}

// add - append a new value to the series
// args:
// key - time series key name
// timestamp - time of value
// value - value
func (client *Client) Add(key string, timestamp int64, value float64) (err error) {
	conn := client.pool.Get()
	defer conn.Close()
	_, err = conn.Do("TS.ADD", key, timestamp, floatToStr(value))
	return err
}

type DataPoint struct {
	Timestamp int64
	Value     float64
}

func parseDataPoints(info interface{}) (dataPoints []DataPoint, err error) {
	values, err := redis.Values(info, err)
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return []DataPoint{}, nil
	}
	for _, i := range values {
		iValues, err := redis.Values(i, err)
		if err != nil {
			return nil, err
		}
		rawTimestamp := iValues[0]
		strValue, err := redis.String(iValues[1], nil)
		if err != nil {
			return nil, err
		}
		timestamp, err := redis.Int64(rawTimestamp, nil)
		if err != nil {
			return nil, err
		}
		value, err := strToFloat(strValue)
		if err != nil {
			return nil, err
		}
		dataPoint := DataPoint{timestamp, value}
		dataPoints = append(dataPoints, dataPoint)
	}
	return dataPoints, nil
}

// range - ranged query
// args:
// key - time series key name
// fromTimestamp - start of range
// toTimestamp - end of range
func (client *Client) Range(key string, fromTimestamp int64, toTimestamp int64) (dataPoints []DataPoint,
	err error) {
	conn := client.pool.Get()
	defer conn.Close()
	info, err := conn.Do("TS.RANGE", key, strconv.FormatInt(fromTimestamp, 10),
		strconv.FormatInt(toTimestamp, 10))
	if err != nil {
		return nil, err
	}
	dataPoints, err = parseDataPoints(info)
	return dataPoints, err
}

// AggRange - aggregation over a ranged query
// args:
// key - time series key name
// fromTimestamp - start of range
// toTimestamp - end of range
// aggType - aggregation type
// bucketSizeSec - time bucket for aggregation
func (client *Client) AggRange(key string, fromTimestamp int64, toTimestamp int64, aggType AggregationType,
	bucketSizeSec int) (dataPoints []DataPoint, err error) {
	conn := client.pool.Get()
	defer conn.Close()
	info, err := conn.Do("TS.RANGE", key, strconv.FormatInt(fromTimestamp, 10),
		strconv.FormatInt(toTimestamp, 10), aggType.String(), bucketSizeSec)
	if err != nil {
		return nil, err
	}
	dataPoints, err = parseDataPoints(info)
	return dataPoints, err
}
