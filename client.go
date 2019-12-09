package redis_timeseries_go

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
	"github.com/gomodule/redigo/redis"
)

type CreateOptions struct {
	RetentionSecs time.Duration
	Labels map[string]string
}

// Append options to args
func (options *CreateOptions) Append(args []interface{}) (result []interface{}) {
	if options.RetentionSecs >= 0 {
		args = append(args, "RETENTION", formatMilliSec(options.RetentionSecs))			
	}		
	if len(options.Labels) > 0 {
		args = append(args, "LABELS")
		for key, value := range options.Labels {
	        args = append(args, key)
	        args = append(args, value)
	    }
	}
	return args
}


// Client is an interface to time series redis commands
type Client struct {
	Pool ConnPool
	Name string
}

var maxConns = 500

// Helper function to create a string pointer from a string literal.
// Useful for calls to NewClient with an auth pass that is known at compile time.
func MakeStringPtr(s string) *string {
	return &s
}

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

func formatMilliSec(dur time.Duration) int64 {
	if dur > 0 && dur < time.Millisecond {
		log.Printf("specified duration is %s, but minimal supported value is %s", dur, time.Millisecond)
	}
	return int64(dur / time.Millisecond)
}

// CreateKey create a new time-series
func (client *Client) CreateKey(key string, retentionTime time.Duration) (err error) {
	conn := client.Pool.Get()
	defer conn.Close()
	_, err = conn.Do("TS.CREATE", key, "RETENTION", formatMilliSec(retentionTime))
	return err
}

func (client *Client) CreateKeyWithOptions(key string, options CreateOptions) (err error) {
	conn := client.Pool.Get()
	defer conn.Close()
	
	args := []interface{}{key}
	args = options.Append(args)

	_, err = conn.Do("TS.CREATE", args...)
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
	RetentionTime      int64
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
		case "retentionTime":
			info.RetentionTime, err = redis.Int64(values[i+1], nil)
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
	conn := client.Pool.Get()
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
	StdPAggregation
	StdSAggregation
	VarPAggregation
	VarSAggregation
)

var aggToString = map[AggregationType]string{
	AvgAggregation:   "AVG",
	SumAggregation:   "SUM",
	MinAggregation:   "MIN",
	MaxAggregation:   "MAX",
	CountAggregation: "COUNT",
	FirstAggregation: "FIRST",
	LastAggregation:  "LAST",
	StdPAggregation: "STD.P",
	StdSAggregation: "STD.S",
	VarPAggregation: "VAR.P",
	VarSAggregation: "VAR.S",
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
	conn := client.Pool.Get()
	defer conn.Close()
	_, err = conn.Do("TS.CREATERULE", sourceKey, destinationKey, "AGGREGATION", aggType.String(), bucketSizeSec)
	return err
}

// deleterule - delete a compaction rule
// args:
// SOURCE_KEY - key name for source time series
// DEST_KEY - key name for destination time series
func (client *Client) DeleteRule(sourceKey string, destinationKey string) (err error) {
	conn := client.Pool.Get()
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
// options - define options for create key on add 
func (client *Client) AddWithOptions(key string, timestamp int64, value float64, options CreateOptions) (err error) {
	conn := client.Pool.Get()
	defer conn.Close()
	
	args := []interface{}{key, timestamp, floatToStr(value)}
	args = options.Append(args)
	_, err = conn.Do("TS.ADD", args...)
	return err
}

func (client *Client) Add(key string, timestamp int64, value float64) (storedTimestamp int64, err error) {
	conn := client.Pool.Get()
	defer conn.Close()
	return redis.Int64( conn.Do("TS.ADD", key, timestamp, floatToStr(value)))
}

// addwithduration - append a new value to the series with a duration
// args:
// key - time series key name
// timestamp - time of value
// value - value
// duration - value
func (client *Client) AddWithRetention(key string, timestamp int64, value float64, duration int64) (err error) {
	conn := client.Pool.Get()
	defer conn.Close()
	_, err = conn.Do("TS.ADD", key, timestamp, floatToStr(value), "RETENTION", strconv.FormatInt(duration, 10))
	return err
}

type DataPoint struct {
	Timestamp int64
	Value     float64
}

func ParseDataPoints(info interface{}) (dataPoints []DataPoint, err error) {
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

func ParseLabels(res interface{}) (labels map[string]string, err error) {
	values, err := redis.Values(res, err)
	if err != nil {
		return
	}
	labels = make(map[string]string, len(values))
	for i := 0; i < len(values); i++ {
		iValues, err := redis.Values(values[i], err)
		if err != nil {
			return nil, err
		}
		if len(iValues) != 2 {
			err = errors.New("ParseLabels: expects 2 elements per inner-array")
			return nil, err
		}
		key, okKey := iValues[0].([]byte)
		value, okValue := iValues[1].([]byte)
		if !okKey || !okValue {
			err = errors.New("ParseLabels: StringMap key not a bulk string value")
			return nil, err
		}
		labels[string(key)] = string(value)
	}
	return
}

type Range struct {
	Name       string
	Labels     map[string]string
	DataPoints []DataPoint
}

func ParseRanges(info interface{}) (ranges []Range, err error) {
	values, err := redis.Values(info, err)
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return []Range{}, nil
	}
	
	for _, i := range values {
		iValues, err := redis.Values(i, err)
		if err != nil {
			return nil, err
		}
		if len(iValues) != 3 {
			err = errors.New("ParseRanges: expects 3 elements per inner-array")
			return nil, err
		}

		name, err := redis.String(iValues[0], nil)
		if err != nil {
			return nil, err
		}

		labels, err := ParseLabels(iValues[1])
		if err != nil {
			return nil, err
		}

		dataPoints, err := ParseDataPoints(iValues[2])
		if err != nil {
			return nil, err
		}
		r := Range{ name, labels, dataPoints}
		ranges = append(ranges, r)
	}
	return ranges, nil
}


// range - ranged query
// args:
// key - time series key name
// fromTimestamp - start of range
// toTimestamp - end of range
func (client *Client) Range(key string, fromTimestamp int64, toTimestamp int64) (dataPoints []DataPoint,
	err error) {
	conn := client.Pool.Get()
	defer conn.Close()
	info, err := conn.Do("TS.RANGE", key, strconv.FormatInt(fromTimestamp, 10), strconv.FormatInt(toTimestamp, 10))
	if err != nil {
		return nil, err
	}
	dataPoints, err = ParseDataPoints(info)
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
	conn := client.Pool.Get()
	defer conn.Close()
	info, err := conn.Do("TS.RANGE", key, strconv.FormatInt(fromTimestamp, 10), strconv.FormatInt(toTimestamp, 10),
		"AGGREGATION", aggType.String(), bucketSizeSec)
	if err != nil {
		return nil, err
	}
	dataPoints, err = ParseDataPoints(info)
	return dataPoints, err
}
	
	// AggRange - aggregation over a ranged query
// args:
// fromTimestamp - start of range
// toTimestamp - end of range
// aggType - aggregation type
// bucketSizeSec - time bucket for aggregation
// filters - list of filters e.g. "a=bb", b!=aa"
func (client *Client) AggMultiRange(fromTimestamp int64, toTimestamp int64, aggType AggregationType,
	bucketSizeSec int, filters ...string) (ranges []Range, err error) {
	conn := client.Pool.Get()
	defer conn.Close()
	
	args := []interface{}{strconv.FormatInt(fromTimestamp, 10), strconv.FormatInt(toTimestamp, 10), 
		"AGGREGATION", aggType.String(), bucketSizeSec, "FILTER"}

	for _, filter := range filters{
		args = append(args, filter) 
	}
	
	info, err := conn.Do("TS.MRANGE", args...)
	if err != nil {
		return nil, err
	}
	ranges, err = ParseRanges(info)
	return ranges, err
}
