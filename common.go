package redis_timeseries_go

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

//go:generate stringer -type=AggregationType
type AggregationType string

//go:generate stringer -type=ReducerType
type ReducerType string

//go:generate stringer -type=DuplicatePolicyType
type DuplicatePolicyType string

const (
	SumReducer ReducerType = "SUM"
	MinReducer ReducerType = "MIN"
	MaxReducer ReducerType = "MAX"
)

const (
	AvgAggregation   AggregationType = "AVG"
	SumAggregation   AggregationType = "SUM"
	MinAggregation   AggregationType = "MIN"
	MaxAggregation   AggregationType = "MAX"
	CountAggregation AggregationType = "COUNT"
	FirstAggregation AggregationType = "FIRST"
	LastAggregation  AggregationType = "LAST"
	StdPAggregation  AggregationType = "STD.P"
	StdSAggregation  AggregationType = "STD.S"
	VarPAggregation  AggregationType = "VAR.P"
	VarSAggregation  AggregationType = "VAR.S"
)

const (
	CREATE_CMD     string = "TS.CREATE"
	ALTER_CMD      string = "TS.ALTER"
	ADD_CMD        string = "TS.ADD"
	MADD_CMD       string = "TS.MADD"
	INCRBY_CMD     string = "TS.INCRBY"
	DECRBY_CMD     string = "TS.DECRBY"
	CREATERULE_CMD string = "TS.CREATERULE"
	DELETERULE_CMD string = "TS.DELETERULE"
	RANGE_CMD      string = "TS.RANGE"
	REVRANGE_CMD   string = "TS.REVRANGE"
	MRANGE_CMD     string = "TS.MRANGE"
	MREVRANGE_CMD  string = "TS.MREVRANGE"
	GET_CMD        string = "TS.GET"
	MGET_CMD       string = "TS.MGET"
	INFO_CMD       string = "TS.INFO"
	QUERYINDEX_CMD string = "TS.QUERYINDEX"
	DEL_CMD        string = "DEL"
	TS_DEL_CMD     string = "TS.DEL"
)

// Check https://oss.redislabs.com/redistimeseries/configuration/#duplicate_policy for more inforamtion about duplicate policies
const (
	BlockDuplicatePolicy DuplicatePolicyType = "block" // an error will occur for any out of order sample
	FirstDuplicatePolicy DuplicatePolicyType = "first" // ignore the new value
	LastDuplicatePolicy  DuplicatePolicyType = "last"  // override with latest value
	MinDuplicatePolicy   DuplicatePolicyType = "min"   // only override if the value is lower than the existing value
	MaxDuplicatePolicy   DuplicatePolicyType = "max"   // only override if the value is higher than the existing value
)

var aggToString = []AggregationType{AvgAggregation, SumAggregation, MinAggregation, MaxAggregation, CountAggregation, FirstAggregation, LastAggregation, StdPAggregation, StdSAggregation, VarPAggregation, VarSAggregation}

// CreateOptions are a direct mapping to the options provided when creating a new time-series
// Check https://oss.redislabs.com/redistimeseries/1.4/commands/#tscreate for a detailed description
type CreateOptions struct {
	Uncompressed    bool
	RetentionMSecs  time.Duration
	Labels          map[string]string
	ChunkSize       int64
	DuplicatePolicy DuplicatePolicyType
}

var DefaultCreateOptions = CreateOptions{
	Uncompressed:    false,
	RetentionMSecs:  0,
	Labels:          map[string]string{},
	ChunkSize:       0,
	DuplicatePolicy: "",
}

// Client is an interface to time series redis commands
type Client struct {
	Pool ConnPool
	Name string
}

const TimeRangeMinimum = 0
const TimeRangeMaximum = math.MaxInt64
const TimeRangeFull = int64(-1)

type Rule struct {
	DestKey       string
	BucketSizeSec int
	AggType       AggregationType
}

type KeyInfo struct {
	TotalSamples       int64
	ChunkCount         int64
	MaxSamplesPerChunk int64 // As of RedisTimeseries >= v1.4 MaxSamplesPerChunk is deprecated in favor of ChunkSize
	ChunkSize          int64
	LastTimestamp      int64
	RetentionTime      int64
	Rules              []Rule
	Labels             map[string]string
	DuplicatePolicy    DuplicatePolicyType // Duplicate sample policy
}

type DataPoint struct {
	Timestamp int64
	Value     float64
}

type Sample struct {
	Key       string
	DataPoint DataPoint
}

func NewDataPoint(timestamp int64, value float64) *DataPoint {
	return &DataPoint{Timestamp: timestamp, Value: value}
}

type Range struct {
	Name       string
	Labels     map[string]string
	DataPoints []DataPoint
}

// Serialize options to args. Given that DUPLICATE_POLICY and ON_DUPLICATE depend upon the issuing command we need to specify the command for which we are generating the args for
func (options *CreateOptions) SerializeSeriesOptions(cmd string, args []interface{}) (result []interface{}, err error) {
	result = args
	if options.DuplicatePolicy != "" {
		if cmd == ADD_CMD {
			result = append(result, "ON_DUPLICATE", string(options.DuplicatePolicy))
		} else {
			result = append(result, "DUPLICATE_POLICY", string(options.DuplicatePolicy))
		}
	}
	return options.Serialize(result)
}

// Serialize options to args
// Deprecated: This function has been deprecated given that DUPLICATE_POLICY and ON_DUPLICATE depend upon the issuing command, use SerializeSeriesOptions instead
func (options *CreateOptions) Serialize(args []interface{}) (result []interface{}, err error) {
	result = args
	if options.Uncompressed {
		result = append(result, "UNCOMPRESSED")
	}
	if options.RetentionMSecs > 0 {
		var value int64
		err, value = formatMilliSec(options.RetentionMSecs)
		if err != nil {
			return
		}
		result = append(result, "RETENTION", value)
	}
	if options.ChunkSize > 0 {
		result = append(result, "CHUNK_SIZE", options.ChunkSize)
	}
	if len(options.Labels) > 0 {
		result = append(result, "LABELS")
		for key, value := range options.Labels {
			result = append(result, key, value)
		}
	}
	return
}

// Helper function to create a string pointer from a string literal.
// Useful for calls to NewClient with an auth pass that is known at compile time.
func MakeStringPtr(s string) *string {
	return &s
}

func floatToStr(inputFloat float64) string {
	return strconv.FormatFloat(inputFloat, 'g', 16, 64)
}

func strToFloat(inputString string) (float64, error) {
	return strconv.ParseFloat(inputString, 64)
}

func formatMilliSec(dur time.Duration) (error error, value int64) {
	if dur > 0 && dur < time.Millisecond {
		error = fmt.Errorf("specified duration is %s, but minimal supported value is %s", dur, time.Millisecond)
		return
	}
	value = int64(dur / time.Millisecond)
	return
}
