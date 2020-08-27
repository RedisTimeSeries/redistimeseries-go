package redis_timeseries_go

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

//go:generate stringer -type=AggregationType
type AggregationType string

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

var aggToString = []AggregationType{AvgAggregation, SumAggregation, MinAggregation, MaxAggregation, CountAggregation, FirstAggregation, LastAggregation, StdPAggregation, StdSAggregation, VarPAggregation, VarSAggregation}

type CreateOptions struct {
	Uncompressed   bool
	RetentionMSecs time.Duration
	Labels         map[string]string
}

var DefaultCreateOptions = CreateOptions{
	Uncompressed:   false,
	RetentionMSecs: 0,
	Labels:         map[string]string{},
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
	ChunkCount         int64
	MaxSamplesPerChunk int64 // As of RedisTimeseries >= v1.4 MaxSamplesPerChunk is deprecated in favor of ChunkSize
	ChunkSize          int64
	LastTimestamp      int64
	RetentionTime      int64
	Rules              []Rule
	Labels             map[string]string
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

// Serialize options to args
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
