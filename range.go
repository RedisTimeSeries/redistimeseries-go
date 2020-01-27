package redis_timeseries_go

import (
	"strconv"
)

// MultiRangeOptions represent the options for querying across multiple time-series
type RangeOptions struct {
	AggType    AggregationType
	TimeBucket int
	Count      int64
}

func NewRangeOptions() *RangeOptions {
	return &RangeOptions{
		AggType:    "",
		TimeBucket: -1,
		Count:      -1,
	}
}

// DefaultRangeOptions are the default options for querying across a time-series range
var DefaultRangeOptions = *NewRangeOptions()

func (rangeopts *RangeOptions) SetCount(count int64) *RangeOptions {
	rangeopts.Count = count
	return rangeopts
}

func (rangeopts *RangeOptions) SetAggregation(aggType AggregationType, timeBucket int) *RangeOptions {
	rangeopts.AggType = aggType
	rangeopts.TimeBucket = timeBucket
	return rangeopts
}

func createRangeCmdArguments(key string, fromTimestamp int64, toTimestamp int64, rangeOptions RangeOptions) []interface{} {
	args := []interface{}{key, strconv.FormatInt(fromTimestamp, 10), strconv.FormatInt(toTimestamp, 10)}
	if rangeOptions.AggType != "" {
		args = append(args, "AGGREGATION", rangeOptions.AggType, strconv.Itoa(rangeOptions.TimeBucket))
	}
	if rangeOptions.Count != -1 {
		args = append(args, "COUNT", strconv.FormatInt(rangeOptions.Count, 10))
	}
	return args
}
