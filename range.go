package redis_timeseries_go

import (
	"fmt"
	"strconv"
)

// MultiRangeOptions represent the options for querying across multiple time-series
type RangeOptions struct {
	AggType          AggregationType
	TimeBucket       int
	Count            int64
	Align            int64
	FilterByTs       []int64
	FilterByValueMin *float64
	FilterByValueMax *float64
}

func NewRangeOptions() *RangeOptions {
	return &RangeOptions{
		AggType:          "",
		TimeBucket:       -1,
		Count:            -1,
		Align:            -1,
		FilterByTs:       []int64{},
		FilterByValueMin: nil,
		FilterByValueMax: nil,
	}
}

// DefaultRangeOptions are the default options for querying across a time-series range
var DefaultRangeOptions = *NewRangeOptions()

func (rangeopts *RangeOptions) SetCount(count int64) *RangeOptions {
	rangeopts.Count = count
	return rangeopts
}

// Time bucket alignment control for AGGREGATION.
// This will control the time bucket timestamps by changing the reference timestamp on which a bucket is defined.
func (rangeopts *RangeOptions) SetAlign(byTimeStamp int64) *RangeOptions {
	rangeopts.Align = byTimeStamp
	return rangeopts
}

// list of timestamps to filter the result by specific timestamps
func (rangeopts *RangeOptions) SetFilterByTs(filterByTS []int64) *RangeOptions {
	rangeopts.FilterByTs = filterByTS
	return rangeopts
}

// Filter result by value using minimum and maximum ( inclusive )
func (rangeopts *RangeOptions) SetFilterByValue(min, max float64) *RangeOptions {
	rangeopts.FilterByValueMin = &min
	rangeopts.FilterByValueMax = &max
	return rangeopts
}

func (rangeopts *RangeOptions) SetAggregation(aggType AggregationType, timeBucket int) *RangeOptions {
	rangeopts.AggType = aggType
	rangeopts.TimeBucket = timeBucket
	return rangeopts
}

func createRangeCmdArguments(key string, fromTimestamp int64, toTimestamp int64, rangeOptions RangeOptions) []interface{} {
	args := []interface{}{key, strconv.FormatInt(fromTimestamp, 10), strconv.FormatInt(toTimestamp, 10)}
	if rangeOptions.FilterByValueMin != nil {
		args = append(args, "FILTER_BY_VALUE",
			fmt.Sprintf("%f", *rangeOptions.FilterByValueMin),
			fmt.Sprintf("%f", *rangeOptions.FilterByValueMax))
	}
	if len(rangeOptions.FilterByTs) > 0 {
		args = append(args, "FILTER_BY_TS")
		for _, timestamp := range rangeOptions.FilterByTs {
			args = append(args, strconv.FormatInt(timestamp, 10))
		}
	}
	if rangeOptions.AggType != "" {
		args = append(args, "AGGREGATION", rangeOptions.AggType, strconv.Itoa(rangeOptions.TimeBucket))
	}
	if rangeOptions.Count != -1 {
		args = append(args, "COUNT", strconv.FormatInt(rangeOptions.Count, 10))
	}
	if rangeOptions.Align != -1 {
		args = append(args, "ALIGN", strconv.FormatInt(rangeOptions.Align, 10))
	}
	return args
}
