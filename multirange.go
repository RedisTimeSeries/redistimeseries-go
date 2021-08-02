package redis_timeseries_go

import (
	"fmt"
	"strconv"
)

// MultiRangeOptions represent the options for querying across multiple time-series
type MultiRangeOptions struct {
	AggType          AggregationType
	TimeBucket       int
	Count            int64
	WithLabels       bool
	SelectedLabels   []string
	Align            int64
	FilterByTs       []int64
	FilterByValueMin *float64
	FilterByValueMax *float64
}

// MultiRangeOptions are the default options for querying across multiple time-series
var DefaultMultiRangeOptions = MultiRangeOptions{
	AggType:          "",
	TimeBucket:       -1,
	Count:            -1,
	WithLabels:       false,
	SelectedLabels:   []string{},
	Align:            -1,
	FilterByTs:       []int64{},
	FilterByValueMin: nil,
	FilterByValueMax: nil,
}

func NewMultiRangeOptions() *MultiRangeOptions {
	return &MultiRangeOptions{
		AggType:          "",
		TimeBucket:       -1,
		Count:            -1,
		WithLabels:       false,
		SelectedLabels:   []string{},
		Align:            -1,
		FilterByTs:       []int64{},
		FilterByValueMin: nil,
		FilterByValueMax: nil,
	}
}

// SetAlign sets the time bucket alignment control for AGGREGATION.
// This will control the time bucket timestamps by changing the reference timestamp on which a bucket is defined.
func (mrangeopts *MultiRangeOptions) SetAlign(byTimeStamp int64) *MultiRangeOptions {
	mrangeopts.Align = byTimeStamp
	return mrangeopts
}

// SetFilterByTs sets the list of timestamps to filter the result by specific timestamps
func (mrangeopts *MultiRangeOptions) SetFilterByTs(filterByTS []int64) *MultiRangeOptions {
	mrangeopts.FilterByTs = filterByTS
	return mrangeopts
}

// SetFilterByValue filters the result by value using minimum and maximum ( inclusive )
func (mrangeopts *MultiRangeOptions) SetFilterByValue(min, max float64) *MultiRangeOptions {
	mrangeopts.FilterByValueMin = &min
	mrangeopts.FilterByValueMax = &max
	return mrangeopts
}

func (mrangeopts *MultiRangeOptions) SetCount(count int64) *MultiRangeOptions {
	mrangeopts.Count = count
	return mrangeopts
}

func (mrangeopts *MultiRangeOptions) SetAggregation(aggType AggregationType, timeBucket int) *MultiRangeOptions {
	mrangeopts.AggType = aggType
	mrangeopts.TimeBucket = timeBucket
	return mrangeopts
}

func (mrangeopts *MultiRangeOptions) SetWithLabels(value bool) *MultiRangeOptions {
	mrangeopts.WithLabels = value
	return mrangeopts
}

// SetSelectedLabels limits the series reply labels to provided label names
func (mrangeopts *MultiRangeOptions) SetSelectedLabels(labels []string) *MultiRangeOptions {
	mrangeopts.SelectedLabels = labels
	return mrangeopts
}

func createMultiRangeCmdArguments(fromTimestamp int64, toTimestamp int64, mrangeOptions MultiRangeOptions, filters []string) []interface{} {
	args := []interface{}{strconv.FormatInt(fromTimestamp, 10), strconv.FormatInt(toTimestamp, 10)}
	if mrangeOptions.FilterByValueMin != nil {
		args = append(args, "FILTER_BY_VALUE",
			fmt.Sprintf("%f", *mrangeOptions.FilterByValueMin),
			fmt.Sprintf("%f", *mrangeOptions.FilterByValueMax))
	}
	if len(mrangeOptions.FilterByTs) > 0 {
		args = append(args, "FILTER_BY_TS")
		for _, timestamp := range mrangeOptions.FilterByTs {
			args = append(args, strconv.FormatInt(timestamp, 10))
		}
	}
	if mrangeOptions.AggType != "" {
		args = append(args, "AGGREGATION", mrangeOptions.AggType, strconv.Itoa(mrangeOptions.TimeBucket))
	}
	if mrangeOptions.Count != -1 {
		args = append(args, "COUNT", strconv.FormatInt(mrangeOptions.Count, 10))
	}
	if mrangeOptions.WithLabels {
		args = append(args, "WITHLABELS")
	} else if len(mrangeOptions.SelectedLabels) > 0 {
		args = append(args, "SELECTED_LABELS")
		for _, label := range mrangeOptions.SelectedLabels {
			args = append(args, label)
		}
	}
	if mrangeOptions.Align != -1 {
		args = append(args, "ALIGN", strconv.FormatInt(mrangeOptions.Align, 10))
	}
	args = append(args, "FILTER")
	for _, filter := range filters {
		args = append(args, filter)
	}
	return args
}
