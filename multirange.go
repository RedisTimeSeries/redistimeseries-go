package redis_timeseries_go

import "strconv"

// MultiRangeOptions represent the options for querying across multiple time-series
type MultiRangeOptions struct {
	AggType    AggregationType
	TimeBucket int
	Count      int64
	WithLabels bool
}

// MultiRangeOptions are the default options for querying across multiple time-series
var DefaultMultiRangeOptions = MultiRangeOptions{
	AggType:    "",
	TimeBucket: -1,
	Count:      -1,
	WithLabels: false,
}

func NewMultiRangeOptions() *MultiRangeOptions {
	return &MultiRangeOptions{
		AggType:    "",
		TimeBucket: -1,
		Count:      -1,
		WithLabels: false,
	}
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

func createMultiRangeCmdArguments(fromTimestamp int64, toTimestamp int64, mrangeOptions MultiRangeOptions, filters []string) []interface{} {
	args := []interface{}{strconv.FormatInt(fromTimestamp, 10), strconv.FormatInt(toTimestamp, 10)}
	if mrangeOptions.AggType != "" {
		args = append(args, "AGGREGATION", mrangeOptions.AggType, strconv.Itoa(mrangeOptions.TimeBucket))
	}
	if mrangeOptions.Count != -1 {
		args = append(args, "COUNT", strconv.FormatInt(mrangeOptions.Count, 10))
	}
	if mrangeOptions.WithLabels == true {
		args = append(args, "WITHLABELS")
	}
	args = append(args, "FILTER")
	for _, filter := range filters {
		args = append(args, filter)
	}
	return args
}
