package redis_timeseries_go

// MultiGetOptions represent the options for querying across multiple time-series
type MultiGetOptions struct {
	WithLabels bool
}

// MultiGetOptions are the default options for querying across multiple time-series
var DefaultMultiGetOptions = MultiGetOptions{
	WithLabels: false,
}

func NewMultiGetOptions() *MultiGetOptions {
	return &MultiGetOptions{
		WithLabels: false,
	}
}

func (mgetopts *MultiGetOptions) SetWithLabels(value bool) *MultiGetOptions {
	mgetopts.WithLabels = value
	return mgetopts
}

func createMultiGetCmdArguments(mgetOptions MultiGetOptions, filters []string) []interface{} {
	args := []interface{}{}
	if mgetOptions.WithLabels == true {
		args = append(args, "WITHLABELS")
	}
	args = append(args, "FILTER")
	for _, filter := range filters {
		args = append(args, filter)
	}
	return args
}
