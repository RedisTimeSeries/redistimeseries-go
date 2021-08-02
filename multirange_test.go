package redis_timeseries_go

import (
	"reflect"
	"testing"
)

func TestCreateMultiRangeCmdArguments(t *testing.T) {
	type args struct {
		fromTimestamp int64
		toTimestamp   int64
		mrangeOptions MultiRangeOptions
		filters       []string
	}
	tests := []struct {
		name string
		args args
		want []interface{}
	}{
		{"default",
			args{0, 1, DefaultMultiRangeOptions, []string{"labels!="}},
			[]interface{}{"0", "1", "FILTER", "labels!="}},
		{"withlabels",
			args{0, 1, *(NewMultiRangeOptions().SetWithLabels(true)),
				[]string{"labels!="}},
			[]interface{}{"0", "1", "WITHLABELS", "FILTER", "labels!="}},
		{"withlabels and aggregation",
			args{0, 1, *(NewMultiRangeOptions().SetAggregation(AvgAggregation, 60).SetWithLabels(true)),
				[]string{"labels!="}},
			[]interface{}{"0", "1", "AGGREGATION", AvgAggregation, "60", "WITHLABELS", "FILTER", "labels!="}},
		{"withlabels, aggregation and count",
			args{0, 1, *(NewMultiRangeOptions().SetAggregation(AvgAggregation, 60).SetWithLabels(true).SetCount(120)),
				[]string{"labels!="}},
			[]interface{}{"0", "1", "AGGREGATION", AvgAggregation, "60", "COUNT", "120", "WITHLABELS", "FILTER", "labels!="}},
		{"withlabels, aggregation, count, and align",
			args{0, 1, *(NewMultiRangeOptions().SetAggregation(AvgAggregation, 60).SetWithLabels(true).SetCount(120).SetAlign(10)),
				[]string{"labels!="}},
			[]interface{}{"0", "1", "AGGREGATION", AvgAggregation, "60", "COUNT", "120", "WITHLABELS", "ALIGN", "10", "FILTER", "labels!="}},
		{"withlabels, aggregation, count, and align, filter by ts",
			args{0, 1, *(NewMultiRangeOptions().SetAggregation(AvgAggregation, 60).SetWithLabels(true).SetCount(120).SetAlign(10).SetFilterByTs([]int64{10, 11, 12, 13})),
				[]string{"labels!="}},
			[]interface{}{"0", "1", "FILTER_BY_TS", "10", "11", "12", "13", "AGGREGATION", AvgAggregation, "60", "COUNT", "120", "WITHLABELS", "ALIGN", "10", "FILTER", "labels!="}},
		{"withlabels, aggregation, count, and align, filter by value",
			args{0, 1, *(NewMultiRangeOptions().SetAggregation(AvgAggregation, 60).SetWithLabels(true).SetCount(120).SetAlign(10).SetFilterByValue(10, 13)),
				[]string{"labels!="}},
			[]interface{}{"0", "1", "FILTER_BY_VALUE", "10.000000", "13.000000", "AGGREGATION", AvgAggregation, "60", "COUNT", "120", "WITHLABELS", "ALIGN", "10", "FILTER", "labels!="}},
		{"selected_labels, aggregation, count, and align, filter by value",
			args{0, 1, *(NewMultiRangeOptions().SetAggregation(AvgAggregation, 60).SetSelectedLabels([]string{"l1", "l2"}).SetCount(120).SetAlign(10).SetFilterByValue(10, 13)),
				[]string{"labels!="}},
			[]interface{}{"0", "1", "FILTER_BY_VALUE", "10.000000", "13.000000", "AGGREGATION", AvgAggregation, "60", "COUNT", "120", "SELECTED_LABELS", "l1", "l2", "ALIGN", "10", "FILTER", "labels!="}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := createMultiRangeCmdArguments(tt.args.fromTimestamp, tt.args.toTimestamp, tt.args.mrangeOptions, tt.args.filters); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CreateMultiRangeCmdArguments() = %v, want %v", got, tt.want)
			}
		})
	}
}
