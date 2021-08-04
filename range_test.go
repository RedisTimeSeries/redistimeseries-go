package redis_timeseries_go

import (
	"reflect"
	"testing"
)

func TestCreateRangeCmdArguments(t *testing.T) {
	type args struct {
		key           string
		fromTimestamp int64
		toTimestamp   int64
		rangeOptions  RangeOptions
	}
	tests := []struct {
		name string
		args args
		want []interface{}
	}{
		{"default", args{"key", 0, 1, DefaultRangeOptions}, []interface{}{"key", "0", "1"}},
		{"aggregation",
			args{"key", 0, 1, *NewRangeOptions().SetAggregation(AvgAggregation, 60)},
			[]interface{}{"key", "0", "1", "AGGREGATION", AvgAggregation, "60"}},
		{"aggregation and count",
			args{"key", 0, 1, *NewRangeOptions().SetAggregation(AvgAggregation, 60).SetCount(120)},
			[]interface{}{"key", "0", "1", "AGGREGATION", AvgAggregation, "60", "COUNT", "120"}},
		{"aggregation and align",
			args{"key", 0, 1, *NewRangeOptions().SetAggregation(AvgAggregation, 60).SetCount(120).SetAlign(4)},
			[]interface{}{"key", "0", "1", "AGGREGATION", AvgAggregation, "60", "COUNT", "120", "ALIGN", "4"}},
		{"aggregation and filter by ts",
			args{"key", 0, 1, *NewRangeOptions().SetAggregation(AvgAggregation, 60).SetCount(120).SetFilterByTs([]int64{10, 5, 11})},
			[]interface{}{"key", "0", "1", "FILTER_BY_TS", "10", "5", "11", "AGGREGATION", AvgAggregation, "60", "COUNT", "120"}},
		{"aggregation and filter by value",
			args{"key", 0, 1, *NewRangeOptions().SetAggregation(AvgAggregation, 60).SetCount(120).SetFilterByValue(5.0, 55.0)},
			[]interface{}{"key", "0", "1", "FILTER_BY_VALUE", "5.000000", "55.000000", "AGGREGATION", AvgAggregation, "60", "COUNT", "120"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := createRangeCmdArguments(tt.args.key, tt.args.fromTimestamp, tt.args.toTimestamp, tt.args.rangeOptions); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CreateMultiRangeCmdArguments() = %v, want %v", got, tt.want)
			}
		})
	}
}
