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
		{"aggregation", args{"key", 0, 1, *NewRangeOptions().SetAggregation(AvgAggregation, 60)}, []interface{}{"key", "0", "1", "AGGREGATION", AvgAggregation, "60"}},
		{"aggregation and count", args{"key", 0, 1, *NewRangeOptions().SetAggregation(AvgAggregation, 60).SetCount(120)}, []interface{}{"key", "0", "1", "AGGREGATION", AvgAggregation, "60", "COUNT", "120"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := createRangeCmdArguments(tt.args.key, tt.args.fromTimestamp, tt.args.toTimestamp, tt.args.rangeOptions); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CreateMultiRangeCmdArguments() = %v, want %v", got, tt.want)
			}
		})
	}
}
