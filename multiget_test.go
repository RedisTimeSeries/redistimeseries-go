package redis_timeseries_go

import (
	"reflect"
	"testing"
)

func Test_createMultiGetCmdArguments(t *testing.T) {
	type args struct {
		mgetOptions MultiGetOptions
		filters     []string
	}
	tests := []struct {
		name string
		args args
		want []interface{}
	}{
		{"default", args{DefaultMultiGetOptions, []string{"labels!="}}, []interface{}{"FILTER", "labels!="}},
		{"withlabels", args{*(NewMultiGetOptions().SetWithLabels(true)), []string{"labels!="}}, []interface{}{"WITHLABELS", "FILTER", "labels!="}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := createMultiGetCmdArguments(tt.args.mgetOptions, tt.args.filters); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("createMultiGetCmdArguments() = %v, want %v", got, tt.want)
			}
		})
	}
}
