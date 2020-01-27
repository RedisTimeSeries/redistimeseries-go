package redis_timeseries_go

import (
	"reflect"
	"testing"
	"time"
)

func TestCreateOptions_Serialize(t *testing.T) {
	type fields struct {
		Uncompressed   bool
		RetentionMSecs time.Duration
		Labels         map[string]string
	}
	type args struct {
		args []interface{}
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		wantResult []interface{}
		wantErr    bool
	}{
		// TODO: Add test cases.
		{"emtpy", fields{false, 0, map[string]string{}}, args{[]interface{}{}}, []interface{}{}, false},
		{"UNCOMPRESSED", fields{true, 0, map[string]string{}}, args{[]interface{}{}}, []interface{}{"UNCOMPRESSED"}, false},
		{"RETENTION", fields{false, 1000000, map[string]string{}}, args{[]interface{}{}}, []interface{}{"RETENTION", int64(1)}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := &CreateOptions{
				Uncompressed:   tt.fields.Uncompressed,
				RetentionMSecs: tt.fields.RetentionMSecs,
				Labels:         tt.fields.Labels,
			}
			gotResult, err := options.Serialize(tt.args.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Serialize() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotResult, tt.wantResult) {
				t.Errorf("Serialize() gotResult = %v, want %v", gotResult, tt.wantResult)
			}
		})
	}
}

func Test_formatMilliSec(t *testing.T) {
	type args struct {
		dur time.Duration
	}
	tests := []struct {
		name      string
		args      args
		wantErr   bool
		wantValue int64
	}{
		{"force error", args{1}, true, 0},
		{"Millisecond", args{time.Millisecond}, false, 1},
		{"Second", args{time.Second}, false, 1000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err, gotValue := formatMilliSec(tt.args.dur)
			if (err != nil) != tt.wantErr {
				t.Errorf("formatMilliSec() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotValue != tt.wantValue {
				t.Errorf("formatMilliSec() gotValue = %v, want %v", gotValue, tt.wantValue)
			}
		})
	}
}

func Test_strToFloat(t *testing.T) {
	type args struct {
		inputString string
	}
	tests := []struct {
		name    string
		args    args
		want    float64
		wantErr bool
	}{
		{"2.0", args{"2.0"},2.0, false  },
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := strToFloat(tt.args.inputString)
			if (err != nil) != tt.wantErr {
				t.Errorf("strToFloat() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("strToFloat() got = %v, want %v", got, tt.want)
			}
		})
	}
}