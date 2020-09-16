package redis_timeseries_go

import (
	"reflect"
	"testing"
	"time"
)

func TestCreateOptions_Serialize(t *testing.T) {
	type fields struct {
		Uncompressed    bool
		RetentionMSecs  time.Duration
		Labels          map[string]string
		ChunkSize       int64
		DuplicatePolicy DuplicatePolicyType
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
		{"empty", fields{false, 0, map[string]string{}, 0, ""}, args{[]interface{}{}}, []interface{}{}, false},
		{"UNCOMPRESSED", fields{true, 0, map[string]string{}, 0, ""}, args{[]interface{}{}}, []interface{}{"UNCOMPRESSED"}, false},
		{"RETENTION", fields{false, 1000000, map[string]string{}, 0, ""}, args{[]interface{}{}}, []interface{}{"RETENTION", int64(1)}, false},
		{"CHUNK_SIZE", fields{false, 1000000, map[string]string{}, 256, ""}, args{[]interface{}{}}, []interface{}{"RETENTION", int64(1), "CHUNK_SIZE", int64(256)}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := &CreateOptions{
				Uncompressed:    tt.fields.Uncompressed,
				RetentionMSecs:  tt.fields.RetentionMSecs,
				Labels:          tt.fields.Labels,
				ChunkSize:       tt.fields.ChunkSize,
				DuplicatePolicy: tt.fields.DuplicatePolicy,
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
		{"2.0", args{"2.0"}, 2.0, false},
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

func TestCreateOptions_SerializeSeriesOptions(t *testing.T) {
	type fields struct {
		Uncompressed    bool
		RetentionMSecs  time.Duration
		Labels          map[string]string
		ChunkSize       int64
		DuplicatePolicy DuplicatePolicyType
	}
	type args struct {
		cmd  string
		args []interface{}
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		wantResult []interface{}
		wantErr    bool
	}{
		{"DUPLICATE_POLICY BLOCK", fields{false, 1000000, map[string]string{}, 256, BlockDuplicatePolicy}, args{"TS.CREATE", []interface{}{}}, []interface{}{"DUPLICATE_POLICY", "block", "RETENTION", int64(1), "CHUNK_SIZE", int64(256)}, false},
		{"DUPLICATE_POLICY FIRST", fields{false, 1000000, map[string]string{}, 256, FirstDuplicatePolicy}, args{"TS.CREATE", []interface{}{}}, []interface{}{"DUPLICATE_POLICY", "first", "RETENTION", int64(1), "CHUNK_SIZE", int64(256)}, false},
		{"DUPLICATE_POLICY LAST", fields{false, 1000000, map[string]string{}, 256, LastDuplicatePolicy}, args{"TS.CREATE", []interface{}{}}, []interface{}{"DUPLICATE_POLICY", "last", "RETENTION", int64(1), "CHUNK_SIZE", int64(256)}, false},
		{"DUPLICATE_POLICY MIN", fields{false, 1000000, map[string]string{}, 256, MinDuplicatePolicy}, args{"TS.CREATE", []interface{}{}}, []interface{}{"DUPLICATE_POLICY", "min", "RETENTION", int64(1), "CHUNK_SIZE", int64(256)}, false},
		{"DUPLICATE_POLICY MAX", fields{false, 1000000, map[string]string{}, 256, MaxDuplicatePolicy}, args{"TS.CREATE", []interface{}{}}, []interface{}{"DUPLICATE_POLICY", "max", "RETENTION", int64(1), "CHUNK_SIZE", int64(256)}, false},
		{"TS.ADD DUPLICATE_POLICY BLOCK", fields{false, 1000000, map[string]string{}, 256, BlockDuplicatePolicy}, args{"TS.ADD", []interface{}{}}, []interface{}{"ON_DUPLICATE", "block", "RETENTION", int64(1), "CHUNK_SIZE", int64(256)}, false},
		{"TS.ADD DUPLICATE_POLICY FIRST", fields{false, 1000000, map[string]string{}, 256, FirstDuplicatePolicy}, args{"TS.ADD", []interface{}{}}, []interface{}{"ON_DUPLICATE", "first", "RETENTION", int64(1), "CHUNK_SIZE", int64(256)}, false},
		{"TS.ADD DUPLICATE_POLICY LAST", fields{false, 1000000, map[string]string{}, 256, LastDuplicatePolicy}, args{"TS.ADD", []interface{}{}}, []interface{}{"ON_DUPLICATE", "last", "RETENTION", int64(1), "CHUNK_SIZE", int64(256)}, false},
		{"TS.ADD DUPLICATE_POLICY MIN", fields{false, 1000000, map[string]string{}, 256, MinDuplicatePolicy}, args{"TS.ADD", []interface{}{}}, []interface{}{"ON_DUPLICATE", "min", "RETENTION", int64(1), "CHUNK_SIZE", int64(256)}, false},
		{"TS.ADD DUPLICATE_POLICY MAX", fields{false, 1000000, map[string]string{}, 256, MaxDuplicatePolicy}, args{"TS.ADD", []interface{}{}}, []interface{}{"ON_DUPLICATE", "max", "RETENTION", int64(1), "CHUNK_SIZE", int64(256)}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := &CreateOptions{
				Uncompressed:    tt.fields.Uncompressed,
				RetentionMSecs:  tt.fields.RetentionMSecs,
				Labels:          tt.fields.Labels,
				ChunkSize:       tt.fields.ChunkSize,
				DuplicatePolicy: tt.fields.DuplicatePolicy,
			}
			gotResult, err := options.SerializeSeriesOptions(tt.args.cmd, tt.args.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("SerializeSeriesOptions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotResult, tt.wantResult) {
				t.Errorf("SerializeSeriesOptions() gotResult = %v, want %v", gotResult, tt.wantResult)
			}
		})
	}
}
