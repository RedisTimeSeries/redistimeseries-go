package redis_timeseries_go

import (
	"reflect"
	"testing"
)

func TestParseLabels(t *testing.T) {
	type args struct {
		res interface{}
	}
	tests := []struct {
		name       string
		args       args
		wantLabels map[string]string
		wantErr    bool
	}{
		{"correctInput",
			args{[]interface{}{[]interface{}{[]byte("hostname"), []byte("host_3")}, []interface{}{[]byte("region"), []byte("us-west-2")}}},
			map[string]string{"hostname": "host_3", "region": "us-west-2"},
			false,
		},
		{"IncorrectInput",
			args{[]interface{}{[]interface{}{[]byte("hostname"), []byte("host_3")}, []interface{}{[]byte("region")}}},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotLabels, err := ParseLabels(tt.args.res)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseLabels() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotLabels, tt.wantLabels) {
				t.Errorf("ParseLabels() gotLabels = %v, want %v", gotLabels, tt.wantLabels)
			}
		})
	}
}

func TestParseRangesSingleDataPoint(t *testing.T) {
	type args struct {
		info interface{}
	}
	tests := []struct {
		name       string
		args       args
		wantRanges []Range
		wantErr    bool
	}{
		{"empty input",
			args{[]interface{}{}},
			[]Range{},
			false,
		},
		{"correct input",
			args{[]interface{}{[]interface{}{[]byte("serie 1"), []interface{}{}, []interface{}{[]byte("1"), []byte("1")}}}},
			[]Range{Range{"serie 1", map[string]string{}, []DataPoint{{1, 1.0}}}},
			false,
		},
		{"incorrect input ( 2 elements on inner array )",
			args{[]interface{}{[]interface{}{[]byte("serie 1"), []interface{}{},}}},
			[]Range{},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotRanges, err := ParseRangesSingleDataPoint(tt.args.info)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseRangesSingleDataPoint() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotRanges, tt.wantRanges) && tt.wantErr == false {
				t.Errorf("ParseRangesSingleDataPoint() gotRanges = %v, want %v", gotRanges, tt.wantRanges)
			}
		})
	}
}

func TestParseDataPoint(t *testing.T) {
	type args struct {
		rawDataPoint interface{}
	}
	tests := []struct {
		name          string
		args          args
		wantDataPoint *DataPoint
		wantErr       bool
	}{
		{"empty input",
			args{[]interface{}{}},
			nil,
			false,
		},
		{"correct input",
			args{[]interface{}{[]byte("1"), []byte("1")}},
			&DataPoint{1, 1.0},
			false,
		},
		{"incorrect input size",
			args{[]interface{}{[]byte("1"), []byte("1"), []byte("1")}},
			&DataPoint{1, 1.0},
			true,
		},
		{"incorrect input value of timestamp",
			args{[]interface{}{[]byte("a"), []byte("1"), []byte("1")}},
			nil,
			true,
		},
		{"incorrect input value of value",
			args{[]interface{}{[]byte("1"), []byte("A"), []byte("1")}},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDataPoint, err := ParseDataPoint(tt.args.rawDataPoint)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDataPoint() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotDataPoint, tt.wantDataPoint) && tt.wantErr == false {
				t.Errorf("ParseDataPoint() gotDataPoint = %v, want %v", gotDataPoint, tt.wantDataPoint)
			}
		})
	}
}
