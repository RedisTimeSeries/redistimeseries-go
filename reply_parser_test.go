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
			args{[]interface{}{[]byte("a a"), []byte("1"), []byte("1")}},
			nil,
			true,
		},
		{"incorrect input value of value",
			args{[]interface{}{[]byte("1"), []byte("A A"), []byte("1")}},
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

func TestParseDataPoints(t *testing.T) {
	type args struct {
		info interface{}
	}
	tests := []struct {
		name           string
		args           args
		wantDataPoints []DataPoint
		wantErr        bool
	}{
		{"empty input",
			args{[]interface{}{}},
			[]DataPoint{},
			false,
		},
		{"correct input one datapoints",
			args{[]interface{}{[]interface{}{[]byte("1"), []byte("1")}}},
			[]DataPoint{{1, 1.0}},
			false,
		},
		{"correct input two datapoints",
			args{[]interface{}{[]interface{}{[]byte("1"), []byte("1")}, []interface{}{[]byte("2"), []byte("2")}}},
			[]DataPoint{{1, 1.0}, {2, 2.0}},
			false,
		},
		{"incorrect input Nan on timestamp",
			args{[]interface{}{[]interface{}{[]byte("A"), []byte("1")}, []interface{}{[]byte("2"), []byte("2")}}},
			[]DataPoint{},
			true,
		},
		{"incorrect input Nan on value",
			args{[]interface{}{[]interface{}{[]byte("1"), []byte("A")}, []interface{}{[]byte("2"), []byte("2")}}},
			[]DataPoint{},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDataPoints, err := ParseDataPoints(tt.args.info)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDataPoints() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotDataPoints, tt.wantDataPoints) {
				t.Errorf("ParseDataPoints() gotDataPoints = %v, want %v", gotDataPoints, tt.wantDataPoints)
			}
		})
	}
}

func TestParseRanges(t *testing.T) {
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
			args{[]interface{}{[]interface{}{[]byte("serie 1"), []interface{}{}, []interface{}{[]interface{}{[]byte("1"), []byte("1")}}}}},
			[]Range{Range{"serie 1", map[string]string{}, []DataPoint{{1, 1.0}}}},
			false,
		},
		{"incorrect input ( 2 elements on inner array )",
			args{[]interface{}{[]interface{}{[]byte("serie 1"), []interface{}{},}}},
			[]Range{},
			true,
		},
		{"incorrect input ( bad datapoint timestamp )",
			args{[]interface{}{[]interface{}{[]byte("serie 1"), []interface{}{}, []interface{}{[]interface{}{[]byte("AA"), []byte("1")}}}}},
			[]Range{Range{"serie 1", map[string]string{}, []DataPoint{}}},
			true,
		},
		{"incorrect input ( bad datapoint value )",
			args{[]interface{}{[]interface{}{[]byte("serie 1"), []interface{}{}, []interface{}{[]interface{}{[]byte("1"), []byte("AA")}}}}},
			[]Range{Range{"serie 1", map[string]string{}, []DataPoint{}}},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotRanges, err := ParseRanges(tt.args.info)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseRanges() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotRanges, tt.wantRanges) && tt.wantErr == false {
				t.Errorf("ParseRanges() gotRanges = %v, want %v", gotRanges, tt.wantRanges)
			}
		})
	}
}
