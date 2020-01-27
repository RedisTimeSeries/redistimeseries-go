package redis_timeseries_go

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

func createClient() *Client {
	valueh, exists := os.LookupEnv("REDISTIMESERIES_TEST_HOST")
	host := "localhost:6379"
	if exists && valueh != "" {
		host = valueh
	}
	valuep, exists := os.LookupEnv("REDISTIMESERIES_TEST_PASSWORD")
	password := "SUPERSECRET"
	var ptr *string = nil
	if exists {
		password = valuep
	}
	if len(password) > 0 {
		ptr = MakeStringPtr(password)
	}
	return NewClient(host, "test_client", ptr)
}

var client = createClient()
var _ = client.FlushAll()

var defaultDuration, _ = time.ParseDuration("1h")
var tooShortDuration, _ = time.ParseDuration("10ms")

func (client *Client) FlushAll() (err error) {
	conn := client.Pool.Get()
	defer conn.Close()
	_, err = conn.Do("FLUSHALL")
	return err
}

func TestCreateKey(t *testing.T) {
	client.FlushAll()
	err := client.CreateKey("test_CreateKey", defaultDuration)
	assert.Equal(t, nil, err)

	labels := map[string]string{
		"cpu":     "cpu1",
		"country": "IT",
	}
	err = client.CreateKeyWithOptions("test_CreateKeyLabels", CreateOptions{RetentionMSecs: defaultDuration, Labels: labels})
	assert.Equal(t, nil, err)

	err = client.CreateKey("test_CreateKey", tooShortDuration)
	assert.NotNil(t, err)
}

func TestCreateUncompressedKey(t *testing.T) {
	client.FlushAll()
	compressedKey := "test_Compressed"
	uncompressedKey := "test_Uncompressed"
	err := client.CreateKeyWithOptions(compressedKey, CreateOptions{Uncompressed: false})
	assert.Equal(t, nil, err)
	err = client.CreateKeyWithOptions(uncompressedKey, CreateOptions{Uncompressed: true})
	assert.Equal(t, nil, err)
	var i int64 = 0
	for ; i < 1000; i++ {
		client.Add(compressedKey, i, 18.7)
		client.Add(uncompressedKey, i, 18.7)
	}
	CompressedInfo, _ := client.Info(compressedKey)
	UncompressedInfo, _ := client.Info(uncompressedKey)
	assert.True(t, CompressedInfo.ChunkCount == 1)
	assert.True(t, UncompressedInfo.ChunkCount == 4)

	compressedKey = "test_Compressed_Add"
	uncompressedKey = "test_Uncompressed_Add"
	for i = 0; i < 1000; i++ {
		client.AddWithOptions(compressedKey, i, 18.7, CreateOptions{Uncompressed: false})
		client.AddWithOptions(uncompressedKey, i, 18.7, CreateOptions{Uncompressed: true})
	}
	CompressedInfo, _ = client.Info(compressedKey)
	UncompressedInfo, _ = client.Info(uncompressedKey)
	assert.True(t, CompressedInfo.ChunkCount == 1)
	assert.True(t, UncompressedInfo.ChunkCount == 4)
}

func TestCreateRule(t *testing.T) {
	client.FlushAll()
	var destinationKey string
	var err error
	key := "test_CreateRule"
	client.CreateKey(key, defaultDuration)
	var found bool
	for _, aggString := range aggToString {
		destinationKey = string("test_CreateRule_dest" + aggString)
		client.CreateKey(destinationKey, defaultDuration)
		err = client.CreateRule(key, aggString, 100, destinationKey)
		assert.Equal(t, nil, err)
		info, _ := client.Info(key)
		found = false
		for _, rule := range info.Rules {
			if aggString == rule.AggType {
				found = true
			}
		}
		assert.True(t, found)
	}
}

func TestClientInfo(t *testing.T) {
	client.FlushAll()
	key := "test_INFO"
	destKey := "test_INFO_dest"
	client.CreateKey(key, defaultDuration)
	client.CreateKey(destKey, defaultDuration)
	client.CreateRule(key, AvgAggregation, 100, destKey)
	res, err := client.Info(key)
	assert.Equal(t, nil, err)
	expected := KeyInfo{ChunkCount: 1,
		MaxSamplesPerChunk: 256, LastTimestamp: 0, RetentionTime: 3600000,
		Rules:  []Rule{{DestKey: destKey, BucketSizeSec: 100, AggType: AvgAggregation}},
		Labels: map[string]string{},
	}
	assert.Equal(t, expected, res)
}

func TestDeleteRule(t *testing.T) {
	client.FlushAll()
	key := "test_DELETE"
	destKey := "test_DELETE_dest"
	client.CreateKey(key, defaultDuration)
	client.CreateKey(destKey, defaultDuration)
	client.CreateRule(key, AvgAggregation, 100, destKey)
	err := client.DeleteRule(key, destKey)
	assert.Equal(t, nil, err)
	info, _ := client.Info(key)
	assert.Equal(t, 0, len(info.Rules))
	err = client.DeleteRule(key, destKey)
	assert.Equal(t, redis.Error("TSDB: compaction rule does not exist"), err)
}

func TestAdd(t *testing.T) {
	client.FlushAll()
	key := "test_ADD"
	now := time.Now().Unix()
	PI := 3.14159265359
	client.CreateKey(key, defaultDuration)
	storedTimestamp, err := client.Add(key, now, PI)
	assert.Equal(t, nil, err)
	assert.Equal(t, now, storedTimestamp)
	info, _ := client.Info(key)
	assert.Equal(t, now, info.LastTimestamp)

	// Test with auto timestamp
	storedTimestamp1, _ := client.AddAutoTs(key, PI)
	time.Sleep(1 * time.Millisecond)
	storedTimestamp2, _ := client.AddAutoTs(key, PI)
	assert.True(t, storedTimestamp1 < storedTimestamp2)

	// Test with auto timestamp with options
	storedTimestamp1, _ = client.AddAutoTsWithOptions(key, PI, CreateOptions{RetentionMSecs: defaultDuration})
	time.Sleep(1 * time.Millisecond)
	storedTimestamp2, _ = client.AddAutoTsWithOptions(key, PI, CreateOptions{RetentionMSecs: defaultDuration})
	assert.True(t, storedTimestamp1 < storedTimestamp2)
}

func TestAddWithRetention(t *testing.T) {
	client.FlushAll()
	key := "test_ADDWITHRETENTION"
	now := time.Now().Unix()
	PI := 3.14159265359
	client.CreateKey(key, defaultDuration)
	_, err := client.AddWithRetention(key, now, PI, 1000000)
	assert.Equal(t, nil, err)
	info, _ := client.Info(key)
	assert.Equal(t, now, info.LastTimestamp)
}

func TestClient_AggRange(t *testing.T) {
	client.FlushAll()
	key := "test_aggRange"
	client.CreateKey(key, defaultDuration)
	ts1 := int64(1)
	ts2 := int64(10)

	value1 := 5.0
	value2 := 6.0

	client.Add(key, ts1, value1)
	client.Add(key, ts2, value2)

	dataPoints, err := client.AggRange(key, ts1, ts2, CountAggregation, 10)
	assert.Equal(t, nil, err)
	assert.Equal(t, 2.0, dataPoints[0].Value)
}

func TestClient_AggMultiRange(t *testing.T) {
	client.FlushAll()
	key := "test_aggMultiRange1"
	labels := map[string]string{
		"cpu":     "cpu1",
		"country": "US",
	}
	ts1 := int64(1)
	ts2 := int64(2)
	client.AddWithOptions(key, ts1, 5.0, CreateOptions{RetentionMSecs: defaultDuration, Labels: labels})
	client.AddWithOptions(key, ts2, 6.0, CreateOptions{RetentionMSecs: defaultDuration, Labels: labels})

	key2 := "test_aggMultiRange2"
	labels2 := map[string]string{
		"cpu":     "cpu2",
		"country": "US",
	}
	client.CreateKeyWithOptions(key2, CreateOptions{RetentionMSecs: defaultDuration, Labels: labels2})
	client.AddWithOptions(key2, ts1, 4.0, CreateOptions{})
	client.Add(key2, ts2, 8.0)

	ranges, err := client.AggMultiRange(ts1, ts2, CountAggregation, 10, "country=US")
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(ranges))
	assert.Equal(t, 2.0, ranges[0].DataPoints[0].Value)

	_, err = client.AggMultiRange(ts1, ts2, CountAggregation, 10)
	assert.NotNil(t, err)

}

func TestClient_AggMultiRangeWithOptions(t *testing.T) {
	client.FlushAll()
	key := "test_aggMultiRange1"
	labels := map[string]string{
		"cpu":     "cpu1",
		"country": "US",
	}
	ts1 := int64(1)
	ts2 := int64(2)
	client.AddWithOptions(key, ts1, 1, CreateOptions{RetentionMSecs: defaultDuration, Labels: labels})
	client.AddWithOptions(key, ts2, 2, CreateOptions{RetentionMSecs: defaultDuration, Labels: labels})

	key2 := "test_aggMultiRange2"
	labels2 := map[string]string{
		"cpu":     "cpu2",
		"country": "US",
	}
	client.CreateKeyWithOptions(key2, CreateOptions{RetentionMSecs: defaultDuration, Labels: labels2})
	client.AddWithOptions(key2, ts1, 1, CreateOptions{})
	client.Add(key2, ts2, 2)

	ranges, err := client.MultiRangeWithOptions(ts1, ts2, DefaultMultiRangeOptions, "country=US")
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(ranges))
}

func TestClient_Get(t *testing.T) {
	client.FlushAll()
	keyWithData := "test_TestClient_Get_keyWithData"
	keyEmpty := "test_TestClient_Get_Empty_Key"
	noKey := "test_TestClient_Get_dontexist"

	err := client.CreateKeyWithOptions(keyEmpty, DefaultCreateOptions)
	if err != nil {
		t.Errorf("TestClient_Get CreateKeyWithOptions() error = %v", err)
		return
	}

	_, err = client.AddWithOptions(keyWithData, 1, 5.0, DefaultCreateOptions)
	if err != nil {
		t.Errorf("TestClient_Get AddWithOptions() error = %v", err)
		return
	}

	type fields struct {
		Pool ConnPool
		Name string
	}
	type args struct {
		key string
	}
	tests := []struct {
		name          string
		fields        fields
		args          args
		wantDataPoint *DataPoint
		wantErr       bool
	}{
		{"empty key", fields{client.Pool, "test"}, args{keyEmpty}, nil, false},
		{"key with value", fields{client.Pool, "test"}, args{keyWithData}, &DataPoint{1, 5.0}, false},
		{"no key error", fields{client.Pool, "test"}, args{noKey}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &Client{
				Pool: tt.fields.Pool,
				Name: tt.fields.Name,
			}
			gotDataPoint, err := client.Get(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr == false {
				if !reflect.DeepEqual(gotDataPoint, tt.wantDataPoint) && tt.wantErr == false {
					t.Errorf("Get() gotDataPoint = %v, want %v", gotDataPoint, tt.wantDataPoint)
				}
			}
		})
	}
}

func TestClient_MultiGet(t *testing.T) {
	client.FlushAll()
	key1 := "test_TestClient_MultiGet_key1"
	key2 := "test_TestClient_MultiGet_key2"
	labels1 := map[string]string{
		"metric":  "cpu",
		"country": "US",
	}
	labels2 := map[string]string{
		"metric":  "cpu",
		"country": "UK",
	}

	_, err := client.AddWithOptions(key1, 1, 5.0, CreateOptions{Labels: labels1})
	if err != nil {
		t.Errorf("TestClient_MultiGet Add() error = %v", err)
		return
	}
	_, err = client.Add(key1, 2, 15.0)
	_, err = client.Add(key1, 3, 15.0)

	_, err = client.AddWithOptions(key2, 1, 5.0, CreateOptions{Labels: labels2})

	if err != nil {
		t.Errorf("TestClient_MultiGet Add() error = %v", err)
		return
	}

	type fields struct {
		Pool ConnPool
		Name string
	}
	type args struct {
		filters []string
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		wantRanges []Range
		wantErr    bool
	}{
		{"multi key", fields{client.Pool, "test"}, args{[]string{"metric=cpu", "country=UK"}}, []Range{Range{key2, map[string]string{}, []DataPoint{DataPoint{1, 5.0}}}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &Client{
				Pool: tt.fields.Pool,
				Name: tt.fields.Name,
			}
			gotRanges, err := client.MultiGet(tt.args.filters...)
			if (err != nil) != tt.wantErr {
				t.Errorf("MultiGet() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotRanges, tt.wantRanges) {
				t.Errorf("MultiGet() gotRanges = %v, want %v", gotRanges, tt.wantRanges)
			}
		})
	}
}

func TestClient_Range(t *testing.T) {
	client.FlushAll()
	key1 := "TestClient_Range_key1"
	key2 := "TestClient_Range_key2"
	client.CreateKeyWithOptions(key1, DefaultCreateOptions)
	client.CreateKeyWithOptions(key2, DefaultCreateOptions)

	client.Add(key1, 1, 5)
	client.Add(key1, 2, 10)

	type fields struct {
		Pool ConnPool
		Name string
	}
	type args struct {
		key           string
		fromTimestamp int64
		toTimestamp   int64
	}
	tests := []struct {
		name           string
		fields         fields
		args           args
		wantDataPoints []DataPoint
		wantErr        bool
	}{
		{"multi points", fields{client.Pool, "test"}, args{key1, 1, 2}, []DataPoint{{1, 5}, {2, 10}}, false},
		{"empty serie", fields{client.Pool, "test"}, args{key2, 1, 2}, []DataPoint{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &Client{
				Pool: tt.fields.Pool,
				Name: tt.fields.Name,
			}
			gotDataPoints, err := client.Range(tt.args.key, tt.args.fromTimestamp, tt.args.toTimestamp)
			if (err != nil) != tt.wantErr {
				t.Errorf("Range() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Equal(t, gotDataPoints, tt.wantDataPoints)
			if !reflect.DeepEqual((gotDataPoints), tt.wantDataPoints) {
				t.Errorf("Range() gotDataPoints = %v, want %v", (gotDataPoints), tt.wantDataPoints)
			}
		})
	}
}
