package redis_timeseries_go

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

func getTestConnectionDetails() (string, string) {
	value, exists := os.LookupEnv("REDISTIMESERIES_TEST_HOST")
	host := "localhost:6379"
	password := ""
	valuePassword, existsPassword := os.LookupEnv("REDISTIMESERIES_TEST_PASSWORD")
	if exists && value != "" {
		host = value
	}
	if existsPassword && valuePassword != "" {
		password = valuePassword
	}
	return host, password
}

func createClient() *Client {
	host, password := getTestConnectionDetails()
	var ptr *string = nil
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
	err := client.FlushAll()
	assert.Nil(t, err)
	err = client.CreateKey("test_CreateKey", defaultDuration)
	assert.Nil(t, err)

	labels := map[string]string{
		"cpu":     "cpu1",
		"country": "IT",
	}
	err = client.CreateKeyWithOptions("test_CreateKeyLabels", CreateOptions{RetentionMSecs: defaultDuration, Labels: labels})
	assert.Nil(t, err)

	err = client.CreateKey("test_CreateKey", tooShortDuration)
	assert.NotNil(t, err)
}

func TestAlterKey(t *testing.T) {
	err := client.FlushAll()
	assert.Nil(t, err)
	labels := map[string]string{
		"cpu":     "cpu1",
		"country": "IT",
	}
	err = client.AlterKeyWithOptions("test_AlterKey", CreateOptions{RetentionMSecs: defaultDuration, Labels: labels})
	assert.NotNil(t, err)
	err = client.CreateKeyWithOptions("test_AlterKey", CreateOptions{RetentionMSecs: defaultDuration, Labels: labels})
	assert.Nil(t, err)
	err = client.AlterKeyWithOptions("test_AlterKey", CreateOptions{RetentionMSecs: defaultDuration, Labels: labels})
	assert.Nil(t, err)
}

func TestQueryIndex(t *testing.T) {
	err := client.FlushAll()
	assert.Nil(t, err)
	labels := map[string]string{
		"sensor_id": "3",
		"area_id":   "32",
	}

	_, err = client.AddWithOptions("test_QueryIndex", 1, 18.7, CreateOptions{Uncompressed: false, Labels: labels})
	assert.Nil(t, err)
	keys, err := client.QueryIndex("sensor_id=3", "area_id=32")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(keys))
	assert.Equal(t, "test_QueryIndex", keys[0])
	keys, err = client.QueryIndex("sensor_id=2")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(keys))
}

func TestCreateUncompressedKey(t *testing.T) {
	err := client.FlushAll()
	assert.Nil(t, err)
	compressedKey := "test_Compressed"
	uncompressedKey := "test_Uncompressed"
	err = client.CreateKeyWithOptions(compressedKey, CreateOptions{Uncompressed: false})
	assert.Nil(t, err)
	err = client.CreateKeyWithOptions(uncompressedKey, CreateOptions{Uncompressed: true})
	assert.Nil(t, err)
	var i int64 = 0
	for ; i < 1000; i++ {
		_, err = client.Add(compressedKey, i, 18.7)
		assert.Nil(t, err)
		_, err = client.Add(uncompressedKey, i, 18.7)
		assert.Nil(t, err)
	}
	CompressedInfo, _ := client.Info(compressedKey)
	UncompressedInfo, _ := client.Info(uncompressedKey)
	assert.True(t, CompressedInfo.ChunkCount == 1)
	assert.True(t, UncompressedInfo.ChunkCount == 4)

	compressedKey = "test_Compressed_Add"
	uncompressedKey = "test_Uncompressed_Add"
	for i = 0; i < 1000; i++ {
		_, err = client.AddWithOptions(compressedKey, i, 18.7, CreateOptions{Uncompressed: false})
		assert.Nil(t, err)
		_, err = client.AddWithOptions(uncompressedKey, i, 18.7, CreateOptions{Uncompressed: true})
		assert.Nil(t, err)
	}
	CompressedInfo, _ = client.Info(compressedKey)
	UncompressedInfo, _ = client.Info(uncompressedKey)
	assert.True(t, CompressedInfo.ChunkCount == 1)
	assert.True(t, UncompressedInfo.ChunkCount == 4)
}

func TestCreateRule(t *testing.T) {
	err := client.FlushAll()
	assert.Nil(t, err)
	var destinationKey string
	key := "test_CreateRule"
	err = client.CreateKey(key, defaultDuration)
	assert.Nil(t, err)
	var found bool
	for _, aggString := range aggToString {
		destinationKey = string("test_CreateRule_dest" + aggString)
		err = client.CreateKey(destinationKey, defaultDuration)
		assert.Nil(t, err)
		err = client.CreateRule(key, aggString, 100, destinationKey)
		assert.Nil(t, err)
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
	err := client.FlushAll()
	assert.Nil(t, err)
	key := "test_INFO"
	destKey := "test_INFO_dest"
	err = client.CreateKey(key, defaultDuration)
	assert.Nil(t, err)
	err = client.CreateKey(destKey, defaultDuration)
	assert.Nil(t, err)
	err = client.CreateRule(key, AvgAggregation, 100, destKey)
	assert.Nil(t, err)
	res, err := client.Info(key)
	assert.Nil(t, err)
	expected := KeyInfo{ChunkCount: 1,
		ChunkSize: 4096, LastTimestamp: 0, RetentionTime: 3600000,
		Rules:  []Rule{{DestKey: destKey, BucketSizeSec: 100, AggType: AvgAggregation}},
		Labels: map[string]string{},
	}
	assert.Equal(t, expected, res)
}

func TestDeleteRule(t *testing.T) {
	err := client.FlushAll()
	assert.Nil(t, err)
	key := "test_DELETE"
	destKey := "test_DELETE_dest"
	err = client.CreateKey(key, defaultDuration)
	assert.Nil(t, err)
	err = client.CreateKey(destKey, defaultDuration)
	assert.Nil(t, err)
	err = client.CreateRule(key, AvgAggregation, 100, destKey)
	assert.Nil(t, err)
	err = client.DeleteRule(key, destKey)
	assert.Nil(t, err)
	info, _ := client.Info(key)
	assert.Equal(t, 0, len(info.Rules))
	err = client.DeleteRule(key, destKey)
	assert.Equal(t, redis.Error("ERR TSDB: compaction rule does not exist"), err)
}

func TestAdd(t *testing.T) {
	err := client.FlushAll()
	assert.Nil(t, err)
	key := "test_ADD"
	now := time.Now().Unix()
	PI := 3.14159265359
	err = client.CreateKey(key, defaultDuration)
	assert.Nil(t, err)
	storedTimestamp, err := client.Add(key, now, PI)
	assert.Nil(t, err)
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
	err := client.FlushAll()
	assert.Nil(t, err)
	key := "test_ADDWITHRETENTION"
	now := time.Now().Unix()
	PI := 3.14159265359
	err = client.CreateKey(key, defaultDuration)
	assert.Nil(t, err)
	_, err = client.AddWithRetention(key, now, PI, 1000000)
	assert.Nil(t, err)
	info, _ := client.Info(key)
	assert.Equal(t, now, info.LastTimestamp)
}

func TestClient_AggRange(t *testing.T) {
	err := client.FlushAll()
	assert.Nil(t, err)
	key := "test_aggRange"
	err = client.CreateKey(key, defaultDuration)
	assert.Nil(t, err)
	ts1 := int64(1)
	ts2 := int64(10)

	value1 := 5.0
	value2 := 6.0

	expectedResponse := []DataPoint{{int64(0), 1.0}, {int64(10), 1.0}}

	_, err = client.Add(key, ts1, value1)
	assert.Nil(t, err)
	_, err = client.Add(key, ts2, value2)
	assert.Nil(t, err)

	dataPoints, err := client.AggRange(key, ts1, ts2, CountAggregation, 10)
	assert.Nil(t, err)
	assert.Equal(t, expectedResponse, dataPoints)

	// ensure zero-based index produces same response
	dataPointsZeroBased, err := client.AggRange(key, 0, ts2, CountAggregation, 10)
	assert.Nil(t, err)
	assert.Equal(t, dataPoints, dataPointsZeroBased)

}

func TestClient_AggMultiRange(t *testing.T) {
	err := client.FlushAll()
	assert.Nil(t, err)
	key := "test_aggMultiRange1"
	labels := map[string]string{
		"cpu":     "cpu1",
		"country": "US",
	}
	ts1 := int64(1)
	ts2 := int64(2)
	_, err = client.AddWithOptions(key, ts1, 5.0, CreateOptions{RetentionMSecs: defaultDuration, Labels: labels})
	assert.Nil(t, err)
	_, err = client.AddWithOptions(key, ts2, 6.0, CreateOptions{RetentionMSecs: defaultDuration, Labels: labels})
	assert.Nil(t, err)
	key2 := "test_aggMultiRange2"
	labels2 := map[string]string{
		"cpu":     "cpu2",
		"country": "US",
	}
	err = client.CreateKeyWithOptions(key2, CreateOptions{RetentionMSecs: defaultDuration, Labels: labels2})
	assert.Nil(t, err)
	_, err = client.AddWithOptions(key2, ts1, 4.0, CreateOptions{})
	assert.Nil(t, err)
	_, err = client.Add(key2, ts2, 8.0)
	assert.Nil(t, err)

	ranges, err := client.AggMultiRange(ts1, ts2, CountAggregation, 10, "country=US")
	assert.Nil(t, err)
	assert.Equal(t, 2, len(ranges))
	assert.Equal(t, 2.0, ranges[0].DataPoints[0].Value)

	_, err = client.AggMultiRange(ts1, ts2, CountAggregation, 10)
	assert.NotNil(t, err)

}

func TestClient_AggMultiRangeWithOptions(t *testing.T) {
	err := client.FlushAll()
	assert.Nil(t, err)
	key := "test_aggMultiRange1"
	labels := map[string]string{
		"cpu":     "cpu1",
		"country": "US",
	}
	ts1 := int64(1)
	ts2 := int64(2)
	_, err = client.AddWithOptions(key, ts1, 1, CreateOptions{RetentionMSecs: defaultDuration, Labels: labels})
	assert.Nil(t, err)
	_, err = client.AddWithOptions(key, ts2, 2, CreateOptions{RetentionMSecs: defaultDuration, Labels: labels})
	assert.Nil(t, err)

	key2 := "test_aggMultiRange2"
	labels2 := map[string]string{
		"cpu":     "cpu2",
		"country": "US",
	}
	err = client.CreateKeyWithOptions(key2, CreateOptions{RetentionMSecs: defaultDuration, Labels: labels2})
	assert.Nil(t, err)
	_, err = client.AddWithOptions(key2, ts1, 1, CreateOptions{})
	assert.Nil(t, err)
	_, err = client.Add(key2, ts2, 2)
	assert.Nil(t, err)

	ranges, err := client.MultiRangeWithOptions(ts1, ts2, DefaultMultiRangeOptions, "country=US")
	assert.Nil(t, err)
	assert.Equal(t, 2, len(ranges))
}

func TestClient_Get(t *testing.T) {
	err := client.FlushAll()
	assert.Nil(t, err)
	keyWithData := "test_TestClient_Get_keyWithData"
	keyEmpty := "test_TestClient_Get_Empty_Key"
	noKey := "test_TestClient_Get_dontexist"

	err = client.CreateKeyWithOptions(keyEmpty, DefaultCreateOptions)
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
	err := client.FlushAll()
	assert.Nil(t, err)
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

	_, err = client.AddWithOptions(key1, 1, 5.0, CreateOptions{Labels: labels1})
	if err != nil {
		t.Errorf("TestClient_MultiGet Add() error = %v", err)
		return
	}
	_, err = client.Add(key1, 2, 15.0)
	assert.Nil(t, err)
	_, err = client.Add(key1, 3, 15.0)
	assert.Nil(t, err)
	_, err = client.AddWithOptions(key2, 1, 5.0, CreateOptions{Labels: labels2})
	assert.Nil(t, err)

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

func TestClient_MultiGetWithOptions(t *testing.T) {
	err := client.FlushAll()
	assert.Nil(t, err)
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

	_, err = client.AddWithOptions(key1, 1, 5.0, CreateOptions{Labels: labels1})
	assert.Nil(t, err)
	_, err = client.Add(key1, 2, 15.0)
	assert.Nil(t, err)
	_, err = client.Add(key1, 3, 15.0)
	assert.Nil(t, err)
	_, err = client.AddWithOptions(key2, 1, 5.0, CreateOptions{Labels: labels2})
	assert.Nil(t, err)

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
		{"multi key", fields{client.Pool, "test"}, args{[]string{"metric=cpu", "country=UK"}}, []Range{Range{key2, map[string]string{"country": "UK", "metric": "cpu"}, []DataPoint{DataPoint{1, 5.0}}}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &Client{
				Pool: tt.fields.Pool,
				Name: tt.fields.Name,
			}
			gotRanges, err := client.MultiGetWithOptions(*NewMultiGetOptions().SetWithLabels(true), tt.args.filters...)
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
	err := client.FlushAll()
	assert.Nil(t, err)
	key1 := "TestClient_Range_key1"
	key2 := "TestClient_Range_key2"
	err = client.CreateKeyWithOptions(key1, DefaultCreateOptions)
	assert.Nil(t, err)
	err = client.CreateKeyWithOptions(key2, DefaultCreateOptions)
	assert.Nil(t, err)

	_, err = client.Add(key1, 1, 5)
	assert.Nil(t, err)
	_, err = client.Add(key1, 2, 10)
	assert.Nil(t, err)
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

func TestNewClientFromPool(t *testing.T) {
	host, password := getTestConnectionDetails()
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}, MaxIdle: maxConns}
	client1 := NewClientFromPool(pool, "cs-client-1")
	client2 := NewClientFromPool(pool, "ts-client-2")
	assert.Equal(t, client1.Pool, client2.Pool)
	err1 := client1.Pool.Close()
	err2 := client2.Pool.Close()
	assert.Nil(t, err1)
	assert.Nil(t, err2)
}
func TestIncrDecrBy(t *testing.T) {
	err := client.FlushAll()
	assert.Nil(t, err)
	labels := map[string]string{
		"sensor_id": "3",
		"area_id":   "32",
	}

	currentTimestamp := time.Now().UnixNano() / 1e6
	timestamp, err := client.IncrBy("Test:IncrDecrBy", currentTimestamp, 13, CreateOptions{Uncompressed: false, Labels: labels})
	assert.Nil(t, err)
	assert.Equal(t, currentTimestamp, timestamp)

	timestamp, err = client.DecrBy("Test:IncrDecrBy", currentTimestamp+1, 14, CreateOptions{Uncompressed: false, Labels: labels})
	assert.Nil(t, err)
	assert.Equal(t, currentTimestamp+1, timestamp)
}

func TestMultiAdd(t *testing.T) {
	err := client.FlushAll()
	assert.Nil(t, err)

	currentTimestamp := time.Now().UnixNano() / 1e6
	_, err = client.AddWithOptions("test:MultiAdd", currentTimestamp, 18.7, CreateOptions{Uncompressed: false})
	assert.Nil(t, err)
	values, err := client.MultiAdd(Sample{Key: "test:MultiAdd", DataPoint: DataPoint{Timestamp: currentTimestamp + 1, Value: 14}},
		Sample{Key: "test:MultiAdd", DataPoint: DataPoint{Timestamp: currentTimestamp + 2, Value: 15}})
	assert.Nil(t, err)
	assert.Equal(t, 2, len(values))
	assert.Equal(t, currentTimestamp+1, values[0])
	assert.Equal(t, currentTimestamp+2, values[1])

	values, err = client.MultiAdd(Sample{Key: "test:MultiAdd", DataPoint: DataPoint{Timestamp: currentTimestamp + 3, Value: 14}},
		Sample{Key: "test:MultiAdd:notExit", DataPoint: DataPoint{Timestamp: currentTimestamp + 4, Value: 14}})
	assert.Nil(t, err)
	assert.Equal(t, 2, len(values))
	assert.Equal(t, currentTimestamp+3, values[0])
	v, ok := values[1].(error)
	assert.NotNil(t, v)
	assert.True(t, ok)

	values, err = client.MultiAdd()
	assert.Nil(t, values)
	assert.Nil(t, err)
}

func TestClient_ReverseRangeWithOptions(t *testing.T) {
	err := client.FlushAll()
	assert.Nil(t, err)
	key1 := "TestClient_RevRange_key1"
	key2 := "TestClient_RevRange_key2"
	err = client.CreateKeyWithOptions(key1, DefaultCreateOptions)
	assert.Nil(t, err)
	err = client.CreateKeyWithOptions(key2, DefaultCreateOptions)
	assert.Nil(t, err)

	_, err = client.Add(key1, 1, 5)
	assert.Nil(t, err)

	_, err = client.Add(key1, 2, 10)
	assert.Nil(t, err)

	type fields struct {
		Pool ConnPool
		Name string
	}
	type args struct {
		key           string
		fromTimestamp int64
		toTimestamp   int64
		rangeOptions  RangeOptions
	}
	tests := []struct {
		name           string
		fields         fields
		args           args
		wantDataPoints []DataPoint
		wantErr        bool
	}{
		{"multi points", fields{client.Pool, "test"}, args{key1, 1, 2, DefaultRangeOptions}, []DataPoint{{2, 10}, {1, 5}}, false},
		{"last point only", fields{client.Pool, "test"}, args{key1, 1, 2, *NewRangeOptions().SetCount(1)}, []DataPoint{{2, 10}}, false},
		{"empty serie", fields{client.Pool, "test"}, args{key2, 1, 2, DefaultRangeOptions}, []DataPoint{}, false},
		{"bad range", fields{client.Pool, "test"}, args{key2, 1, 0, DefaultRangeOptions}, []DataPoint{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &Client{
				Pool: tt.fields.Pool,
				Name: tt.fields.Name,
			}
			gotDataPoints, err := client.ReverseRangeWithOptions(tt.args.key, tt.args.fromTimestamp, tt.args.toTimestamp, tt.args.rangeOptions)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReverseRangeWithOptions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotDataPoints, tt.wantDataPoints) {
				t.Errorf("ReverseRangeWithOptions() gotDataPoints = %v, want %v", gotDataPoints, tt.wantDataPoints)
			}
		})
	}
}

func TestClient_RangeWithOptions(t *testing.T) {
	err := client.FlushAll()
	assert.Nil(t, err)
	key1 := "TestClient_RangeWithOptions_key1"
	key2 := "TestClient_RangeWithOptions_key2"
	err = client.CreateKeyWithOptions(key1, DefaultCreateOptions)
	assert.Nil(t, err)
	err = client.CreateKeyWithOptions(key2, DefaultCreateOptions)
	assert.Nil(t, err)

	_, err = client.Add(key1, 1, 5)
	assert.Nil(t, err)
	_, err = client.Add(key1, 2, 10)
	assert.Nil(t, err)

	type fields struct {
		Pool ConnPool
		Name string
	}
	type args struct {
		key           string
		fromTimestamp int64
		toTimestamp   int64
		rangeOptions  RangeOptions
	}
	tests := []struct {
		name           string
		fields         fields
		args           args
		wantDataPoints []DataPoint
		wantErr        bool
	}{
		{"multi points", fields{client.Pool, "test"}, args{key1, 1, 2, DefaultRangeOptions}, []DataPoint{{1, 5}, {2, 10}}, false},
		{"first point only", fields{client.Pool, "test"}, args{key1, 1, 2, *NewRangeOptions().SetCount(1)}, []DataPoint{{1, 5}}, false},
		{"empty serie", fields{client.Pool, "test"}, args{key2, 1, 2, DefaultRangeOptions}, []DataPoint{}, false},
		{"bad range", fields{client.Pool, "test"}, args{key2, 1, 0, DefaultRangeOptions}, []DataPoint{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &Client{
				Pool: tt.fields.Pool,
				Name: tt.fields.Name,
			}
			gotDataPoints, err := client.RangeWithOptions(tt.args.key, tt.args.fromTimestamp, tt.args.toTimestamp, tt.args.rangeOptions)
			if (err != nil) != tt.wantErr {
				t.Errorf("RangeWithOptions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotDataPoints, tt.wantDataPoints) {
				t.Errorf("RangeWithOptions() gotDataPoints = %v, want %v", gotDataPoints, tt.wantDataPoints)
			}
		})
	}
}

func TestClient_MultiReverseRangeWithOptions(t *testing.T) {
	err := client.FlushAll()
	assert.Nil(t, err)
	key1 := "test_TestClient_MultiReverseRangeWithOptions_key1"
	key2 := "test_TestClient_MultiReverseRangeWithOptions_key2"
	labels1 := map[string]string{
		"metric":  "cpu",
		"country": "US",
	}
	labels2 := map[string]string{
		"metric":  "cpu",
		"country": "UK",
	}

	_, err = client.AddWithOptions(key1, 1, 5.0, CreateOptions{Labels: labels1})
	assert.Nil(t, err)
	_, err = client.Add(key1, 2, 15.0)
	assert.Nil(t, err)
	_, err = client.AddWithOptions(key2, 1, 5.0, CreateOptions{Labels: labels2})
	assert.Nil(t, err)

	type fields struct {
		Pool ConnPool
		Name string
	}
	type args struct {
		fromTimestamp int64
		toTimestamp   int64
		mrangeOptions MultiRangeOptions
		filters       []string
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		wantRanges []Range
		wantErr    bool
	}{
		{"error one matcher", fields{client.Pool, "test"}, args{1, 2, DefaultMultiRangeOptions, []string{}}, nil, true},
		{"last point only single serie", fields{client.Pool, "test"}, args{1, 2, *NewMultiRangeOptions().SetCount(1), []string{"country=UK"}}, []Range{{key2, map[string]string{}, []DataPoint{DataPoint{1, 5.0}}}}, false},
		{"multi series", fields{client.Pool, "test"}, args{1, 2, DefaultMultiRangeOptions, []string{"metric=cpu"}}, []Range{Range{key1, map[string]string{}, []DataPoint{{2, 15.0}, {1, 5.0}}}, {key2, map[string]string{}, []DataPoint{DataPoint{1, 5.0}}}}, false},
		{"last point only multi series", fields{client.Pool, "test"}, args{1, 2, *NewMultiRangeOptions().SetCount(1), []string{"metric=cpu"}}, []Range{Range{key1, map[string]string{}, []DataPoint{{2, 15.0}}}, {key2, map[string]string{}, []DataPoint{DataPoint{1, 5.0}}}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &Client{
				Pool: tt.fields.Pool,
				Name: tt.fields.Name,
			}
			gotRanges, err := client.MultiReverseRangeWithOptions(tt.args.fromTimestamp, tt.args.toTimestamp, tt.args.mrangeOptions, tt.args.filters...)
			if (err != nil) != tt.wantErr {
				t.Errorf("MultiReverseRangeWithOptions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !cmp.Equal(gotRanges, tt.wantRanges) {
				t.Errorf("MultiReverseRangeWithOptions() gotRanges = %v, want %v. Difference: %s", gotRanges, tt.wantRanges, cmp.Diff(gotRanges, tt.wantRanges))
			}
		})
	}
}
