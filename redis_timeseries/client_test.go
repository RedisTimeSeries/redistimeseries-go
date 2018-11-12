package redis_timeseries

import (
	"github.com/stretchr/testify/assert"
	"github.com/garyburd/redigo/redis"
	"testing"
	"time"
)

var client = NewClient("localhost:6379", "test_client")

var defaultDuration, _ = time.ParseDuration("1h")
var defaultMaxSamplesPerChunk uint = 360

func TestCreateKey(t *testing.T) {
	err := client.CreateKey("test_CreateKey", defaultDuration, defaultMaxSamplesPerChunk)
	assert.Equal(t, nil, err)
}

func TestCreateRule(t *testing.T) {
	var destinationKey string
	var err error
	key := "test_CreateRule"
	client.CreateKey(key, defaultDuration, defaultMaxSamplesPerChunk)
	var found bool
	for aggType, aggString := range aggToString {
		destinationKey = "test_CreateRule_dest" + aggString
		client.CreateKey(destinationKey, defaultDuration, defaultMaxSamplesPerChunk)
		err = client.CreateRule(key, aggType, 100, destinationKey)
		assert.Equal(t, nil, err)
		info, _ := client.Info(key)
		found = false
		for _, rule := range info.Rules {
			if aggType == rule.AggType {
				found = true
			}
		}
		assert.True(t, found)
	}
}

func TestClientInfo(t *testing.T) {
	key := "test_INFO"
	destKey := "test_INFO_dest"
	client.CreateKey(key, defaultDuration, defaultMaxSamplesPerChunk)
	client.CreateKey(destKey, defaultDuration, defaultMaxSamplesPerChunk)
	client.CreateRule(key, AvgAggregation, 100, destKey)
	res, err := client.Info(key)
	assert.Equal(t, nil, err)
	expected := KeyInfo{ChunkCount: 1,
		MaxSamplesPerChunk: 360, LastTimestamp: 0, RetentionSecs: 3600,
		Rules: []Rule{{DestKey: destKey, BucketSizeSec: 100, AggType: AvgAggregation}}}
	assert.Equal(t, expected, res)
}

func TestDeleteRule(t *testing.T) {
	key := "test_DELETE"
	destKey := "test_DELETE_dest"
	client.CreateKey(key, defaultDuration, defaultMaxSamplesPerChunk)
	client.CreateKey(destKey, defaultDuration, defaultMaxSamplesPerChunk)
	client.CreateRule(key, AvgAggregation, 100, destKey)
	err := client.DeleteRule(key, destKey)
	assert.Equal(t, nil, err)
	info, _ := client.Info(key)
	assert.Equal(t, 0, len(info.Rules))
	err = client.DeleteRule(key, destKey)
	assert.Equal(t, redis.Error("TSDB: compaction rule does not exist"), err)
}
