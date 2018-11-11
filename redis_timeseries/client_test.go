package redis_timeseries

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var client = NewClient("localhost:6379", "test_client")

func TestClientCreateKey(t *testing.T) {
	oneHour, _ := time.ParseDuration("1h")

	err := client.CreateKey("test_CreateKey", oneHour, 360)
	assert.Equal(t, nil, err)
}

func TestClientInfo(t *testing.T) {
	oneHour, _ := time.ParseDuration("1h")
	key := "test_INFO"
	client.CreateKey(key, oneHour, 360)
	res, err := client.Info("test_INFO")
	assert.Equal(t, nil, err)
	assert.Equal(t, nil, res)


}
