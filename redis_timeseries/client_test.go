package redis_timeseries

import (
	"testing"
	"time"
)


func TestClientCreateKey(*testing.T){
	client := NewClient("localhost", "test_client")
	oneHour, _ := time.ParseDuration("1h")
	client.CreateKey("test_key", oneHour, 100)
}
