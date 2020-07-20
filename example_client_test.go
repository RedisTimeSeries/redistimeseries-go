package redis_timeseries_go_test

import (
	"fmt"
	redistimeseries "github.com/RedisTimeSeries/redistimeseries-go"
	"github.com/gomodule/redigo/redis"
)

// exemplifies the NewClientFromPool function
func ExampleNewClientFromPool() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client-1")
	client.Add("ts", 1, 5)
	datapoints, _ := client.RangeWithOptions("ts", 0, 1000, redistimeseries.DefaultRangeOptions)
	fmt.Println(datapoints[0])
	// Output: {1 5}
}

// Exemplifies the usage of RangeWithOptions function
func ExampleRangeWithOptions() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client-1")
	for ts := 1; ts < 10; ts++ {
		client.Add("ts", int64(ts), float64(ts))
	}

	datapoints, _ := client.RangeWithOptions("ts", 0, 1000, redistimeseries.DefaultRangeOptions)
	fmt.Println(fmt.Sprintf("Datapoints: %v", datapoints))
	// Output:
	// Datapoints: [{1 1} {2 2} {3 3} {4 4} {5 5} {6 6} {7 7} {8 8} {9 9}]
}

// Exemplifies the usage of MultiRangeWithOptions function.
func ExampleMultiRangeWithOptions() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client-1")

	labels := map[string]string{
		"machine": "machine-1",
		"az":      "us-east-1",
	}
	client.AddWithOptions("time-serie-1", 2, 1.0, redistimeseries.CreateOptions{RetentionMSecs: 0, Labels: labels})
	client.Add("time-serie-1", 4, 2.0)

	labels2 := map[string]string{
		"machine": "machine-2",
		"az":      "us-east-1",
	}
	client.AddWithOptions("time-serie-2", 1, 5.0, redistimeseries.CreateOptions{RetentionMSecs: 0, Labels: labels2})
	client.Add("time-serie-1", 4, 2.0)

	ranges, _ := client.MultiRangeWithOptions(1, 10, redistimeseries.DefaultMultiRangeOptions, "az=us-east-1")

	fmt.Println(fmt.Sprintf("Ranges: %v", ranges))
	// Output:
	// Ranges: [{time-serie-1 map[] [{2 1} {4 2}]} {time-serie-2 map[] [{1 5}]}]
}

// Exemplifies the usage of MultiGetWithOptions function while using the default MultiGetOptions and while using user defined MultiGetOptions.
func ExampleMultiGetWithOptions() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client-1")

	labels := map[string]string{
		"machine": "machine-1",
		"az":      "us-east-1",
	}
	client.AddWithOptions("time-serie-1", 2, 1.0, redistimeseries.CreateOptions{RetentionMSecs: 0, Labels: labels})
	client.Add("time-serie-1", 4, 2.0)

	labels2 := map[string]string{
		"machine": "machine-2",
		"az":      "us-east-1",
	}
	client.AddWithOptions("time-serie-2", 1, 5.0, redistimeseries.CreateOptions{RetentionMSecs: 0, Labels: labels2})
	client.Add("time-serie-1", 4, 2.0)

	ranges, _ := client.MultiGetWithOptions(redistimeseries.DefaultMultiGetOptions, "az=us-east-1")

	rangesWithLabels, _ := client.MultiGetWithOptions(*redistimeseries.NewMultiGetOptions().SetWithLabels(true), "az=us-east-1")

	fmt.Println(fmt.Sprintf("Ranges: %v", ranges))
	fmt.Println(fmt.Sprintf("Ranges with labels: %v", rangesWithLabels))

	// Output:
	// Ranges: [{time-serie-1 map[] [{4 2}]} {time-serie-2 map[] [{1 5}]}]
	// Ranges with labels: [{time-serie-1 map[az:us-east-1 machine:machine-1] [{4 2}]} {time-serie-2 map[az:us-east-1 machine:machine-2] [{1 5}]}]
}
