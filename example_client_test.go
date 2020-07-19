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

// Exemplifies the usage of RangeWithOptions function while filtering the results.
// To filter the results we use redistimeseries.FilterDataPoints that returns a new slice
// containing all datapoints in the slice that satisfy the predicate f.
func ExampleRangeWithOptions_FilterDataPoints() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client-1")
	for ts := 1; ts < 10; ts++ {
		client.Add("ts", int64(ts), float64(ts))
	}

	filterLower := func(d redistimeseries.DataPoint) bool {
		return d.Value > 5
	}

	datapoints, _ := client.RangeWithOptions("ts", 0, 1000, redistimeseries.DefaultRangeOptions)
	filteredDatapoints := redistimeseries.FilterDataPoints(datapoints, filterLower)
	fmt.Println(fmt.Sprintf("Datapoints: %v", datapoints))
	fmt.Println(fmt.Sprintf("Filtered Datapoints: %v", filteredDatapoints))
	// Output:
	// Datapoints: [{1 1} {2 2} {3 3} {4 4} {5 5} {6 6} {7 7} {8 8} {9 9}]
	// Filtered Datapoints: [{6 6} {7 7} {8 8} {9 9}]
}

// Exemplifies the usage of RangeWithOptions function while applying a function f (power) to each datapoint in the original slice.
func ExampleRangeWithOptions_MapDataPoints() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client-1")
	for ts := 1; ts < 10; ts++ {
		client.Add("ts", int64(ts), float64(ts))
	}

	mapPow := func(d redistimeseries.DataPoint) redistimeseries.DataPoint {
		d.Value = d.Value * d.Value
		return d
	}

	datapoints, _ := client.RangeWithOptions("ts", 0, 1000, redistimeseries.DefaultRangeOptions)
	mapDatapoints := redistimeseries.MapDataPoints(datapoints, mapPow)
	fmt.Println(fmt.Sprintf("Datapoints: %v", datapoints))
	fmt.Println(fmt.Sprintf("Datapoints with map applied: %v", mapDatapoints))
	// Output:
	// Datapoints: [{1 1} {2 2} {3 3} {4 4} {5 5} {6 6} {7 7} {8 8} {9 9}]
	// Datapoints with map applied: [{1 1} {2 4} {3 9} {4 16} {5 25} {6 36} {7 49} {8 64} {9 81}]
}

// Exemplifies the usage of MultiRangeWithOptions function while filtering the ranges results.
// To filter the results we use redistimeseries.FilterRanges that returns a new slice
// containing all ranges with the filtered datapoints that satisfy the predicate f.
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

// Exemplifies the usage of MultiRangeWithOptions function while filtering the ranges results.
// To filter the results we use redistimeseries.FilterRanges that returns a new slice
// containing all ranges with the filtered datapoints that satisfy the predicate f.
func ExampleMultiRangeWithOptions_FilterRanges() {
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

	filterLower := func(d redistimeseries.DataPoint) bool {
		return d.Value >= 5
	}

	filteredRanges := redistimeseries.FilterRanges(ranges, filterLower)
	fmt.Println(fmt.Sprintf("Ranges: %v", ranges))
	fmt.Println(fmt.Sprintf("Filtered Ranges: %v", filteredRanges))
	// Output:
	// Ranges: [{time-serie-1 map[] [{2 1} {4 2}]} {time-serie-2 map[] [{1 5}]}]
	// Filtered Ranges: [{time-serie-1 map[] []} {time-serie-2 map[] [{1 5}]}]
}

// Exemplifies the usage of MultiRangeWithOptions function while applying a function f (power) to each datapoint in the original slices.
func ExampleMultiRangeWithOptions_MapRanges() {
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

	mapPow := func(d redistimeseries.DataPoint) redistimeseries.DataPoint {
		d.Value = d.Value * d.Value
		return d
	}

	ranges, _ := client.MultiRangeWithOptions(1, 10, redistimeseries.DefaultMultiRangeOptions, "az=us-east-1")

	mapRanges := redistimeseries.MapRanges(ranges, mapPow)
	fmt.Println(fmt.Sprintf("Ranges: %v", ranges))
	fmt.Println(fmt.Sprintf("Ranges with map applied: %v", mapRanges))
	// Output:
	// Ranges: [{time-serie-1 map[] [{2 1} {4 2}]} {time-serie-2 map[] [{1 5}]}]
	// Ranges with map applied: [{time-serie-1 map[] [{2 1} {4 4}]} {time-serie-2 map[] [{1 25}]}]
}
