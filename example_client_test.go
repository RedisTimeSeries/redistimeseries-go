package redis_timeseries_go_test

import (
	"fmt"
	redistimeseries "github.com/RedisTimeSeries/redistimeseries-go"
	"github.com/gomodule/redigo/redis"
	"log"
	"time"
)

// exemplifies the NewClientFromPool function
//nolint:errcheck
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

// Exemplifies the usage of CreateKeyWithOptions function with a duplicate policy of LAST (override with latest value)
// nolint:errcheck
func ExampleClient_CreateKeyWithOptions() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client-1")

	client.CreateKeyWithOptions("time-serie-last-policy", redistimeseries.CreateOptions{DuplicatePolicy: redistimeseries.LastDuplicatePolicy})

	// Add duplicate timestamp just to ensure it obeys the duplicate policy
	client.Add("time-serie-last-policy", 4, 2.0)
	client.Add("time-serie-last-policy", 4, 10.0)

	// Retrieve the latest data point
	latestDatapoint, _ := client.Get("time-serie-last-policy")

	fmt.Printf("Latest datapoint: timestamp=%d value=%f\n", latestDatapoint.Timestamp, latestDatapoint.Value)
	// Output:
	// Latest datapoint: timestamp=4 value=10.000000
}

// Exemplifies the usage of CreateKeyWithOptions function with a retention time of 1 hour
// nolint:errcheck
func ExampleClient_CreateKeyWithOptions_retentionTime() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client")

	// get the default options and set the retention time
	options := redistimeseries.DefaultCreateOptions
	options.RetentionMSecs = time.Hour

	client.CreateKeyWithOptions("time-series-example-retention-time", options)

	client.Add("time-series-example-retention-time", 1, 1)
	client.Add("time-series-example-retention-time", 2, 2)

	// Retrieve the latest data point
	latestDatapoint, _ := client.Get("time-series-example-retention-time")

	fmt.Printf("Latest datapoint: timestamp=%d value=%f\n", latestDatapoint.Timestamp, latestDatapoint.Value)
	// Output:
	// Latest datapoint: timestamp=2 value=2.000000
}

// Exemplifies the usage of Add function with a time-series created with the default options
// nolint:errcheck
func ExampleClient_Add() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client-1")

	labels := map[string]string{
		"machine": "machine-1",
		"az":      "us-west-2",
	}
	// get the default options and set the time-serie labels
	options := redistimeseries.DefaultCreateOptions
	options.Labels = labels

	client.CreateKeyWithOptions("time-serie-add", options)

	client.Add("time-serie-add", 1, 2.0)
	client.Add("time-serie-add", 2, 4.0)

	// Retrieve the latest data point
	latestDatapoint, _ := client.Get("time-serie-add")

	fmt.Printf("Latest datapoint: timestamp=%d value=%f\n", latestDatapoint.Timestamp, latestDatapoint.Value)
	// Output:
	// Latest datapoint: timestamp=2 value=4.000000
}

// Exemplifies the usage of Add function for back filling - Add samples to a time series where the time of the sample is older than the newest sample in the series
// nolint:errcheck
func ExampleClient_Add_backFilling() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client-1")

	// get the default options and set the time-serie labels
	options := redistimeseries.DefaultCreateOptions

	client.CreateKeyWithOptions("time-serie-add-back-filling", options)

	client.Add("time-serie-add-back-filling", 1, 1)
	client.Add("time-serie-add-back-filling", 2, 1)
	client.Add("time-serie-add-back-filling", 4, 1)
	// Add sample with timestamp ( 3 ) where the time of the sample is older than the newest sample in the series ( 4 )
	client.Add("time-serie-add-back-filling", 3, 1)

	// Retrieve the time-series data points
	datapoints, _ := client.RangeWithOptions("time-serie-add-back-filling", 0, 1000, redistimeseries.DefaultRangeOptions)
	fmt.Printf("Datapoints: %v\n", datapoints)
	// Output:
	// Datapoints: [{1 1} {2 1} {3 1} {4 1}]
}

// Exemplifies the usage of Add function with a duplicate policy of LAST (override with latest value)
// nolint:errcheck
func ExampleClient_Add_duplicateDatapointsLastDuplicatePolicy() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client-1")

	// get the default options and set the duplicate policy to LAST (override with latest value)
	options := redistimeseries.DefaultCreateOptions
	options.DuplicatePolicy = redistimeseries.LastDuplicatePolicy

	client.CreateKeyWithOptions("time-series-add-duplicate-last", options)

	client.Add("time-series-add-duplicate-last", 1, 1.0)
	client.Add("time-series-add-duplicate-last", 1, 10.0)

	// Retrieve the latest data point
	latestDatapoint, _ := client.Get("time-series-add-duplicate-last")

	fmt.Printf("Latest datapoint: timestamp=%d value=%f\n", latestDatapoint.Timestamp, latestDatapoint.Value)
	// Output:
	// Latest datapoint: timestamp=1 value=10.000000
}

// Exemplifies the usage of AddWithOptions function with the default options and some additional time-serie labels
// nolint:errcheck
func ExampleClient_AddWithOptions() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client")

	labels := map[string]string{
		"machine": "machine-1",
		"az":      "us-west-2",
	}
	// get the default options and set the time-serie labels
	options := redistimeseries.DefaultCreateOptions
	options.Labels = labels

	client.AddWithOptions("time-series-example-add", 1, 1, options)
	client.AddWithOptions("time-series-example-add", 2, 2, options)

	// Retrieve the latest data point
	latestDatapoint, _ := client.Get("time-series-example-add")

	fmt.Printf("Latest datapoint: timestamp=%d value=%f\n", latestDatapoint.Timestamp, latestDatapoint.Value)
	// Output:
	// Latest datapoint: timestamp=2 value=2.000000
}

// Exemplifies the usage of AddWithOptions function with a duplicate policy of LAST (override with latest value)
// nolint:errcheck
func ExampleClient_AddWithOptions_duplicateDatapointsLastDuplicatePolicy() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client")

	labels := map[string]string{
		"machine": "machine-1",
		"az":      "us-west-2",
	}

	// get the default options and set the duplicate policy to LAST (override with latest value)
	options := redistimeseries.DefaultCreateOptions
	options.DuplicatePolicy = redistimeseries.LastDuplicatePolicy
	options.Labels = labels

	client.AddWithOptions("time-series-example-duplicate-last", 1, 1, options)
	client.AddWithOptions("time-series-example-duplicate-last", 1, 10, options)

	// Retrieve the latest data point
	latestDatapoint, _ := client.Get("time-series-example-duplicate-last")

	fmt.Printf("Latest datapoint: timestamp=%d value=%f\n", latestDatapoint.Timestamp, latestDatapoint.Value)
	// Output:
	// Latest datapoint: timestamp=1 value=10.000000
}

// Exemplifies the usage of AddWithOptions function with a duplicate policy of MAX (only override if the value is higher than the existing value)
// nolint:errcheck
func ExampleClient_AddWithOptions_duplicateDatapointsMaxDuplicatePolicy() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client")

	labels := map[string]string{
		"machine": "machine-1",
		"az":      "us-west-2",
	}

	// get the default options and set the duplicate policy to MAX (only override if the value is higher than the existing value)
	options := redistimeseries.DefaultCreateOptions
	options.DuplicatePolicy = redistimeseries.MaxDuplicatePolicy
	options.Labels = labels

	client.AddWithOptions("time-series-example-duplicate-max", 1, 10.0, options)

	// this should not override the value given that the previous one ( 10.0 ) is greater than the new one we're trying to add
	client.AddWithOptions("time-series-example-duplicate-max", 1, 5.0, options)

	// Retrieve the latest data point
	latestDatapoint, _ := client.Get("time-series-example-duplicate-max")

	fmt.Printf("Latest datapoint: timestamp=%d value=%f\n", latestDatapoint.Timestamp, latestDatapoint.Value)
	// Output:
	// Latest datapoint: timestamp=1 value=10.000000
}

// Exemplifies the usage of AddWithOptions function for back filling - Add samples to a time series where the time of the sample is older than the newest sample in the series
// nolint:errcheck
func ExampleClient_AddWithOptions_backFilling() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client")

	labels := map[string]string{
		"machine": "machine-1",
		"az":      "us-west-2",
	}

	// use the default options
	options := redistimeseries.DefaultCreateOptions
	options.Labels = labels

	client.AddWithOptions("time-series-example-back-filling", 1, 1, options)
	client.AddWithOptions("time-series-example-back-filling", 2, 1, options)
	client.AddWithOptions("time-series-example-back-filling", 4, 1, options)
	// Add sample with timestamp ( 3 ) where the time of the sample is older than the newest sample in the series ( 4 )
	client.AddWithOptions("time-series-example-back-filling", 3, 1, options)

	// Retrieve the time-series data points
	datapoints, _ := client.RangeWithOptions("time-series-example-back-filling", 0, 1000, redistimeseries.DefaultRangeOptions)
	fmt.Printf("Datapoints: %v\n", datapoints)
	// Output:
	// Datapoints: [{1 1} {2 1} {3 1} {4 1}]
}

// Exemplifies the usage of RangeWithOptions function
// nolint:errcheck
func ExampleClient_RangeWithOptions() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client-1")
	for ts := 1; ts < 10; ts++ {
		client.Add("ts-1", int64(ts), float64(ts))
	}

	datapoints, _ := client.RangeWithOptions("ts-1", 0, 1000, redistimeseries.DefaultRangeOptions)
	fmt.Printf("Datapoints: %v\n", datapoints)
	// Output:
	// Datapoints: [{1 1} {2 2} {3 3} {4 4} {5 5} {6 6} {7 7} {8 8} {9 9}]
}

// Exemplifies the usage of RangeWithOptions function, while changing the reference timestamp on which a bucket is defined.
// nolint:errcheck
func ExampleClient_RangeWithOptions_aggregationMax() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client-1")
	for ts := 1; ts < 10; ts++ {
		client.Add("ts-1", int64(ts), float64(ts))
	}

	datapoints, _ := client.RangeWithOptions("ts-1", 0, 1000, *redistimeseries.NewRangeOptions().SetAggregation(redistimeseries.MaxAggregation, 5))
	fmt.Printf("Datapoints: %v\n", datapoints)
	// Output:
	// Datapoints: [{0 4} {5 9}]
}

// Exemplifies the usage of RangeWithOptions function, while changing the reference timestamp on which a bucket is defined.
// nolint:errcheck
func ExampleClient_RangeWithOptions_aggregationAlign() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client-1")
	for ts := 1; ts < 10; ts++ {
		client.Add("ts-1", int64(ts), float64(ts))
	}

	datapoints, _ := client.RangeWithOptions("ts-1", 0, 1000, *redistimeseries.NewRangeOptions().SetAggregation(redistimeseries.CountAggregation, 2).SetAlign(1))
	fmt.Printf("Datapoints: %v\n", datapoints)
	// Output:
	// Datapoints: [{1 2} {3 2} {5 2} {7 2} {9 1}]
}

// nolint
// Exemplifies the usage of ReverseRangeWithOptions function
func ExampleClient_ReverseRangeWithOptions() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client-1")
	for ts := 1; ts < 10; ts++ {
		client.Add("ts-2", int64(ts), float64(ts))
	}

	datapoints, _ := client.ReverseRangeWithOptions("ts-2", 0, 1000, redistimeseries.DefaultRangeOptions)
	fmt.Printf("Datapoints: %v\n", datapoints)
	// Output:
	// Datapoints: [{9 9} {8 8} {7 7} {6 6} {5 5} {4 4} {3 3} {2 2} {1 1}]
}

// nolint
// Exemplifies the usage of ReverseRangeWithOptions function while filtering value
func ExampleClient_ReverseRangeWithOptions_filterByValue() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client-1")
	for ts := 1; ts < 10; ts++ {
		client.Add("ts-2", int64(ts), float64(ts))
	}

	datapoints, _ := client.ReverseRangeWithOptions("ts-2", 0, 1000, *redistimeseries.NewRangeOptions().SetFilterByValue(5, 50))
	fmt.Printf("Datapoints: %v\n", datapoints)
	// Output:
	// Datapoints: [{9 9} {8 8} {7 7} {6 6} {5 5}]
}

// nolint
// Exemplifies the usage of ReverseRangeWithOptions function while filtering by timestamp
func ExampleClient_ReverseRangeWithOptions_filterByTs() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client-1")
	for ts := 1; ts < 10; ts++ {
		client.Add("ts-2", int64(ts), float64(ts))
	}

	datapoints, _ := client.ReverseRangeWithOptions("ts-2", 0, 1000, *redistimeseries.NewRangeOptions().SetFilterByTs([]int64{1, 2, 3, 4, 5}))
	fmt.Printf("Datapoints: %v\n", datapoints)
	// Output:
	// Datapoints: [{5 5} {4 4} {3 3} {2 2} {1 1}]
}

// nolint
// Exemplifies the usage of MultiRangeWithOptions function.
func ExampleClient_MultiRangeWithOptions() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client-1")

	// ensure clean DB
	client.FlushAll()

	labels1 := map[string]string{
		"machine": "machine-1",
		"az":      "us-east-1",
	}
	client.AddWithOptions("time-serie-1", 2, 1.0, redistimeseries.CreateOptions{Labels: labels1})
	client.Add("time-serie-1", 4, 2.0)

	labels2 := map[string]string{
		"machine": "machine-2",
		"az":      "us-east-1",
	}
	client.AddWithOptions("time-serie-2", 1, 5.0, redistimeseries.CreateOptions{Labels: labels2})
	client.Add("time-serie-2", 4, 10.0)

	ranges, _ := client.MultiRangeWithOptions(1, 10, redistimeseries.DefaultMultiRangeOptions, "az=us-east-1")

	fmt.Printf("Ranges: %v\n", ranges)
	// Output:
	// Ranges: [{time-serie-1 map[] [{2 1} {4 2}]} {time-serie-2 map[] [{1 5} {4 10}]}]
}

// nolint
// Exemplifies the usage of MultiRangeWithOptions function.
// grouping multiple time-series
func ExampleClient_MultiRangeWithOptions_groupByReduce() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client-1")

	// ensure clean DB
	client.FlushAll()

	labels1 := map[string]string{
		"machine": "machine-1",
		"az":      "us-east-1",
		"team":    "team-1",
	}
	client.AddWithOptions("time-serie-1", 2, 1.0, redistimeseries.CreateOptions{Labels: labels1})
	client.Add("time-serie-1", 4, 2.0)

	labels2 := map[string]string{
		"machine": "machine-2",
		"az":      "us-east-1",
		"team":    "team-2",
	}
	client.AddWithOptions("time-serie-2", 1, 5.0, redistimeseries.CreateOptions{Labels: labels2})
	client.Add("time-serie-2", 4, 10.0)

	labels3 := map[string]string{
		"machine": "machine-3",
		"az":      "us-east-1",
		"team":    "team-2",
	}
	client.AddWithOptions("time-serie-3", 1, 55.0, redistimeseries.CreateOptions{Labels: labels3})
	client.Add("time-serie-3", 4, 99.0)

	// Find out the total resources usage by team
	ranges, _ := client.MultiRangeWithOptions(1, 10, *redistimeseries.NewMultiRangeOptions().SetWithLabels(true).SetGroupByReduce("team", redistimeseries.SumReducer), "az=us-east-1")

	fmt.Printf("Sum of usage by team: %v\n", ranges)
	// Output:
	// Sum of usage by team: [{team=team-1 map[__reducer__:sum __source__:time-serie-1 team:team-1] [{2 1} {4 2}]} {team=team-2 map[__reducer__:sum __source__:time-serie-2,time-serie-3 team:team-2] [{1 60} {4 109}]}]
}

// Exemplifies the usage of MultiRangeWithOptions function,
// filtering the result by specific timestamps
// nolint:errcheck
func ExampleClient_MultiRangeWithOptions_filterByTs() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client-1")

	// ensure clean DB
	client.FlushAll()

	labels1 := map[string]string{
		"machine": "machine-1",
		"az":      "us-east-1",
	}
	client.AddWithOptions("time-serie-1", 2, 1.0, redistimeseries.CreateOptions{Labels: labels1})
	client.Add("time-serie-1", 4, 2.0)

	labels2 := map[string]string{
		"machine": "machine-2",
		"az":      "us-east-1",
	}
	client.AddWithOptions("time-serie-2", 1, 5.0, redistimeseries.CreateOptions{Labels: labels2})
	client.Add("time-serie-2", 4, 10.0)

	ranges, _ := client.MultiRangeWithOptions(1, 10, *redistimeseries.NewMultiRangeOptions().SetFilterByTs([]int64{1, 2}), "az=us-east-1")

	fmt.Printf("Ranges: %v\n", ranges)
	// Output:
	// Ranges: [{time-serie-1 map[] [{2 1}]} {time-serie-2 map[] [{1 5}]}]
}

// Exemplifies the usage of MultiRangeWithOptions function,
// filtering the result by value using minimum and maximum.
// nolint:errcheck
func ExampleClient_MultiRangeWithOptions_filterByValue() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client-1")

	// ensure the DB is empty
	client.FlushAll()

	labels1 := map[string]string{
		"machine": "machine-1",
		"az":      "us-east-1",
	}
	client.AddWithOptions("time-serie-1", 2, 1.0, redistimeseries.CreateOptions{Labels: labels1})
	client.Add("time-serie-1", 4, 2.0)

	labels2 := map[string]string{
		"machine": "machine-2",
		"az":      "us-east-1",
	}
	client.AddWithOptions("time-serie-2", 1, 2.0, redistimeseries.CreateOptions{Labels: labels2})
	client.Add("time-serie-2", 4, 10.0)

	ranges, _ := client.MultiRangeWithOptions(1, 10, *redistimeseries.NewMultiRangeOptions().SetFilterByValue(1, 5), "az=us-east-1")

	fmt.Printf("Ranges: %v\n", ranges)
	// Output:
	// Ranges: [{time-serie-1 map[] [{2 1} {4 2}]} {time-serie-2 map[] [{1 2}]}]
}

// Exemplifies the usage of MultiRangeWithOptions function,
// filtering the returned labels.
// nolint:errcheck
func ExampleClient_MultiRangeWithOptions_selectedLabels() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client-1")

	// ensure the DB is empty
	client.FlushAll()

	labels1 := map[string]string{
		"machine":  "machine-1",
		"team":     "SF-1",
		"location": "SF",
		"az":       "us-east-1",
	}
	client.AddWithOptions("selected-labels-ex-time-serie-1", 2, 1.0, redistimeseries.CreateOptions{Labels: labels1})
	client.Add("selected-labels-ex-time-serie-1", 4, 2.0)

	labels2 := map[string]string{
		"machine":  "machine-2",
		"team":     "NY-1",
		"location": "NY",
		"az":       "us-east-1",
	}
	client.AddWithOptions("selected-labels-ex-time-serie-2", 1, 10.0, redistimeseries.CreateOptions{Labels: labels2})
	client.Add("selected-labels-ex-time-serie-2", 4, 15.0)

	ranges, _ := client.MultiRangeWithOptions(1, 10, *redistimeseries.NewMultiRangeOptions().SetSelectedLabels([]string{"az", "location"}), "az=us-east-1")

	fmt.Printf("Ranges: %v\n", ranges)
	// Output:
	// Ranges: [{selected-labels-ex-time-serie-1 map[az:us-east-1 location:SF] [{2 1} {4 2}]} {selected-labels-ex-time-serie-2 map[az:us-east-1 location:NY] [{1 10} {4 15}]}]
}

// Exemplifies the usage of MultiReverseRangeWithOptions function.
// nolint:errcheck
func ExampleClient_MultiReverseRangeWithOptions() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client-1")

	// ensure the DB is empty
	client.FlushAll()

	labels1 := map[string]string{
		"machine": "machine-1",
		"az":      "us-east-1",
	}
	client.AddWithOptions("time-serie-1", 2, 1.0, redistimeseries.CreateOptions{Labels: labels1})
	client.Add("time-serie-1", 4, 2.0)

	labels2 := map[string]string{
		"machine": "machine-2",
		"az":      "us-east-1",
	}
	client.AddWithOptions("time-serie-2", 1, 5.0, redistimeseries.CreateOptions{Labels: labels2})
	client.Add("time-serie-2", 4, 10.0)

	ranges, _ := client.MultiReverseRangeWithOptions(1, 10, redistimeseries.DefaultMultiRangeOptions, "az=us-east-1")

	fmt.Printf("Ranges: %v\n", ranges)
	// Output:
	// Ranges: [{time-serie-1 map[] [{4 2} {2 1}]} {time-serie-2 map[] [{4 10} {1 5}]}]
}

// Exemplifies the usage of MultiReverseRangeWithOptions function,
// filtering the result by specific timestamps
// nolint:errcheck
func ExampleClient_MultiReverseRangeWithOptions_filterByTs() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client-1")

	labels1 := map[string]string{
		"machine": "machine-1",
		"az":      "us-east-1",
	}
	client.AddWithOptions("time-serie-1", 2, 1.0, redistimeseries.CreateOptions{Labels: labels1})
	client.Add("time-serie-1", 4, 2.0)

	labels2 := map[string]string{
		"machine": "machine-2",
		"az":      "us-east-1",
	}
	client.AddWithOptions("time-serie-2", 1, 5.0, redistimeseries.CreateOptions{Labels: labels2})
	client.Add("time-serie-2", 4, 10.0)

	ranges, _ := client.MultiReverseRangeWithOptions(1, 10, *redistimeseries.NewMultiRangeOptions().SetFilterByTs([]int64{1, 2}), "az=us-east-1")

	fmt.Printf("Ranges: %v\n", ranges)
	// Output:
	// Ranges: [{time-serie-1 map[] [{2 1}]} {time-serie-2 map[] [{1 5}]}]
}

// Exemplifies the usage of MultiReverseRangeWithOptions function,
// filtering the result by value using minimum and maximum.
// nolint:errcheck
func ExampleClient_MultiReverseRangeWithOptions_filterByValue() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client-1")

	// ensure the DB is empty
	client.FlushAll()

	labels1 := map[string]string{
		"machine": "machine-1",
		"az":      "us-east-1",
	}
	client.AddWithOptions("time-serie-1", 2, 1.0, redistimeseries.CreateOptions{Labels: labels1})
	client.Add("time-serie-1", 4, 2.0)

	labels2 := map[string]string{
		"machine": "machine-2",
		"az":      "us-east-1",
	}
	client.AddWithOptions("time-serie-2", 1, 2.0, redistimeseries.CreateOptions{Labels: labels2})
	client.Add("time-serie-2", 4, 10.0)

	ranges, _ := client.MultiReverseRangeWithOptions(1, 10, *redistimeseries.NewMultiRangeOptions().SetFilterByValue(1, 5), "az=us-east-1")

	fmt.Printf("Ranges: %v\n", ranges)
	// Output:
	// Ranges: [{time-serie-1 map[] [{4 2} {2 1}]} {time-serie-2 map[] [{1 2}]}]
}

// Exemplifies the usage of MultiReverseRangeWithOptions function,
// filtering the returned labels.
// nolint:errcheck
func ExampleClient_MultiReverseRangeWithOptions_selectedLabels() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client-1")

	// ensure the DB is empty
	client.FlushAll()

	labels1 := map[string]string{
		"machine":  "machine-1",
		"team":     "SF-1",
		"location": "SF",
		"az":       "us-east-1",
	}
	client.AddWithOptions("selected-labels-ex-time-serie-1", 2, 1.0, redistimeseries.CreateOptions{Labels: labels1})
	client.Add("selected-labels-ex-time-serie-1", 4, 2.0)

	labels2 := map[string]string{
		"machine":  "machine-2",
		"team":     "NY-1",
		"location": "NY",
		"az":       "us-east-1",
	}
	client.AddWithOptions("selected-labels-ex-time-serie-2", 1, 10.0, redistimeseries.CreateOptions{Labels: labels2})
	client.Add("selected-labels-ex-time-serie-2", 4, 15.0)

	ranges, _ := client.MultiReverseRangeWithOptions(1, 10, *redistimeseries.NewMultiRangeOptions().SetSelectedLabels([]string{"az", "location"}), "az=us-east-1")

	fmt.Printf("Ranges: %v\n", ranges)
	// Output:
	// Ranges: [{selected-labels-ex-time-serie-1 map[az:us-east-1 location:SF] [{4 2} {2 1}]} {selected-labels-ex-time-serie-2 map[az:us-east-1 location:NY] [{4 15} {1 10}]}]
}

//nolint:errcheck
// Exemplifies the usage of MultiGetWithOptions function while using the default MultiGetOptions and while using user defined MultiGetOptions.
func ExampleClient_MultiGetWithOptions() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client-1")

	// ensure the DB is empty
	client.FlushAll()

	labels1 := map[string]string{
		"machine": "machine-1",
		"az":      "us-east-1",
	}
	client.AddWithOptions("time-serie-1", 2, 1.0, redistimeseries.CreateOptions{Labels: labels1})
	client.Add("time-serie-1", 4, 2.0)

	labels2 := map[string]string{
		"machine": "machine-2",
		"az":      "us-east-1",
	}
	client.AddWithOptions("time-serie-2", 1, 5.0, redistimeseries.CreateOptions{Labels: labels2})
	client.Add("time-serie-2", 4, 10.0)

	ranges, _ := client.MultiGetWithOptions(redistimeseries.DefaultMultiGetOptions, "az=us-east-1")

	rangesWithLabels, _ := client.MultiGetWithOptions(*redistimeseries.NewMultiGetOptions().SetWithLabels(true), "az=us-east-1")

	fmt.Printf("Ranges: %v\n", ranges)
	fmt.Printf("Ranges with labels: %v\n", rangesWithLabels)

	// Output:
	// Ranges: [{time-serie-1 map[] [{4 2}]} {time-serie-2 map[] [{4 10}]}]
	// Ranges with labels: [{time-serie-1 map[az:us-east-1 machine:machine-1] [{4 2}]} {time-serie-2 map[az:us-east-1 machine:machine-2] [{4 10}]}]
}

// Exemplifies the usage of MultiAdd.
func ExampleClient_MultiAdd() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client-1")

	labels1 := map[string]string{
		"machine": "machine-1",
		"az":      "us-east-1",
	}
	labels2 := map[string]string{
		"machine": "machine-2",
		"az":      "us-east-1",
	}

	err := client.CreateKeyWithOptions("timeserie-1", redistimeseries.CreateOptions{Labels: labels1})
	if err != nil {
		log.Fatal(err)
	}
	err = client.CreateKeyWithOptions("timeserie-2", redistimeseries.CreateOptions{Labels: labels2})
	if err != nil {
		log.Fatal(err)
	}

	// Adding multiple datapoints to multiple series
	datapoints := []redistimeseries.Sample{
		{"timeserie-1", redistimeseries.DataPoint{1, 10.5}},
		{"timeserie-1", redistimeseries.DataPoint{2, 40.5}},
		{"timeserie-2", redistimeseries.DataPoint{1, 60.5}},
	}
	timestamps, _ := client.MultiAdd(datapoints...)

	fmt.Printf("Example adding multiple datapoints to multiple series. Added timestamps: %v\n", timestamps)

	// Adding multiple datapoints to the same serie
	datapointsSameSerie := []redistimeseries.Sample{
		{"timeserie-1", redistimeseries.DataPoint{3, 10.5}},
		{"timeserie-1", redistimeseries.DataPoint{4, 40.5}},
		{"timeserie-1", redistimeseries.DataPoint{5, 60.5}},
	}
	timestampsSameSerie, _ := client.MultiAdd(datapointsSameSerie...)

	fmt.Printf("Example of adding multiple datapoints to the same serie. Added timestamps: %v\n", timestampsSameSerie)

	// Output:
	// Example adding multiple datapoints to multiple series. Added timestamps: [1 2 1]
	// Example of adding multiple datapoints to the same serie. Added timestamps: [3 4 5]
}

// exemplifies the usage of DeleteSerie function
//nolint:errcheck
func ExampleClient_DeleteSerie() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	client := redistimeseries.NewClientFromPool(pool, "ts-client-1")

	// Create serie and add datapoint
	client.Add("ts", 1, 5)

	// Query the serie
	datapoints, _ := client.RangeWithOptions("ts", 0, 1000, redistimeseries.DefaultRangeOptions)
	fmt.Println(datapoints[0])
	// Output: {1 5}

	// Delete the serie
	client.DeleteSerie("ts")

}
