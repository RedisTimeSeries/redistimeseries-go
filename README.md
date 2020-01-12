[![license](https://img.shields.io/github/license/RedisTimeSeries/RedisTimeSeries-go.svg)](https://github.com/RedisTimeSeries/RedisTimeSeries-go)
[![CircleCI](https://circleci.com/gh/RedisTimeSeries/redistimeseries-go.svg?style=svg&circle-token=022ed6c86563cbb7d19ff4fd3ca6eab9053603f2)](https://circleci.com/gh/RedisTimeSeries/redistimeseries-go)
[![GitHub issues](https://img.shields.io/github/release/RedisTimeSeries/redistimeseries-go.svg)](https://github.com/RedisTimeSeries/redistimeseries-go/releases/latest)
[![Codecov](https://codecov.io/gh/RedisTimeSeries/redistimeseries-go/branch/master/graph/badge.svg)](https://codecov.io/gh/RedisTimeSeries/redistimeseries-go)
[![GoDoc](https://godoc.org/github.com/RedisTimeSeries/redistimeseries-go?status.svg)](https://godoc.org/github.com/RedisTimeSeries/redistimeseries-go)


# redis-timeseries-go

Go client for RedisTimeSeries (https://github.com/RedisLabsModules/redis-timeseries), based on redigo.

Client and ConnPool based on the work of dvirsky and mnunberg on https://github.com/RediSearch/redisearch-go

## Installing

```sh
$ go get github.com/RedisTimeSeries/redistimeseries-go
```

## Running tests

A simple test suite is provided, and can be run with:

```sh
$ go test
```

The tests expect a Redis server with the RedisTimeSeries module loaded to be available at localhost:6379

## Example Code

```
import (
        "fmt"
        "github.com/RedisTimeSeries/redistimeseries-go"
        "time"
)

func main() {
	// Connect to localhost with no password
        var client = redis_timeseries_go.NewClient("localhost:6379", "nohelp", nil)
        var duration, _ = time.ParseDuration("1m")
        var keyname = "mytest"
        _, havit := client.Info(keyname)
        if havit != nil {
                client.CreateKey(keyname, duration)
                client.CreateKey(keyname+"_avg", 0)
                client.CreateRule(keyname, redis_timeseries_go.AvgAggregation, 60, keyname+"_avg")
        }
        now :=  time.Now().UnixNano() / 1e6 // now in ms
        err := client.Add(keyname, now, 100)
        if err != nil {
                fmt.Println("Error:", err)
        }

}
```

## License

redistimeseries-go is distributed under the Apache-2 license - see [LICENSE](LICENSE)
