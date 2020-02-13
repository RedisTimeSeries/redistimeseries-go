[![license](https://img.shields.io/github/license/RedisTimeSeries/RedisTimeSeries-go.svg)](https://github.com/RedisTimeSeries/RedisTimeSeries-go)
[![CircleCI](https://circleci.com/gh/RedisTimeSeries/redistimeseries-go.svg?style=svg)](https://circleci.com/gh/RedisTimeSeries/redistimeseries-go)
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
$ REDISTIMESERIES_TEST_PASSWORD="" go test
```

The tests expect a Redis server with the RedisTimeSeries module loaded to be available at localhost:6379

## Example Code

```go
package main 

import (
        "fmt"
        redistimeseries "github.com/RedisTimeSeries/redistimeseries-go"
)

func main() {
		// Connect to localhost with no password
        var client = redistimeseries.NewClient("localhost:6379", "nohelp", nil)
        var keyname = "mytest"
        _, haveit := client.Info(keyname)
        if haveit != nil {
			client.CreateKeyWithOptions(keyname, redistimeseries.DefaultCreateOptions)
			client.CreateKeyWithOptions(keyname+"_avg", redistimeseries.DefaultCreateOptions)
			client.CreateRule(keyname, redistimeseries.AvgAggregation, 60, keyname+"_avg")
        }
		// Add sample with timestamp from server time and value 100
        // TS.ADD mytest * 100 
        _, err := client.AddAutoTs(keyname, 100)
        if err != nil {
                fmt.Println("Error:", err)
        }
}
```

## Supported RedisTimeSeries Commands

| Command | Recommended API and godoc  |
| :---          |  ----: |
| [TS.CREATE](https://oss.redislabs.com/redistimeseries/commands/#tscreate) |   [CreateKeyWithOptions](https://godoc.org/github.com/RedisTimeSeries/redistimeseries-go#Client.CreateKeyWithOptions)          |
| [TS.ALTER](https://oss.redislabs.com/redistimeseries/commands/#tsalter) |   N/A          |
| [TS.ADD](https://oss.redislabs.com/redistimeseries/commands/#tsadd) |   <ul><li>[Add](https://godoc.org/github.com/RedisTimeSeries/redistimeseries-go#Client.Add)</li><li>[AddAutoTs](https://godoc.org/github.com/RedisTimeSeries/redistimeseries-go#Client.AddAutoTs)</li><li>[AddWithOptions](https://godoc.org/github.com/RedisTimeSeries/redistimeseries-go#Client.AddWithOptions)</li><li>[AddAutoTsWithOptions](https://godoc.org/github.com/RedisTimeSeries/redistimeseries-go#Client.AddWithOptions)</li> </ul>          |
| [TS.MADD](https://oss.redislabs.com/redistimeseries/commands/#tsmadd) |    N/A |
| [TS.INCRBY/TS.DECRBY](https://oss.redislabs.com/redistimeseries/commands/#tsincrbytsdecrby) |    N/A         |
| [TS.CREATERULE](https://oss.redislabs.com/redistimeseries/commands/#tscreaterule) |   [CreateRule](https://godoc.org/github.com/RedisTimeSeries/redistimeseries-go#Client.CreateRule)          |
| [TS.DELETERULE](https://oss.redislabs.com/redistimeseries/commands/#tsdeleterule) |   [DeleteRule](https://godoc.org/github.com/RedisTimeSeries/redistimeseries-go#Client.DeleteRule)          |
| [TS.RANGE](https://oss.redislabs.com/redistimeseries/commands/#tsrange) |   [RangeWithOptions](https://godoc.org/github.com/RedisTimeSeries/redistimeseries-go#Client.RangeWithOptions)          |
| [TS.MRANGE](https://oss.redislabs.com/redistimeseries/commands/#tsmrange) |   [MultiRangeWithOptions](https://godoc.org/github.com/RedisTimeSeries/redistimeseries-go#Client.MultiRangeWithOptions)          |
| [TS.GET](https://oss.redislabs.com/redistimeseries/commands/#tsget) |   [Get](https://godoc.org/github.com/RedisTimeSeries/redistimeseries-go#Client.Get)          |
| [TS.MGET](https://oss.redislabs.com/redistimeseries/commands/#tsmget) |   [MultiGet](https://godoc.org/github.com/RedisTimeSeries/redistimeseries-go#Client.MultiGet)          |
| [TS.INFO](https://oss.redislabs.com/redistimeseries/commands/#tsinfo) |   [Info](https://godoc.org/github.com/RedisTimeSeries/redistimeseries-go#Client.Info)          |
| [TS.QUERYINDEX](https://oss.redislabs.com/redistimeseries/commands/#tsqueryindex) |    N/A |


## License

redistimeseries-go is distributed under the Apache-2 license - see [LICENSE](LICENSE)
