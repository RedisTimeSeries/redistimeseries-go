package redis_timeseries_go

import (
	"errors"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"strconv"
)

func toAggregationType(aggType interface{}) (aggTypeStr AggregationType, err error) {
	agg, err := redis.String(aggType, nil)
	if err != nil {
		return
	}
	aggTypeStr = (AggregationType)(agg)
	return
}

func toDuplicatePolicy(duplicatePolicy interface{}) (duplicatePolicyStr DuplicatePolicyType, err error) {
	duplicatePolicyStr = ""
	if duplicatePolicy == nil {
		return
	}
	policy, err := redis.String(duplicatePolicy, nil)
	if err != nil {
		return
	}
	duplicatePolicyStr = (DuplicatePolicyType)(policy)
	return
}

func ParseRules(ruleInterface interface{}, err error) (rules []Rule, retErr error) {
	if err != nil {
		return nil, err
	}
	ruleSlice, err := redis.Values(ruleInterface, nil)
	if err != nil {
		return nil, err
	}
	for _, ruleSlice := range ruleSlice {

		ruleValues, err := redis.Values(ruleSlice, nil)
		if err != nil {
			return nil, err
		}
		destKey, err := redis.String(ruleValues[0], nil)
		if err != nil {
			return nil, err
		}
		bucketSizeSec, err := redis.Int(ruleValues[1], nil)
		if err != nil {
			return nil, err
		}
		aggType, err := toAggregationType(ruleValues[2])
		if err != nil {
			return nil, err
		}
		rules = append(rules, Rule{destKey, bucketSizeSec, aggType})
	}
	return rules, nil
}

func ParseInfo(result interface{}, err error) (info KeyInfo, outErr error) {
	values, outErr := redis.Values(result, err)
	if outErr != nil {
		return KeyInfo{}, err
	}
	if len(values)%2 != 0 {
		return KeyInfo{}, errors.New("ParseInfo expects even number of values result")
	}
	var key string
	for i := 0; i < len(values); i += 2 {
		key, outErr = redis.String(values[i], nil)
		switch key {
		case "totalSamples":
			info.TotalSamples, outErr = redis.Int64(values[i+1], nil)
		case "rules":
			info.Rules, outErr = ParseRules(values[i+1], nil)
		case "retentionTime":
			info.RetentionTime, outErr = redis.Int64(values[i+1], nil)
		case "chunkCount":
			info.ChunkCount, outErr = redis.Int64(values[i+1], nil)
		// Backwards compatible
		case "maxSamplesPerChunk":
			var v int64
			v, outErr = redis.Int64(values[i+1], nil)
			info.MaxSamplesPerChunk = v
			info.ChunkSize = 16 * v
		case "chunkSize":
			info.ChunkSize, outErr = redis.Int64(values[i+1], nil)
		case "lastTimestamp":
			info.LastTimestamp, outErr = redis.Int64(values[i+1], nil)
		case "labels":
			info.Labels, outErr = ParseLabels(values[i+1])
		case "duplicatePolicy":
			info.DuplicatePolicy, outErr = toDuplicatePolicy(values[i+1])
		}
		if outErr != nil {
			return KeyInfo{}, outErr
		}
	}

	return info, nil
}

func ParseDataPoints(info interface{}) (dataPoints []DataPoint, err error) {
	dataPoints = make([]DataPoint, 0)
	values, err := redis.Values(info, err)
	if err != nil {
		return
	}
	for _, rawDataPoint := range values {
		var dataPoint *DataPoint = nil //nolint:ineffassign
		dataPoint, err = ParseDataPoint(rawDataPoint)
		if err != nil {
			return
		}
		if dataPoint != nil {
			dataPoints = append(dataPoints, *dataPoint)
		}
	}
	return
}

func ParseDataPoint(rawDataPoint interface{}) (dataPoint *DataPoint, err error) {
	dataPoint = nil
	iValues, err := redis.Values(rawDataPoint, nil)
	if err != nil || len(iValues) == 0 {
		return
	}
	if len(iValues) != 2 {
		err = fmt.Errorf("ParseDataPoint expects array reply of size 2 with timestamp and value.Got %v", iValues)
		return
	}
	timestamp, err := redis.Int64(iValues[0], nil)
	if err != nil {
		return
	}
	value, err := redis.String(iValues[1], nil)
	if err != nil {
		return
	}
	float, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return
	}
	dataPoint = NewDataPoint(timestamp, float)
	return
}

func ParseLabels(res interface{}) (labels map[string]string, err error) {
	values, err := redis.Values(res, err)
	if err != nil {
		return
	}
	labels = make(map[string]string, len(values))
	for i := 0; i < len(values); i++ {
		iValues, err := redis.Values(values[i], err)
		if err != nil {
			return nil, err
		}
		if len(iValues) != 2 {
			err = errors.New("ParseLabels: expects 2 elements per inner-array")
			return nil, err
		}
		key, okKey := iValues[0].([]byte)
		value, okValue := iValues[1].([]byte)
		if !okKey || !okValue {
			err = errors.New("ParseLabels: StringMap key not a bulk string value")
			return nil, err
		}
		labels[string(key)] = string(value)
	}
	return
}

func ParseRanges(info interface{}) (ranges []Range, err error) {
	values, err := redis.Values(info, err)
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return []Range{}, nil
	}
	for _, i := range values {
		iValues, err := redis.Values(i, err)
		if err != nil {
			return nil, err
		}
		if len(iValues) != 3 {
			err = errors.New("ParseRanges: expects 3 elements per inner-array")
			return nil, err
		}

		name, err := redis.String(iValues[0], nil)
		if err != nil {
			return nil, err
		}

		labels, err := ParseLabels(iValues[1])
		if err != nil {
			return nil, err
		}

		dataPoints, err := ParseDataPoints(iValues[2])
		if err != nil {
			return nil, err
		}
		r := Range{name, labels, dataPoints}
		ranges = append(ranges, r)
	}
	return
}

func ParseRangesSingleDataPoint(info interface{}) (ranges []Range, err error) {
	values, err := redis.Values(info, err)

	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return []Range{}, nil
	}
	for _, i := range values {
		dataPoints := make([]DataPoint, 0)

		iValues, err := redis.Values(i, err)
		if err != nil {
			return nil, err
		}
		if len(iValues) != 3 {
			err = errors.New("ParseRanges: expects 3 elements per inner-array")
			return nil, err
		}

		name, err := redis.String(iValues[0], nil)
		if err != nil {
			return nil, err
		}

		labels, err := ParseLabels(iValues[1])
		if err != nil {
			return nil, err
		}
		var dataPoint *DataPoint = nil //nolint:ineffassign
		dataPoint, err = ParseDataPoint(iValues[2])
		if err != nil {
			return nil, err
		}
		if dataPoint != nil {
			dataPoints = append(dataPoints, *dataPoint)
		}
		if err != nil {
			return nil, err
		}
		r := Range{name, labels, dataPoints}
		ranges = append(ranges, r)
	}
	return
}
