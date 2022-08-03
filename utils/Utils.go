package utils

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

func FormatTime(varTime string) (string, error) {
	layout := "2006-01-02 15:04:05"
	t, err := ParseTime(varTime)
	if err != nil {
		return "", err
	}
	et := t.Format(layout)
	return et, nil
}

func ParseTime(varTime string) (time.Time, error) {
	location, _ := time.LoadLocation("Asia/Shanghai")

	tm, err := strconv.ParseInt(varTime, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	et := time.Unix(tm/1000, 0).In(location)
	return et, nil
}

func ParseInt64ToTime(tm int64) (time.Time, error) {
	location, _ := time.LoadLocation("Asia/Shanghai")
	et := time.Unix(tm/1000, 0).In(location)
	return et, nil
}

func AdjustTime(varTime string, duration int64) string {
	t, err := ParseTime(varTime)
	if err != nil {
		return varTime
	}
	adjustTime := t.Add(time.Duration(time.Second * time.Duration(duration)))
	return fmt.Sprintf("%d", adjustTime.UnixNano()/1e6)

}

func GetInterfaceToString(value interface{}) string {
	// interface 转 string
	var key string
	if value == nil {
		return key
	}

	switch value.(type) {
	case float64:
		ft := value.(float64)
		key = strconv.FormatFloat(ft, 'f', -1, 64)
	case float32:
		ft := value.(float32)
		key = strconv.FormatFloat(float64(ft), 'f', -1, 64)
	case int:
		it := value.(int)
		key = strconv.Itoa(it)
	case uint:
		it := value.(uint)
		key = strconv.Itoa(int(it))
	case int8:
		it := value.(int8)
		key = strconv.Itoa(int(it))
	case uint8:
		it := value.(uint8)
		key = strconv.Itoa(int(it))
	case int16:
		it := value.(int16)
		key = strconv.Itoa(int(it))
	case uint16:
		it := value.(uint16)
		key = strconv.Itoa(int(it))
	case int32:
		it := value.(int32)
		key = strconv.Itoa(int(it))
	case uint32:
		it := value.(uint32)
		key = strconv.Itoa(int(it))
	case int64:
		it := value.(int64)
		key = strconv.FormatInt(it, 10)
	case uint64:
		it := value.(uint64)
		key = strconv.FormatUint(it, 10)
	case string:
		key = value.(string)
	case []byte:
		key = string(value.([]byte))
	default:
		newValue, _ := json.Marshal(value)
		key = string(newValue)
	}

	return key

}
