package utils

import (
	"context"
	"encoding/json"
	"fmt"
	stdlog "log"
	"os"
	promremote "spark-api/prom"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

type labelList []promremote.Label

type dp promremote.Datapoint

type header struct {
	name  string
	value string
}

var (
	logger       = stdlog.New(os.Stderr, "promremotecli_log ", stdlog.LstdFlags)
	writeURLFlag string
	// headers      map[string]string
	client promremote.Client
	err    error
)

func init() {
	writeURLFlag = os.Getenv("PROM_REMOTE_WRITE_URL")
	if len(writeURLFlag) == 0 {
		panic("no Prom Remote Write URL defined, please set PromRemoteWriteURL")
	}

	cfg := promremote.NewConfig(
		promremote.WriteURLOption(writeURLFlag),
	)

	client, err = promremote.NewClient(cfg)
	if err != nil {
		logger.Fatal(fmt.Errorf("unable to construct client: %v", err))
	}
}

func stringTobyteSlice(s string) []byte {
	tmp1 := (*[2]uintptr)(unsafe.Pointer(&s))
	tmp2 := [3]uintptr{tmp1[0], tmp1[1], tmp1[1]}
	return *(*[]byte)(unsafe.Pointer(&tmp2))
}

func WriteInBatch(jsonArr []string, currentKey string) error {
	tslist := make([]promremote.TimeSeries, 0)
	for _, jsonStr := range jsonArr {
		ts, err := convert(jsonStr)
		if err == nil {
			tslist = append(tslist, ts)
		} else {
			return err
		}
	}
	//return nil
	return send2Prom(tslist, currentKey)

}

func convertMap(jsonStr string) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	err := json.Unmarshal(stringTobyteSlice(jsonStr), &result)
	if err != nil {
		logger.Println("the source message is invalid, ignore this message:"+jsonStr, err)
		return nil, err
	}
	return result, nil

}

func convertTimeSeries(map1 map[string]interface{}) (promremote.TimeSeries, string, error) {
	//map1 := make(map[string]interface{})
	appId := ""
	var result = promremote.TimeSeries{}

	dpValue := map1["epochMillis"]

	var dpFlag dp = dp{}
	switch dpValue.(type) {
	case float64:
		tm := dpValue.(float64)
		ts, err := ParseInt64ToTime(int64(tm))
		if err == nil {
			dpFlag = dp{
				//time.Now(),
				ts,
				tm,
			}
			i := 0
			l := len(map1)
			var labelsListFlag = make([]promremote.Label, l)
			for k, v := range map1 {
				if k == "name" {
					labelsListFlag[i] = promremote.Label{
						Name:  "__name__",
						Value: v.(string),
					}

				} else {
					vStr := GetInterfaceToString(v)
					if k == "appId" {
						appId = vStr
					}
					labelsListFlag[i] = promremote.Label{
						Name:  k,
						Value: vStr,
					}
				}
				i++
			}

			result = promremote.TimeSeries{
				Labels:    []promremote.Label(labelsListFlag),
				Datapoint: promremote.Datapoint(dpFlag),
			}
		} else {
			return result, appId, err
		}
	}
	return result, appId, nil
}

func convert(jsonStr string) (promremote.TimeSeries, error) {
	map1 := make(map[string]interface{})
	var result = promremote.TimeSeries{}
	err := json.Unmarshal(stringTobyteSlice(jsonStr), &map1)
	if err != nil {
		logger.Println("the source message is invalid, ignore this message:"+jsonStr, err)
		return result, err
	}

	dpValue := map1["epochMillis"]

	var dpFlag dp = dp{}
	switch dpValue.(type) {
	case float64:
		tm := dpValue.(float64)
		ts, err := ParseInt64ToTime(int64(tm))
		if err == nil {
			dpFlag = dp{
				//time.Now(),
				ts,
				tm,
			}
			i := 0
			l := len(map1)
			var labelsListFlag = make([]promremote.Label, l)
			for k, v := range map1 {
				if k == "name" {
					labelsListFlag[i] = promremote.Label{
						Name:  "__name__",
						Value: v.(string),
					}

				} else {
					labelsListFlag[i] = promremote.Label{
						Name:  k,
						Value: GetInterfaceToString(v),
					}
				}
				i++
			}

			result = promremote.TimeSeries{
				Labels:    []promremote.Label(labelsListFlag),
				Datapoint: promremote.Datapoint(dpFlag),
			}
		} else {
			return result, err
		}
	}
	return result, nil
}

func send2Prom(tsList promremote.TSList, currentKey string) error {
	// logger.Println("writing datapoint", dpFlag.String())
	// logger.Println("labelled", labelsListFlag.String())
	logger.Println("currentKey set to head", currentKey)
	headers := make(map[string]string)
	headers["User-Agent"] = "kafka-consumer"
	headers["appid"] = currentKey
	result, writeErr := client.WriteTimeSeries(context.Background(), tsList,
		promremote.WriteOptions{Headers: headers})
	if err := error(writeErr); err != nil {
		json.NewEncoder(os.Stdout).Encode(struct {
			Success    bool   `json:"success"`
			Error      string `json:"error"`
			StatusCode int    `json:"statusCode"`
		}{
			Success:    false,
			Error:      err.Error(),
			StatusCode: writeErr.StatusCode(),
		})
		os.Stdout.Sync()

		logger.Println("write error", err)
		return err
	}

	json.NewEncoder(os.Stdout).Encode(struct {
		Success    bool `json:"success"`
		StatusCode int  `json:"statusCode"`
	}{
		Success:    true,
		StatusCode: result.StatusCode,
	})
	os.Stdout.Sync()

	logger.Println("write success")
	return nil
}

func (t *labelList) String() string {
	var labels [][]string
	for _, v := range []promremote.Label(*t) {
		labels = append(labels, []string{v.Name, v.Value})
	}
	return fmt.Sprintf("%v", labels)
}

func (t *labelList) Set(value string) error {
	labelPair := strings.Split(value, ":")
	if len(labelPair) != 2 {
		return fmt.Errorf("incorrect number of arguments to '-t': %d", len(labelPair))
	}

	label := promremote.Label{
		Name:  labelPair[0],
		Value: labelPair[1],
	}

	*t = append(*t, label)

	return nil
}

func (d *dp) String() string {
	return fmt.Sprintf("%v", []string{d.Timestamp.String(), fmt.Sprintf("%v", d.Value)})
}

func (d *dp) Set(value string) error {
	dp := strings.Split(value, ",")
	if len(dp) != 2 {
		return fmt.Errorf("incorrect number of arguments to '-d': %d", len(dp))
	}

	var ts time.Time
	if strings.ToLower(dp[0]) == "now" {
		ts = time.Now()
	} else {
		i, err := strconv.Atoi(dp[0])
		if err != nil {
			return fmt.Errorf("unable to parse timestamp: %s", dp[1])
		}
		ts = time.Unix(int64(i), 0)
	}

	val, err := strconv.ParseFloat(dp[1], 64)
	if err != nil {
		return fmt.Errorf("unable to parse value as float64: %s", dp[0])
	}

	d.Timestamp = ts
	d.Value = val

	return nil
}
