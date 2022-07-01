package controllers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/gin-gonic/gin"

	client "github.com/influxdata/influxdb1-client/v2"
)

type InfluxDB struct {
}

func newInfluxDB() InfluxDB {
	return InfluxDB{}
}

func (influx InfluxDB) doHandler(c *gin.Context) error {
	app := c.Param("app")
	err := queryInflux(app)
	return err
}

// 查询
func queryInflux(app string) error {
	//db := "sparkmeasure"
	qryjobStart := "select * from \"jobs_started\""
	if app != "" {
		qryjobStart = qryjobStart + " where applicationId='" + app + "'"
		if _, ok := sparkApplicationMap[app]; ok {
			if sparkApplicationMap[app].endTime != "now" {
				return nil
			}
		}
	}

	// Make client
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: influxdbhost,
	})
	if err != nil {
		fmt.Println("Error creating InfluxDB Client: ", err.Error())
		log.Println("Error creating InfluxDB Client: ", err.Error())
		return err
	}
	defer c.Close()

	res, err := queryDB(c, qryjobStart, influxdb)

	if err != nil {
		log.Println("Error for :"+qryjobStart, err.Error())
		return err
	}

	for _, row := range res[0].Series[0].Values {
		var app SparkApplication
		t, err := time.Parse(time.RFC3339, row[0].(string))
		if err != nil {
			return err
		}

		applicationId := row[1].(string)

		app.startTime = fmt.Sprintf("%d", t.UnixNano()/1e6)
		app.StartTimeStr = t.Format("2006-01-02 15:04:05")
		app.ApplicationId = applicationId
		//applicationId = applicationId + "a"

		qryJobEnd := "select * from jobs_ended where applicationId='" + applicationId + "'"
		resJobEnd, err := queryDB(c, qryJobEnd, influxdb)
		if err != nil {
			fmt.Println("Error for :"+qryJobEnd, err.Error())
			log.Println("Error for :"+qryJobEnd, err.Error())
			return err
		}
		if len(resJobEnd[0].Series) == 0 {
			if app.endTime == "" {
				app.endTime = "now"
				app.EndTimeStr = "now"
			}
		} else {
			for _, endRow := range resJobEnd[0].Series[0].Values {
				et, err := time.Parse(time.RFC3339, endRow[0].(string))
				if err != nil {
					return err
				}

				app.endTime = fmt.Sprintf("%d", et.UnixNano()/1e6)
				app.EndTimeStr = et.Format("2006-01-02 15:04:05")
				break
			}
		}
		url := fmt.Sprintf(dashboardUrlTemplate, app.startTime, app.endTime, app.ApplicationId)
		app.DashboardUrl = url
		sparkApplicationMap[applicationId] = app
	}
	return nil
}

//query
func queryDB(cli client.Client, cmd string, db string) (res []client.Result, err error) {
	q := client.Query{
		Command:  cmd,
		Database: db,
	}
	if response, err := cli.Query(q); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	} else {
		return res, err
	}
	return res, nil
}

//序列化
func marshal_inner(data interface{}) ([]byte, error) {
	bf := bytes.NewBuffer([]byte{})
	jsonEncoder := json.NewEncoder(bf)
	jsonEncoder.SetEscapeHTML(false)
	if err := jsonEncoder.Encode(data); err != nil {
		return nil, err
	}

	return bf.Bytes(), nil
}
