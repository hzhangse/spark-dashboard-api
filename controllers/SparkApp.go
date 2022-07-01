package controllers

import (
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
)

var influxdbhost = os.Getenv(INFLUXDB_HOST)
var influxdb = os.Getenv(InfluxDBDatabaseName)
var dashboardUrlTemplate = os.Getenv("DASHBOARD_URL_TEMPLATE")
var prom_http_url = os.Getenv(PROM_HTTP_URL)

const (
	INFLUXDB_HOST        = "INFLUXDB_HOST"
	InfluxDBDatabaseName = "INFLUXDB_DATABASE_NAME"
	InfluxDBMeasurement  = "INFLUXDB_MEASUREMENT"
	PROM_HTTP_URL        = "PROM_HTTP_URL"
)

type SparkApplication struct {
	ApplicationId string `json:"applicationId`
	startTime     string
	StartTimeStr  string `json:"startTime"`
	endTime       string
	EndTimeStr    string `json:"endTime"`
	DashboardUrl  string `json:"dashboardUrl"`
}

var sparkApplicationMap map[string]SparkApplication = make(map[string]SparkApplication)

type ISparkAppControl interface {
	doHandler(c *gin.Context) error
}

func SparkAppControllerHandler() func(c *gin.Context) {
	return func(c *gin.Context) {
		app := c.Param("app")
		dbtype := c.DefaultQuery("db", "prom")
		var control ISparkAppControl

		if dbtype == "prom" {
			control = newPrometheus()
		} else if dbtype == "influx" {
			control = newInfluxDB()
		}
		err := control.doHandler(c)
		if err != nil {
			c.PureJSON(500, gin.H{"msg": err.Error()})
		}
		if app == "" {
			c.PureJSON(http.StatusOK, sparkApplicationMap)
		} else {
			if _, ok := sparkApplicationMap[app]; ok {
				c.PureJSON(http.StatusOK, sparkApplicationMap[app])
			} else {
				c.PureJSON(404, gin.H{"msg": "Not found"})
			}
		}
	}
}
