package controllers

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/common/model"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
)

type Prometheus struct {
}

func newPrometheus() Prometheus {
	return Prometheus{}
}

func (influx Prometheus) doHandler(c *gin.Context) error {

	app := c.Param("app")
	beforeDaysStr := c.DefaultQuery("beforeDays", "30")
	beforeDays, err := strconv.ParseInt(beforeDaysStr, 10, 64)
	if err != nil {
		return err
	}
	err = qryProm(app, beforeDays)
	return err

}

func qryProm(app string, beforeDays int64) error {
	conditions := make(map[string]string)
	if app != "" {
		conditions["applicationId"] = app
		if _, ok := sparkApplicationMap[app]; ok {
			if sparkApplicationMap[app].endTime != "now" {
				return nil
			}
		}
	}

	lbls, err := querySeries("jobs_started", conditions, beforeDays)
	if err != nil {
		return err
	}

	for _, row := range lbls {
		var app SparkApplication

		startTime := string(row["startTime"])
		applicationId := string(row["applicationId"])
		app.startTime = startTime
		app.StartTimeStr, err = parseTime(startTime)
		if err != nil {
			return err
		}
		app.ApplicationId = applicationId

		conditions["applicationId"] = applicationId
		resJobEnd, err := querySeries("jobs_ended", conditions, beforeDays)
		if err != nil {

			return err
		}
		if len(resJobEnd) == 0 {
			if app.endTime == "" {
				app.endTime = "now"
				app.EndTimeStr = "now"
			}
		} else {
			for _, endRow := range resJobEnd {
				endTime := string(endRow["completionTime"])
				app.endTime = endTime
				app.EndTimeStr, err = parseTime(endTime)
				if err != nil {
					return err
				}
				break
			}
		}
		url := fmt.Sprintf(dashboardUrlTemplate, app.startTime, app.endTime, app.ApplicationId)
		app.DashboardUrl = url
		sparkApplicationMap[applicationId] = app
	}
	return nil
}
func querySeries(metric string, conditions map[string]string, beforeDays int64) ([]model.LabelSet, error) {
	client, err := api.NewClient(api.Config{
		Address: prom_http_url,
	})
	if err != nil {
		fmt.Printf("Error creating client: %v\n", err)
		return nil, err
	}

	v1api := v1.NewAPI(client)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	matchArr := make([]string, 0)
	// matchArr = append(matchArr, "{__name__=\""+metric+"\"")
	// for k, v := range conditions {
	// 	matchArr = append(matchArr, ","+k+"=\""+v+"\"")
	// }
	// matchArr = append(matchArr, "}")
	match := "{__name__=\"" + metric + "\""
	for k, v := range conditions {
		match = match + "," + k + "=\"" + v + "\""
	}
	match = match + "}"
	matchArr = append(matchArr, match)

	lbls, warnings, err := v1api.Series(ctx, matchArr, time.Now().Add(-time.Hour*24*time.Duration(beforeDays)), time.Now())
	if err != nil {
		fmt.Printf("Error querying Prometheus: %v\n", err)
		return nil, err
	}
	if len(warnings) > 0 {
		fmt.Printf("Warnings: %v\n", warnings)
	}
	fmt.Println("Result:")
	for _, lbl := range lbls {

		fmt.Println(lbl)
	}
	return lbls, nil
}

func parseTime(varTime string) (string, error) {
	location, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return "", err
	}

	tm, err := strconv.ParseInt(varTime, 10, 64)
	if err != nil {
		return "", err
	}
	layout := "2006-01-02 15:04:05"
	et := time.Unix(tm/1000, 0).In(location).Format(layout)
	return et, nil
}
