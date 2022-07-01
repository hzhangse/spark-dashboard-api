...
## Spark dashboard api 


***
### Description:
To query spark job releated  time series record from influxdb/prometheus (job started/ended time). and fullfill the template url of grafana for spark-dashboard

### lists of implementation:
1. access infludb using influxdb go client v1
2. access prometheus using prometheus go client
3. using gin as http controler server
4. support build and deployed on kubernetes enviroment
### build and run

GOOS=linux GOARCH=amd64 go build

export INFLUXDB_HOST=http://10.233.60.195:8086
export INFLUXDB_DATABASE_NAME="sparkmeasure"
export DASHBOARD_URL_TEMPLATE="http://10.1.2.61:31042/d/-H0ElOqmiv/spark_perf_dashboard_v03?from=%d&to=%d&var-UserName=donggetan&var-ApplicationId=%s"
./spark-api

### debug on vscode
create launch.json in .vsocde folder 

    {
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Launch file",
            "type": "go",
            "request": "launch",
            "mode": "debug",
            "program": "app.go",
            "env": { 
                "PROM_HTTP_URL" : "http://10.1.2.61:30003/",
                "INFLUXDB_HOST":"http://10.1.2.61:30617",
                "INFLUXDB_DATABASE_NAME":"sparkmeasure",
                "DASHBOARD_URL_TEMPLATE":"http://10.1.2.61:31042/d/-H0ElOqmiv/spark_perf_dashboard_v03?from=%s&to=%s&var-UserName=donggetan&var-ApplicationId=%s"
                }
        }, 
      ]
    }

### deploy on kubernetes
   docker build -t registry.cn-shanghai.aliyuncs.com/xxxx/sparkapi:latest .
   docker push registry.cn-shanghai.aliyuncs.com/xxxx/sparkapi:latest

    see from the deploy.yaml
        env:                    ## set the variable using env 
          - name: INFLUXDB_HOST     #Key 
            value: "http://10.1.2.61:30617"  #value
          - name: INFLUXDB_DATABASE_NAME     
            value: "sparkmeasure"
          - name: PROM_HTTP_URL     #if using prometheus as data source 
            value: "http://10.1.2.61:30003/"            
          - name: DASHBOARD_URL_TEMPLATE     #template url of grafana for spark-dashboard
            value: "http://10.1.2.61:31042/d/-H0ElOqmiv/spark_perf_dashboard_v03?from=%s&to=%s&var-UserName=xxx&var-ApplicationId=%s"  

### access api 
  Base url:  http://localhost:8087/api/v1/sparkapp/
  If want to get by applicationId : http://localhost:8087/api/v1/sparkapp/{applicationId}
  
| request parameter  |  description |   value  |
|  ----  | ----  | ----  |
| db  | db type,for example: http://localhost:8087/api/v1/sparkapp/?db=influx means query influxDB| "influx" "prom" |
| beforeDays  | only used when db="prom",count from now | any int64|


