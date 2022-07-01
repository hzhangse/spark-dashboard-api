export INFLUXDB_HOST=http://10.233.60.195:8086
export INFLUXDB_DATABASE_NAME="sparkmeasure"
export DASHBOARD_URL_TEMPLATE="http://10.1.2.61:31042/d/-H0ElOqmiv/spark_perf_dashboard_v03?from=%d&to=%d&var-UserName=donggetan&var-ApplicationId=%s"
./spark-api
