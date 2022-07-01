FROM golang:1.17


ENV APP_NAME=spark-api
ENV GIN_MODE=release

#ENV INFLUXDB_HOST=http://10.1.2.61:30617
#ENV INFLUXDB_HOST=http://10.233.60.195:8086

#ENV INFLUXDB_DATABASE_NAME="sparkmeasure"
#ENV INFLUXDB_MEASUREMENT=""
#ENV PORT=8087
#ENV DASHBOARD_URL_TEMPLATE="http://10.1.2.61:31042/d/-H0ElOqmiv/spark_perf_dashboard_v03?from=%s&to=%s&var-UserName=donggetan&var-ApplicationId=%s"
WORKDIR /go/src/app
COPY . .

RUN go get -d -v ./...
RUN go install -v ./...
RUN rm -rf /go/src/app

EXPOSE $PORT

ENTRYPOINT $APP_NAME
