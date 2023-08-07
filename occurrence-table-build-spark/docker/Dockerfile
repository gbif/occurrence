FROM alpine:latest
RUN apk update && \
    apk upgrade && \
    apk add bash

RUN mkdir /jobs
COPY ./target/occurrence-table-build-spark-0.193.0-H3-SNAPSHOT.jar /jobs/occurrence-table-builder-spark.jar

