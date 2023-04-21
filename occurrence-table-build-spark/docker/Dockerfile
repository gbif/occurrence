FROM alpine:latest
RUN apk update && \
    apk upgrade && \
    apk add bash

RUN mkdir /jobs
COPY ./target/occurrence-table-build-spark-0.192.7-H3-SNAPSHOT.jar /jobs/gbif-occurrence-table.jar

