FROM alpine:latest
RUN apk update && \
    apk upgrade && \
    apk add bash

RUN apk add --no-cache bash && mkdir -p "/stackable/spark/jobs/"
COPY ../target/occurrence-download-spark-0.194.0-H3-SNAPSHOT.jar /stackable/spark/jobs/occurrence-download-spark.jar
