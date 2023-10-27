FROM alpine:latest
RUN apk update && \
    apk upgrade && \
    apk add bash

RUN apk add --no-cache bash && mkdir -p "/jobs/" && addgroup -S stackable && adduser -S stackable -G stackable
COPY --chown=stackable:stackable "./target/occurrence-table-build-spark-0.194.0-H3-SNAPSHOT.jar" "/jobs/occurrence-table-build-spark.jar"
