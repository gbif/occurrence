# syntax = docker/dockerfile:1.3

FROM docker.gbif.org/gbif/builder-image:1.0.0 as builder
# standard branch currently just test for my own stackable test branch
ARG BUILD_BRANCH=feature/stackable-hadoop-3-test
# number of
ARG MAVEN_THREADS=1

USER gbif

RUN mkdir -p /home/gbif/.m2/repository

RUN --mount=type=cache,mode=0777,target=/home/gbif/.m2,uid=1000,gid=1000,z git clone https://github.com/gbif/occurrence.git && \
    cd occurrence && \
    git fetch && \
    git checkout ${BUILD_BRANCH} && \
    cd occurrence-table-build-spark && \
    mvn -f pom.xml clean package -T ${MAVEN_THREADS} -am

FROM docker.gbif.org/gbif/base-image:1.0.0
USER root
RUN mkdir /jobs
COPY --from=builder --chown=1000:1000 /home/gbif/occurrence/occurrence-table-build-spark/target/occurrence-table-build-spark-*-shaded.jar /jobs/occurrence-table-build-spark.jar
USER gbif
