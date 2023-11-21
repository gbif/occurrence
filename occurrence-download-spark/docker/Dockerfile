# syntax = docker/dockerfile:1.3

FROM docker.gbif.org/gbif/builder-image:1.0.0 as builder

# Correct branch must be passed as a "--build-arg BUILD_BRANCH=unknown" value
ARG BUILD_BRANCH=unknown
# number of
ARG MAVEN_THREADS=1

USER gbif

RUN echo "Using git branch: $BUILD_BRANCH"

RUN mkdir -p /home/gbif/.m2/repository

RUN git clone https://github.com/gbif/occurrence.git && \
    cd occurrence && \
    git fetch && \
    git checkout ${BUILD_BRANCH} && \
    cd occurrence-download-spark && \
    mvn -f pom.xml clean package -DskipTests -T ${MAVEN_THREADS} -am

FROM docker.gbif.org/gbif/base-image:1.0.0
USER root
RUN mkdir /jobs
COPY --from=builder --chown=1000:1000 /home/gbif/occurrence/occurrence-download-spark/target/occurrence-download-spark-*-shaded.jar /jobs/occurrence-download-spark.jar
USER gbif
