#!/usr/bin/env bash
#Simple script for pushing a image containing the named modules build artifact
set -e

MODULE="occurrence-download-spark"
docker build -f ./${MODULE}/docker/Dockerfile ./${MODULE} --build-arg JAR_FILE=${MODULE}-${POM_VERSION}-shaded.jar -t docker.gbif.org/${MODULE}:${POM_VERSION}
docker push docker.gbif.org/${MODULE}:${POM_VERSION}
if [[ $IS_M2RELEASEBUILD = true ]]; then
  echo "Updated latest tag pointing to the newly released docker.gbif.org/${MODULE}:${POM_VERSION}"
  docker tag docker.gbif.org/${MODULE}:${POM_VERSION} docker.gbif.org/${MODULE}:latest
  docker push docker.gbif.org/${MODULE}:latest
fi
