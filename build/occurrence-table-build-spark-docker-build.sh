#!/usr/bin/env bash
#Simple script for pushing a image containing the named modules build artifact
set -e

MODULE="occurrence-table-build-spark"

POM_VERSION=$(mvn -q -Dexec.executable="echo" -Dexec.args='${project.version}' --non-recursive exec:exec)

echo "Building Docker image module:version - ${MODULE}:${POM_VERSION}"
docker build -f ./${MODULE}/docker/Dockerfile ./${MODULE} --build-arg JAR_FILE=${MODULE}-${POM_VERSION}-shaded.jar -t docker.gbif.org/${MODULE}:${POM_VERSION}

echo "Pushing Docker image to the repository"
docker push docker.gbif.org/${MODULE}:${POM_VERSION}
if [[ $IS_M2RELEASEBUILD = true ]]; then
  echo "Updated latest tag pointing to the newly released docker.gbif.org/${MODULE}:${POM_VERSION}"
  docker tag docker.gbif.org/${MODULE}:${POM_VERSION} docker.gbif.org/${MODULE}:latest
  docker push docker.gbif.org/${MODULE}:latest
fi
