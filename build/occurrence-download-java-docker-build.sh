#!/usr/bin/env bash
#Simple script for pushing a image containing the named modules build artifact
set -e

MODULE="occurrence-download"

POM_VERSION=$1
IMAGE=docker.gbif.org/${MODULE}-java:${POM_VERSION}
IMAGE_LATEST=docker.gbif.org/${MODULE}:latest

echo "Building Docker image module:version - ${MODULE}:${POM_VERSION}"
docker build -f ./${MODULE}/docker/Dockerfile-java ./${MODULE} --build-arg JAR_FILE=${MODULE}-${POM_VERSION}-shaded.jar -t ${IMAGE}

echo "Pushing Docker image to the repository"
docker push ${IMAGE}
if [[ $IS_M2RELEASEBUILD = true ]]; then
  echo "Updated latest tag pointing to the newly released ${IMAGE}"
  docker tag ${IMAGE} ${IMAGE_LATEST}
  docker push ${IMAGE_LATEST}
fi

echo "Removing local Docker image: ${IMAGE}"
docker rmi ${IMAGE}

if [[ $IS_M2RELEASEBUILD = true ]]; then
  echo "Removing local Docker image with latest tag: ${IMAGE_LATEST}"
  docker rmi ${IMAGE_LATEST}
fi
