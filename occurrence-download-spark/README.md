# Occurrence Download Spark
This project enables de executions of GBIF downloads using Spark 3.3.0.
It supports both small and big downloads using Elasticsearch and Saprk SQL.

It also contains the definition of a Docker images that can be used to run
this project in a Docker or Kubernetes environment.

# Build

To build the project including the Docker image run:
``
mvn clean package install
``

To push the image to currently connected Docker registry run:

To login into a Docker registry
``
docker login
``

To push the image into the Docker Registry.
``
mvn clean package install docker:push
``

By default, the project pushes the image to the Docker registry docker.gbif.org


