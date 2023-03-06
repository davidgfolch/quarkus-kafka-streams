#!/bin/bash
set -x

cd aggregator &&
 mvn package -Pnative -Dquarkus.native.container-build=true &&
  docker build -f src/main/docker/Dockerfile.native -t quarkus/quarkus-kafka-streams-aggregator .
cd ..

cd producer &&
mvn package -Pnative -Dquarkus.native.container-build=true &&
docker build -f src/main/docker/Dockerfile.native -t quarkus/quarkus-kafka-streams-producer .

cd ..
QUARKUS_MODE=native docker-compose up --build
