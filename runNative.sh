#!/bin/bash
#set -x

startTimer() {
  start=$(date +'%s')
}
stopTimer() {
  now=$(date +'%s')
  diff=$((now-start))
  echo "$1 $diff sec"
}

startTimer
cd aggregator &&
 mvn package -Pnative -Dquarkus.native.container-build=true &&
  docker build -f src/main/docker/Dockerfile.native -t quarkus/quarkus-kafka-streams-aggregator .
cd ..
stopTimer "Native compile aggregator"

startTimer
cd producer &&
mvn package -Pnative -Dquarkus.native.container-build=true &&
docker build -f src/main/docker/Dockerfile.native -t quarkus/quarkus-kafka-streams-producer .
cd ..
stopTimer "Native compile producer"

startTimer
#QUARKUS_MODE=native docker-compose up --build -d
QUARKUS_MODE=native docker-compose up --build -d --scale aggregator=3
stopTimer "Wake up runtime"
