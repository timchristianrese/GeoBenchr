#!/bin/bash

# Fail if no argument is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <mode>"
  exit 1
fi

MODE="$1"
sudo systemctl restart postgresql
# Ensure UTF-8 Maven encoding
export MAVEN_OPTS="-Dfile.encoding=UTF-8"
mvn clean compile
# Run Maven with the passed argument
mvn exec:java \
  -Dexec.mainClass="benchmark_client.BenchmarkClient" \
  -Dexec.args="$MODE" \
  -Dexec.cleanupDaemonThreads=false \
  -Dexec.systemPropertyVariables="file.encoding=UTF-8"

