#!/bin/bash

# Starts a distributed benchmark worker that pulls tasks from a running manager (run_manager.sh).
# Run this on as many machines/processes as you want load spread across.
# Usage: ./run_worker.sh <managerHost:port> [numThreads] [dialect1,dialect2,...]
# Example: ./run_worker.sh geobenchr-client-1:8787
# Example: ./run_worker.sh geobenchr-client-1:8787 4 DuckDB,ClickHouse

if [ -z "$1" ]; then
  echo "Usage: $0 <managerHost:port> [numThreads] [dialects]"
  exit 1
fi

export MAVEN_OPTS="-Dfile.encoding=UTF-8"
mvn clean compile

mvn exec:java \
  -Dexec.mainClass="benchmark_client.distributed.WorkerMain" \
  -Dexec.args="$1 $2 $3" \
  -Dexec.cleanupDaemonThreads=false \
  -Dexec.systemPropertyVariables="file.encoding=UTF-8"
