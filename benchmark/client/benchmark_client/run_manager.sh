#!/bin/bash

# Starts the distributed benchmark manager for one application/scenario.
# Usage: ./run_manager.sh <application> [dialect1,dialect2,...] [port]
# Example: ./run_manager.sh aviation
# Example: ./run_manager.sh aviation DuckDB,ClickHouse 8787

if [ -z "$1" ]; then
  echo "Usage: $0 <application> [dialects] [port]"
  exit 1
fi

export MAVEN_OPTS="-Dfile.encoding=UTF-8"
mvn clean compile

mvn exec:java \
  -Dexec.mainClass="benchmark_client.distributed.ManagerMain" \
  -Dexec.args="$1 $2 $3" \
  -Dexec.cleanupDaemonThreads=false \
  -Dexec.systemPropertyVariables="file.encoding=UTF-8"
