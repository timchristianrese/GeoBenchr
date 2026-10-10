#!/bin/bash
# Run ScenarioFunctionalityCheck for a given scenario from nst-inserter against nst-1's SUTs.
# Usage: ./run_scenario_check.sh <scenario> [comma,separated,engines]
set -euo pipefail
SCENARIO="${1:-cycling}"
ENGINES="${2:-}"
DEPS_DIR="$HOME/benchmark_client_deps"

export SUT_HOST="10.1.0.11"
export SUT_DB_PASSWORD="postgres"
export SPACETIME_PYTHON="$HOME/bench/.venv/bin/python"
export SPACETIME_RUNNER_SCRIPT="$HOME/bench/spacetime_query_runner.py"
export SPACETIME_PORT="31340"
export SPARK_MASTER="yarn"
export SPARK_HOME="/opt/spark"

java \
  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
  --add-opens=java.base/sun.security.action=ALL-UNNAMED \
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  -cp "$DEPS_DIR/benchmark_client-1.0-SNAPSHOT.jar:$DEPS_DIR/*" \
  benchmark_client.ScenarioFunctionalityCheck "$SCENARIO" ${ENGINES:+"$ENGINES"}
