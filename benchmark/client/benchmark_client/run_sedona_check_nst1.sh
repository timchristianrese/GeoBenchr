#!/bin/bash
# Run ScenarioFunctionalityCheck's Sedona engine only, from nst-1 itself -- Sedona's driver hits
# an unresolved "unread block data" closure-deserialization crash when launched from nst-inserter
# (see docs/database-state.md), so this carve-out runs it where it's known to work while the other
# 5 engines run from nst-inserter via run_scenario_check.sh.
# Usage: ./run_sedona_check_nst1.sh <scenario>
set -euo pipefail
SCENARIO="${1:-cycling}"
DEPS_DIR="$HOME/benchmark_client_deps"

export SUT_HOST="10.1.0.11"
export SUT_DB_PASSWORD="postgres"
export SPACETIME_PYTHON="$HOME/bench/.venv/bin/python"
export SPACETIME_RUNNER_SCRIPT="$HOME/bench/spacetime_query_runner.py"
export SPACETIME_PORT="31340"
export SPARK_MASTER="yarn"
export SPARK_HOME="/opt/spark"

/opt/java/latest/bin/java \
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
  benchmark_client.ScenarioFunctionalityCheck "$SCENARIO" Sedona
