#!/bin/bash

# Usage: ./trigger_benchmark.sh <REMOTE_IP> <SPARK_MASTER_IP>
# REMOTE_IP is where the benchmark client is installed
# SPARK_MASTER_IP is where the Spark master is running

set -e

REMOTE_IP="${1:-10.35.0.20}"
SPARK_MASTER_IP="${1:-10.35.0.22}"
SSH_USER="tim"  # Change this if needed

if [ -z "$REMOTE_IP" ] || [ -z "$SPARK_MASTER_IP" ]; then
  echo "❌ Usage: $0 <REMOTE_IP> <SPARK_MASTER_IP>"
  exit 1
fi

echo "🚀 Triggering benchmark on $REMOTE_IP..."

ssh "$SSH_USER@$REMOTE_IP" bash << 'EOF'
set -e

export SPARK_HOME="/opt/spark"
export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
export PATH=\$SPARK_HOME/bin:\$JAVA_HOME/bin:\$PATH

cd ~/sedona-benchmark

echo "🛠 Compiling benchmark..."
mkdir -p target
# Path to Spark and Sedona jars
SPARK_HOME=/opt/spark
SEDONA_HOME=~/sedona

# On the remote machine
SPARK_JARS=$(find /opt/spark/jars -name '*.jar' | tr '\n' ':')
SEDONA_JARS=$(find ~/sedona -name 'sedona-spark-shaded*.jar' | tr '\n' ':')

ALL_JARS="$SPARK_JARS$SEDONA_JARS"
echo "Using jars: $ALL_JARS"
javac -cp "$ALL_JARS" SedonaBenchmark.java -d target




echo "🚦 Submitting job to Spark master at spark://${SPARK_MASTER_IP}:7077..."

\$SPARK_HOME/bin/spark-submit \
  --class SedonaBenchmark \
  --master spark://${SPARK_MASTER_IP}:7077 \
  --jars \$(find ~/sedona -name 'sedona-spark-shaded*.jar' | head -n 1) \
  --driver-class-path target \
  target/SedonaBenchmark.class
EOF

echo "Benchmark run completed on $REMOTE_IP"
