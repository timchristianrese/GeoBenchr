#!/bin/bash

# Usage: ./install_benchmark_client.sh <REMOTE_IP>

set -e

REMOTE_IP="${1:-10.35.0.20}"  # Default IP if not provided
if [ -z "$REMOTE_IP" ]; then
    echo "❌ Please provide the remote IP address."
    exit 1
fi

SSH_USER="tim"  # Change this if needed

echo "Connecting to $REMOTE_IP to install benchmark client..."

ssh "$SSH_USER@$REMOTE_IP" bash << 'EOF'
set -e

echo "Updating package lists..."
sudo apt update

echo "Installing Java, Maven, Git, and other dependencies..."
sudo apt install -y openjdk-11-jdk maven git wget unzip curl

# Set JAVA_HOME
export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
echo "export JAVA_HOME=$JAVA_HOME" >> ~/.bashrc

# Versions
SPARK_VERSION="3.5.5"
HADOOP_VERSION="3"
SCALA_VERSION="2.12"
SEDONA_VERSION="1.7.1"

echo "Installing Apache Spark ${SPARK_VERSION}..."
if [ ! -d "/opt/spark" ]; then
  wget -q https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
  tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
  sudo mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark
  echo "export SPARK_HOME=/opt/spark" >> ~/.bashrc
  echo "export PATH=\$SPARK_HOME/bin:\$PATH" >> ~/.bashrc
fi

export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$PATH

echo "📁 Cloning and building Sedona..."
if [ ! -d "$HOME/sedona" ]; then
  git clone https://github.com/apache/sedona.git
  cd sedona
  git checkout master
  mvn clean install -DskipTests
  cd ..
fi

mkdir -p ~/sedona-benchmark
cd ~/sedona-benchmark

echo "📝 Writing benchmark Java file..."

cat <<EOL > SedonaBenchmark.java
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.sedona.spark.SedonaContext;

public class SedonaBenchmark {
    public static void main(String[] args) {

        // Build base Spark config
        SparkSession config = SedonaContext.builder()
            .master("local[*]") // Optional: Remove or replace if running on Spark cluster
            .appName("SedonaBenchmark")
            .getOrCreate();

        // Activate Sedona SQL extensions
        SparkSession sedona = SedonaContext.create(config);

        long start = System.currentTimeMillis();

        // Load CSV data
        Dataset<Row> rawDf = sedona.read()
            .format("csv")
            .option("delimiter", "\t")
            .option("header", "false")
            .load("/home/tim/data/aviation/NRW_HIGH_0123.csv");

        rawDf.createOrReplaceTempView("rawdf");

        // Build a Point geometry from the 5th and 6th columns
        String sql = SELECT ST_GeomFromWKT(_c0) AS countyshape, _c
        // Run sample query
        rawDf.show();

        long end = System.currentTimeMillis();
        System.out.println("Query took " + (end - start) + " ms");

        // Stop Spark
        sedona.stop();
    }
}
EOL

echo "Benchmark Java file written to ~/sedona-benchmark/SedonaBenchmark.java"
EOF

echo "Remote setup complete on $REMOTE_IP. You can now ssh in and run the benchmark."
