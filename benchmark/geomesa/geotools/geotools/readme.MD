# GeoTools API Benchmark for GeoMesa
This benchmark setup currently does not function as it should
## Requirements
Maven
Java 
## Installation
`mvn clean install``
## Running the benchmark
You can then run the main class by doing the following, you'll have to replace the IP:
```
mvn exec:java -Dexec.mainClass="com.example.Main" -Dexec.args="test root test example <GCP_IP_HERE>"
```