# Installation  
The benchmark client is split into the `client` itself and necessary `configuration` files. 
This installation assumes that you have already set up the database systems which you would like to benchmark.  
Please notice that the benchmark client for SedonaDB is managed separately, as currently the interaction patterns for that platform are limited to Python and R. This can be found in the notebook here in the folder. For all other platforms, the client is centralized. To run SedonaDB experiments, please check further below in this readme

To run a benchmark, the following steps are required (assuming completion of the `server`and `data` installation processes):

 - Check the configuration files found in scenario folders under `configuration` to adjust the queries, repetitions, and which queries you want executed. Each query in `combinedBenchConf` can be toggled to `true` or `false`, which either enables or disables its execution during the benchmark experiment. The `repetition` parameter changes how often the query is repeated with varying parameters. 
 - After adjusting the files, run the python script `generateParameterVariations` found in the `scripts` folder of each scenario (from the `scripts` folder). This actually creates the queries with parameters, which can then be found in the `queries` folder, split into separate files based on the database system. These files are also automatically transferred into the `client` folder, as they are used by it to run the benchmark. 
 - Adjust the client to reflect which database systems you want to evaluate, and the IP/hostname of your database system (Line 27 and 38). In `benchmark/client/benchmark_client/src/main/java/benchmark_client/BenchmarkClient.java`, comment or uncomment the code blocks which you want to run experiments for so that the client only runs the desired ones.For example, assuming you do not want to run TimescaleDB experiments, comment the code section 81-84. Save your changes afterwards. 
 - Now we build the benchmark client. For this, Maven is required. From this folder (`benchmark`), run the following commands to build the benchmark client
 ```
 cd client/benchmark_client
 mvn clean install
 ```
- Afterwards, you can choose which benchmark you want to run simply by running 
```
./run_benchmark.sh <ais|aviation|cycling>
Example: ./run_benchmark.sh ais
```
- You can also run all benchmarks twice
Results can be found in this folder, with the following identifiers:
```
<platform>query_results_<random_id>.log
Example: PostGISquery_results_4084d.log 
```

## SedonaDB
The Jupyter Notebook found here in the folder contains all steps for SedonaDB, including benchmark execution and data loading. 

