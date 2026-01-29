package benchmark_client;

import java.util.List;

public class BenchmarkClient {
    public static void main(String[] args) throws Exception {
        //File encoding to handle german characters in data
        System.setProperty("file.encoding", "UTF-8");
        int numThreads = Integer.parseInt(System.getenv().getOrDefault("NUM_THREADS", "1"));
        //generate random 5 letter ID for benchmark run
        String runID = java.util.UUID.randomUUID().toString().substring(0, 5);

        //Select application
        String application = args.length > 0 ? args[0] : "aviation";
        System.out.println("Starting benchmark for application: " + application + " with " + numThreads + " threads.");

        //Initialize variables
        String yamlPath;
        List<QueryConfig> queries;
        BenchmarkRunner runner;


        //SpaceTime
        yamlPath = application + "/spaceTimeSQL_queries.yaml";
        SpaceTimeExecutor spaceTimeExecutor = new SpaceTimeExecutor(
                "141.23.28.216:31339",
        "mireo","root","@2e9R]3]=O"
        );
        queries = QueryDispatcher.loadQueries(yamlPath);
        System.out.println("Setup complete. Starting benchmark...");
        runner = new BenchmarkRunner(spaceTimeExecutor, queries, numThreads, "SpaceTime", application, runID);
        runner.run();

    


        PostgreSQLExecutor postgresqlExecutor = new PostgreSQLExecutor(
              "jdbc:postgresql://server-peter-lan.3s.tu-berlin.de:5432/", "postgres", "test", application
        );
        //PostGIS
        yamlPath = application + "/" + "postgisSQL_queries.yaml";
        queries = QueryDispatcher.loadQueries(yamlPath);
        runner = new BenchmarkRunner(postgresqlExecutor, queries, numThreads, "PostGIS", application, runID);
        runner.run();

        //MobilityDB
        yamlPath = application + "/" + "mobilitydb_queries.yaml";
        queries = QueryDispatcher.loadQueries(yamlPath);
        runner = new BenchmarkRunner(postgresqlExecutor, queries, numThreads, "MobilityDB", application, runID);
        runner.run();

        // //MobilityDB Time Partitioned
        // yamlPath = application + "/" + "mobilitydb_queries_time_partitioned.yaml";
        // queries = QueryDispatcher.loadQueries(yamlPath);
        // runner = new BenchmarkRunner(postgresqlExecutor, queries, numThreads, "MobilityDB_Time_Partitioned", application, runID);
        // runner.run();

        // //MobilityDB Space Partitioned
        // yamlPath = application + "/" + "mobilitydb_queries_space_partitioned.yaml";
        // queries = QueryDispatcher.loadQueries(yamlPath);
        // runner = new BenchmarkRunner(postgresqlExecutor, queries, numThreads, "MobilityDB_Space_Partitioned", application, runID);
        // runner.run();

        //TimescaleDB
        yamlPath = application + "/" + "tsdb_queries.yaml";
        queries = QueryDispatcher.loadQueries(yamlPath);
        runner = new BenchmarkRunner(postgresqlExecutor, queries, numThreads, "TimescaleDB", application, runID);
        runner.run();


        
        //Sedona (Not SedonaDB, currently not functional)
        // yamlPath = application + "/" + "sedonaSQL_queries.yaml";
        // String sparkMasterUrl = "spark://server-peter-lan.3s.tu-berlin.de:7077";
        // SedonaExecutor sedonaExecutor = new SedonaExecutor(sparkMasterUrl);
        // queries = QueryDispatcher.loadQueries(yamlPath);
        // runner = new BenchmarkRunner(sedonaExecutor, queries, numThreads, "Sedona", application,);
        // runner.run();
        
        //GeoMesa
        // //yamlPath = application + "/" + "geomesaSQL_queries.yaml";
        // GeoMesaExecutor geoMesaExecutor = new GeoMesaExecutor(
        //         "test","server-peter-lan","root","test","example","flight_points",
        // );
        // queries = QueryDispatcher.loadQueries(yamlPath);
        // runner = new BenchmarkRunner(geoMesaExecutor, queries, numThreads, "GeoMesa", application, runID);
        // runner.run();
        
        //QueryExecutor executor = new MockExecutor();

        
    }
}
