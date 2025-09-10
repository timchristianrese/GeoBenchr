package aviation_client;

import java.util.List;

public class BenchmarkClient {
    public static void main(String[] args) throws Exception {
        int numThreads = Integer.parseInt(System.getenv().getOrDefault("NUM_THREADS", "4"));

        String yamlPath;
        List<QueryConfig> queries;
        BenchmarkRunner runner;
        //SpaceTime
        yamlPath = "spaceTimeSQL_queries.yaml";
        SpaceTimeExecutor spaceTimeExecutor = new SpaceTimeExecutor(
                "141.23.28.216:31339",
        "mireo","root","@2e9R]3]=O"
        );
        queries = QueryDispatcher.loadQueries(yamlPath);
        System.out.println("Setup complete. Starting benchmark...");
        runner = new BenchmarkRunner(spaceTimeExecutor, queries, numThreads, "SpaceTime");
        runner.run();

        // yamlPath = "sedonaSQL_queries.yaml";
        // String sparkMasterUrl = "spark://server-peter-lan.3s.tu-berlin.de:7077";
        // SedonaExecutor sedonaExecutor = new SedonaExecutor(sparkMasterUrl);
        // queries = QueryDispatcher.loadQueries(yamlPath);
        // runner = new BenchmarkRunner(sedonaExecutor, queries, numThreads, "Sedona");
        // runner.run();

        //PostGIS
        yamlPath = "postgisSQL_queries.yaml";
        PostgreSQLExecutor postgresqlExecutor = new PostgreSQLExecutor(
             "jdbc:postgresql://server-peter-lan.3s.tu-berlin.de:5432/", "postgres", "test"
         );
        queries = QueryDispatcher.loadQueries(yamlPath);
        runner = new BenchmarkRunner(postgresqlExecutor, queries, numThreads, "PostGIS");
        runner.run();

        //MobilityDB
        yamlPath = "mobilitydb_queries.yaml";
        queries = QueryDispatcher.loadQueries(yamlPath);
        runner = new BenchmarkRunner(postgresqlExecutor, queries, numThreads, "MobilityDB");
        runner.run();

        //TimescaleDB
        yamlPath = "tsdb_queries.yaml";
        queries = QueryDispatcher.loadQueries(yamlPath);
        runner = new BenchmarkRunner(postgresqlExecutor, queries, numThreads, "TimescaleDB");
        runner.run();


        
        
        
        //QueryExecutor executor = new MockExecutor();

        
    }
}
