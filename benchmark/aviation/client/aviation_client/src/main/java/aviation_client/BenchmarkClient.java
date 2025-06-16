package aviation_client;

import java.util.List;

public class BenchmarkClient {
    public static void main(String[] args) throws Exception {
        //String yamlPath = "postgisSQL_queries.yaml";
        String yamlPath = "sedonaSQL_queries.yaml";
        //String yamlPath = "mobilitydb_queries.yaml";
        int numThreads = Integer.parseInt(System.getenv().getOrDefault("NUM_THREADS", "1"));
        
        List<QueryConfig> queries = QueryDispatcher.loadQueries(yamlPath);
        // PostgreSQLExecutor executor = new PostgreSQLExecutor(
        //     "jdbc:postgresql://130.149.253.149:5432/", "postgres", "test"
        // );
        String sparkMasterUrl = "spark://server-peter-lan.3s.tu-berlin.de:7077";
        SedonaExecutor executor = new SedonaExecutor(sparkMasterUrl);
        
        //QueryExecutor executor = new MockExecutor();

        System.out.println("Setup complete. Starting benchmark...");

        BenchmarkRunner runner = new BenchmarkRunner(executor, queries, numThreads);
        runner.run();
    }
}
