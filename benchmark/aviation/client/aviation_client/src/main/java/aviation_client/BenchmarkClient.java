package aviation_client;

import java.util.List;

public class BenchmarkClient {
    public static void main(String[] args) throws Exception {
        String yamlPath = "spatialSQL_queries.yaml";
        int numThreads = Integer.parseInt(System.getenv().getOrDefault("NUM_THREADS", "4"));
        
        List<QueryConfig> queries = QueryDispatcher.loadQueries(yamlPath);
         PostgreSQLExecutor executor = new PostgreSQLExecutor(
             "jdbc:postgresql://130.149.253.149:5432/", "postgres", "test"
         );
        //QueryExecutor executor = new MockExecutor();

        System.out.println("Setup complete. Starting benchmark...");

        BenchmarkRunner runner = new BenchmarkRunner(executor, queries, numThreads);
        runner.run();
    }
}
