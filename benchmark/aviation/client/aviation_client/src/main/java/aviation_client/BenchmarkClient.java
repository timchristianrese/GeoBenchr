package aviation_client;

import java.util.List;

public class BenchmarkClient {
    public static void main(String[] args) throws Exception {
        String yamlPath = "spatialSQL_queries.yaml";
        int numThreads = Integer.parseInt(System.getenv().getOrDefault("NUM_THREADS", "4"));
        
        List<QueryConfig> queries = QueryDispatcher.loadQueries(yamlPath);
        // PostgreSQLExecutor executor = new PostgreSQLExecutor(
        //     "jdbc:postgresql://localhost:5432/yourdb", "user", "password"
        // );
        QueryExecutor executor = new MockExecutor();

        System.out.println("Setup complete. Starting benchmark...");

        BenchmarkRunner runner = new BenchmarkRunner(executor, queries, numThreads);
        runner.run();
    }
}
