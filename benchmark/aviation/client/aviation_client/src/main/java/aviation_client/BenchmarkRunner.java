package aviation_client;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class BenchmarkRunner {
    private final QueryExecutor executor;
    private final List<QueryConfig> queries;
    private final int numThreads;

    public BenchmarkRunner(QueryExecutor executor, List<QueryConfig> queries, int numThreads) {
        this.executor = executor;
        this.queries = queries;
        this.numThreads = numThreads;
    }

    public void run() throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(numThreads);
        System.out.println("Running benchmark with " + numThreads + " threads, loaded " + queries.size() + " queries.");

        for (QueryConfig query : queries) {
            pool.submit(() -> {
                try{
                    executor.execute(query.getSql(), query.getName());
                } 
                catch (Exception e) {
                    System.err.println("Error executing query " + query.getName() + ": " + e.getMessage()+ "in thread " + Thread.currentThread().getName());
                }
            });
        }
        System.out.println("All queries submitted to the executor.");
        pool.shutdown();
        pool.awaitTermination(1, TimeUnit.HOURS);
        System.out.println("Benchmark completed. All threads finished execution.");
        System.out.println("Exiting BenchmarkRunner.");
    }
}