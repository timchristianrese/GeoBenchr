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
        int chunkSize = (int) Math.ceil((double) queries.size() / numThreads);
        for (int i = 0; i < numThreads; i++) {
            System.out.println("Processing chunk " + (i + 1) + " of size " + chunkSize);
            int start = i * chunkSize;
            int end = Math.min(start + chunkSize, queries.size());
            List<QueryConfig> subset = queries.subList(start, end);
            // Print the first query in the subset for debugging
            pool.submit(() -> {
                for (QueryConfig query : subset) {
                    System.out.printf("Thread %s executing query: %s%n", Thread.currentThread().getName(), query.getName());
                    executor.execute(query.getSql(), query.getName());
                }
            });
        }

        pool.shutdown();
        pool.awaitTermination(1, TimeUnit.HOURS);
    }
}