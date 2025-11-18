package cycling_client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
    List<Future<?>> futures = new ArrayList<>();

    System.out.println("Running benchmark with " + numThreads + " threads, loaded " + queries.size() + " queries.");

    for (QueryConfig query : queries) {
        Future<?> future = pool.submit(() -> {
            try {
                executor.execute(query.getSql(), query.getName());
            } catch (Exception e) {
                System.err.println("Error executing query " + query.getName() + ": " + e.getMessage() +
                                   " in thread " + Thread.currentThread().getName());
            }
        });
        futures.add(future);
    }

    System.out.println("All queries submitted to the executor.");
    System.out.println("Waiting for all threads to finish execution...");

    // Wait for all tasks to complete
    for (Future<?> future : futures) {
        try {
            future.get(); 
        } catch (ExecutionException e) {
            System.err.println("Task execution failed: " + e.getCause());
        }
    }

    pool.shutdown();
    if (!pool.awaitTermination(1, TimeUnit.HOURS)) {
        System.err.println("Executor did not terminate in the specified time.");
        pool.shutdownNow();
    }

    System.out.println("Benchmark completed. All threads finished execution.");
    System.out.println("Exiting BenchmarkRunner.");
}
}