package benchmark_client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

public class BenchmarkRunner {
    // FIX: some queries against the largest tables (anything above ~1B points -- ride_points,
    // crossing_points at full scale) can run far longer than a benchmark run should tolerate for
    // a single query, and future.get() below used to block indefinitely -- one slow/hung query
    // could stall the whole run rather than being cut off and counted as a timeout. Defaulted to
    // 10 minutes; override via the other constructor for a dataset-specific cutoff (e.g. a
    // shorter one for small reference-table-only queries, longer for the largest point tables).
    public static final long DEFAULT_TIMEOUT_SECONDS = 600;
    // after cancelling a query, how long to wait for its execute() to return before moving on
    static final long CANCEL_GRACE_SECONDS = 300;
    // (2026-10-14) with several client threads, DuckDB's server restart for one timed-out query also ends the
    // other threads' queries (17 of them for 3 timeouts in an 8-thread trial), which lost those samples. Such
    // a query is run again once the server is back, as a new execution with its own timing and timeout;
    // at most MAX_ATTEMPTS attempts in total, so a query that keeps being interrupted cannot hold its thread for long.
    static final int MAX_ATTEMPTS = 2;

    private final QueryExecutor executor;
    private final List<QueryConfig> queries;
    private final int numThreads;
    private final String dbType;
    private final String application;
    private final String runID;
    private final long timeoutSeconds;

    public BenchmarkRunner(QueryExecutor executor, List<QueryConfig> queries, int numThreads, String runID) {
        this(executor, queries, numThreads, "PostgreSQL", "aviation", runID, DEFAULT_TIMEOUT_SECONDS);
    }

    public BenchmarkRunner(QueryExecutor executor, List<QueryConfig> queries, int numThreads, String dbType, String application, String runID) {
        this(executor, queries, numThreads, dbType, application, runID, DEFAULT_TIMEOUT_SECONDS);
    }

    public BenchmarkRunner(QueryExecutor executor, List<QueryConfig> queries, int numThreads, String dbType, String application, String runID, long timeoutSeconds) {
        this.executor = executor;
        this.queries = queries;
        this.numThreads = numThreads;
        this.dbType = dbType; // Custom database type
        this.application = application;
        this.runID = runID;
        this.timeoutSeconds = timeoutSeconds;
    }

    public void run() throws InterruptedException {
    ExecutorService pool = Executors.newFixedThreadPool(numThreads);
    List<Future<?>> futures = new ArrayList<>();
    List<QueryConfig> queryByFuture = new ArrayList<>();
    List<String> tagByFuture = new ArrayList<>();
    List<AtomicLong> startByFuture = new ArrayList<>();
    List<java.util.concurrent.atomic.AtomicReference<Thread>> threadByFuture = new ArrayList<>();
    List<AtomicLong> epochByFuture = new ArrayList<>();
    List<java.util.concurrent.atomic.AtomicBoolean> timedOutByFuture = new ArrayList<>();
    // held by the waiting thread while it cancels a query, so the next query cannot start before the
    // system has been brought back (DuckDB's cancel restarts the server)
    Object gate = new Object();
    java.util.Map<String, Integer> repetitions = new java.util.HashMap<>();

    System.out.println("Running benchmark with " + numThreads + " threads, loaded " + queries.size() + " queries, " + timeoutSeconds + "s per-query timeout.");

    for (QueryConfig query : queries) {
        // a system without a translation of this query has an empty SQL string in its YAML; running
        // it would "succeed" in ~10 ms and show up as a (bogus) timing, so it's skipped instead
        if (query.getSql() == null || query.getSql().trim().isEmpty()) {
            System.out.println("Skipping " + query.getName() + " on " + dbType + ": no query for this system");
            continue;
        }
        int rep = repetitions.merge(query.getName(), 1, Integer::sum);
        String tag = String.format("rep=%d sql=%08x", rep, query.getSql().hashCode());
        AtomicLong started = new AtomicLong();
        java.util.concurrent.atomic.AtomicReference<Thread> worker = new java.util.concurrent.atomic.AtomicReference<>();
        // (2026-10-14) executor.restarts() when the current attempt started; set with `started`, under the gate
        AtomicLong epoch = new AtomicLong();
        // set (under the gate) before this query is cancelled for running past the timeout: it is not retried
        java.util.concurrent.atomic.AtomicBoolean timedOut = new java.util.concurrent.atomic.AtomicBoolean();
        Future<?> future = pool.submit(() -> {
            for (int attempt = 1; ; attempt++) {
                // (2026-10-14) a retry waits here like any new query: the gate is held while the server
                // restarts, so it starts on the restarted server, with its own start time and epoch
                synchronized (gate) {
                    worker.set(Thread.currentThread());
                    epoch.set(executor.restarts());
                    started.set(System.nanoTime());
                }
                QueryLogger.setTag(tag);
                try {
                    executor.execute(query.getSql(), query.getName(), dbType, application, runID);
                } catch (QueryExecutor.InterruptedByRestart e) {
                    // (2026-10-14) ended by a restart for another query's timeout: run it again, logging
                    // this attempt only as RETRIED; the timed-out query itself stays a TIMEOUT
                    if (!timedOut.get() && attempt < MAX_ATTEMPTS) {
                        QueryLogger.logRetried(dbType, application, query.getName(), runID, e.getMessage(), attempt);
                        continue;
                    }
                    QueryLogger.logError(dbType, application, query.getName(), runID, e);
                } catch (Throwable e) {
                    // the executors log their own Exceptions; anything that escapes them (an Error such
                    // as OutOfMemoryError) is logged here, so no execution can go missing from the results
                    System.err.println("Error executing query " + query.getName() + ": " + e +
                                       " in thread " + Thread.currentThread().getName());
                    QueryLogger.logError(dbType, application, query.getName(), runID, new RuntimeException(e.toString(), e));
                } finally {
                    QueryLogger.clearTag();
                }
                break;
            }
        });
        futures.add(future);
        queryByFuture.add(query);
        tagByFuture.add(tag);
        startByFuture.add(started);
        threadByFuture.add(worker);
        epochByFuture.add(epoch);
        timedOutByFuture.add(timedOut);
    }

    System.out.println("All queries submitted to the executor.");
    System.out.println("Waiting for all threads to finish execution...");

    // Wait for all tasks to complete, cutting off any single query that runs past the configured
    // timeout (see DEFAULT_TIMEOUT_SECONDS above).
    // FIX (2026-10-09): the timeout used to start when this loop reached a query, not when the query
    // started, and a timed-out query was only interrupted on the client (future.cancel), which no JDBC
    // driver notices -- the query kept running on the server and the single worker thread stayed
    // blocked on it, while every query queued behind it "timed out" here and was then cancelled
    // before it ever ran. Now each query's time counts from its own start, and a query past the
    // timeout is stopped on the server through executor.cancel() (Statement.cancel, a SpaceTime
    // worker kill, Spark job cancellation, a DuckDB server restart) before the next one may start.
    for (int i = 0; i < futures.size(); i++) {
        Future<?> future = futures.get(i);
        QueryConfig query = queryByFuture.get(i);
        AtomicLong started = startByFuture.get(i);
        AtomicLong epoch = epochByFuture.get(i);
        try {
            while (true) {
                try {
                    future.get(1, TimeUnit.SECONDS);
                    break;
                } catch (TimeoutException e) {
                    if (pastTimeout(started, epoch)) {
                        synchronized (gate) {
                            // (2026-10-14) checked again under the gate, where no attempt can start: an
                            // attempt ended by a restart is being retried, and its time counts from the retry
                            if (!pastTimeout(started, epoch)) {
                                continue;
                            }
                            QueryLogger.logTimeout(dbType, application, query.getName(), runID, timeoutSeconds * 1000, tagByFuture.get(i));
                            timedOutByFuture.get(i).set(true);
                            try {
                                // only this query: with several client threads the others keep running
                                executor.cancel(threadByFuture.get(i).get());
                            } catch (Throwable t) {
                                System.err.println("Cancelling " + query.getName() + " failed: " + t);
                            }
                            try {
                                future.get(CANCEL_GRACE_SECONDS, TimeUnit.SECONDS);
                            } catch (TimeoutException stillRunning) {
                                System.err.println(query.getName() + " still running " + CANCEL_GRACE_SECONDS
                                        + " s after cancel; the next query waits for it");
                            } catch (ExecutionException ignored) {
                                // logged by the task
                            }
                        }
                        break;
                    }
                }
            }
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

    // (2026-10-14) whether the running attempt has passed the timeout. An attempt that started before the last
    // server restart is not: the restart ended it, and it is about to fail and be retried (or logged), so the
    // first attempt's time and the restart wait never count against the retry. Executors without restarts
    // always report 0, so for them this is the plain check it was before.
    private boolean pastTimeout(AtomicLong started, AtomicLong epoch) {
        long e = epoch.get();
        long t0 = started.get();
        return t0 != 0 && e == executor.restarts()
                && System.nanoTime() - t0 >= TimeUnit.SECONDS.toNanos(timeoutSeconds);
    }
}