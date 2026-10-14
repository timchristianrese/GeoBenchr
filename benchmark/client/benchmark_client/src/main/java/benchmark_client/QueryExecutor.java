package benchmark_client;

public interface QueryExecutor {
    void execute(String sql, String queryName, String dbType, String application, String runID);

    /**
     * Stops the query that is running right now. Called by BenchmarkRunner (from another thread) when a
     * query has run past the timeout; execute() then fails and returns. Default: nothing to stop.
     */
    default void cancel() {
    }

    /**
     * Stops only the query that the given worker thread is running (2026-10-14, for runs with several client
     * threads). Default: cancel(), for executors that cannot stop a single query.
     */
    default void cancel(Thread worker) {
        cancel();
    }

    /**
     * (2026-10-14) How many times cancel() has also ended the queries of the other client threads (a DuckDB
     * server restart). BenchmarkRunner compares it before and after an attempt. Default 0: cancel() stops
     * only the timed-out query, so nothing else is ever interrupted.
     */
    default long restarts() {
        return 0;
    }

    /**
     * (2026-10-14) Thrown by execute() instead of logging an error when the query failed because cancel()
     * restarted the server for another thread's timed-out query. BenchmarkRunner runs such a query again
     * (or logs this as its ERROR when it was the timed-out query itself or the retries are used up).
     */
    class InterruptedByRestart extends RuntimeException {
        public InterruptedByRestart(String message, Throwable cause) {
            super(message, cause);
        }
    }
}