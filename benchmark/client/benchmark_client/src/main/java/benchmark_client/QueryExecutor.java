package benchmark_client;

public interface QueryExecutor {
    void execute(String sql, String queryName, String dbType, String application, String runID);

    /**
     * Stops the query that is running right now. Called by BenchmarkRunner (from another thread) when a
     * query has run past the timeout; execute() then fails and returns. Default: nothing to stop.
     */
    default void cancel() {
    }
}