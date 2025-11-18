package benchmark_client;

public interface QueryExecutor {
    void execute(String sql, String queryName, String dbType, String application, String runID);
}