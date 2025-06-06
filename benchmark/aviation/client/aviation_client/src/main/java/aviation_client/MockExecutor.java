package aviation_client;

public class MockExecutor implements QueryExecutor {
    @Override
    public void execute(String sql, String queryName) {
        System.out.println("Executing mock query: " + sql);
        String threadName = Thread.currentThread().getName();
        System.out.printf("[Thread: %s] Executing mock query: %s%n", threadName, queryName);
    }
}