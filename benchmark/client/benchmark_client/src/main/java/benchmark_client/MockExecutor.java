package benchmark_client;

public class MockExecutor implements QueryExecutor {
    @Override
    public void execute(String sql, String queryName, String dbType, String application, String runID) {
        
        String threadName = Thread.currentThread().getName();
        System.out.println(threadName+": Executing mock query of type " +queryName+": "+ sql);
        // simulate some processing time with a sleep
        try {
            Thread.sleep(100); // Simulate processing time
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore interrupted status
            System.err.println("Thread was interrupted: " + e.getMessage());
        }
    }
}