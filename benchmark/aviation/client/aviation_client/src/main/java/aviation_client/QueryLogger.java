package aviation_client;

public class QueryLogger {
    public static void logStart(String executorType, String queryName) {
        String thread = Thread.currentThread().getName();
        System.out.printf("[%s] [%s] Starting query: '%s'%n", thread, executorType, queryName);
    }

    public static void logSuccess(String executorType, String queryName, long durationMs) {
        String thread = Thread.currentThread().getName();
        System.out.printf("[%s] [%s] Query '%s' completed in %d ms%n", thread, executorType, queryName, durationMs);
    }

    public static void logError(String executorType, String queryName, Exception e) {
        String thread = Thread.currentThread().getName();
        System.err.printf("[%s] [%s] Error executing query '%s': %s%n", thread, executorType, queryName, e.getMessage());
    }
}
