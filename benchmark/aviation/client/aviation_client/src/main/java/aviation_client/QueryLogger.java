package aviation_client;

public class QueryLogger {
    public static void logStart(String executorType, String queryName) {
        String thread = Thread.currentThread().getName();
        System.out.printf("[%s] [%s] INFO: Starting '%s'%n", thread, executorType, queryName);
    }

    public static void logSuccess(String executorType, String queryName, long durationMs) {
        String thread = Thread.currentThread().getName();
        System.out.printf("[%s] [%s] SUCCESS: '%s': %dms%n", thread, executorType, queryName, durationMs);
        // Write to a log file as well, called query_results.log
        // Append to the file
        // get the current hour of day as a String
        String hour = String.valueOf(java.time.LocalDateTime.now().getHour());
        try (java.io.FileWriter fw = new java.io.FileWriter("query_results_" + hour + ".log", true);
             java.io.BufferedWriter bw = new java.io.BufferedWriter(fw);
             java.io.PrintWriter out = new java.io.PrintWriter(bw)) {
            out.printf("[%s] [%s] SUCCESS: '%s': %dms%n", thread, executorType, queryName, durationMs);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void logError(String executorType, String queryName, Exception e) {
        String thread = Thread.currentThread().getName();
        System.err.printf("[%s] [%s] ERROR: '%s': %s%n", thread, executorType, queryName, e.getMessage());
        // Write to a log file as well, called query_results.log
        // Append to the file
        String hour = String.valueOf(java.time.LocalDateTime.now().getHour());
        try (java.io.FileWriter fw = new java.io.FileWriter("query_results_" + hour + ".log", true);
        java.io.BufferedWriter bw = new java.io.BufferedWriter(fw);
        java.io.PrintWriter out = new java.io.PrintWriter(bw)) {
         out.printf("[%s] [%s] ERROR: '%s': %s%n", thread, executorType, queryName, e.getMessage());
        } catch (Exception f) {
            f.printStackTrace();
        }
    }
}