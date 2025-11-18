package benchmark_client;

public class QueryLogger {
    public static void logStart(String executorType, String application, String queryName, String runID) {
        String thread = Thread.currentThread().getName();
        System.out.printf("[%s] [%s] INFO: Starting '%s'%n", thread, executorType, application, queryName);
    }

    public static void logSuccess(String executorType, String application, String queryName, String runID, long durationMs) {
        String thread = Thread.currentThread().getName();
        System.out.printf("[%s] [%s] [%s] SUCCESS: '%s': %dms%n", thread, executorType, application, queryName, durationMs);
        // Write to a log file as well, called query_results.log
        // Append to the file
        // get the current hour of day as a String
        try (java.io.FileWriter fw = new java.io.FileWriter(executorType+"query_results_" + runID + ".log", true);
             java.io.BufferedWriter bw = new java.io.BufferedWriter(fw);
             java.io.PrintWriter out = new java.io.PrintWriter(bw)) {
            out.printf("[%s] [%s] [%s] SUCCESS: '%s': %dms%n", thread, executorType, application, queryName, durationMs);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void logError(String executorType, String application, String queryName, String runID, Exception e) {
        String thread = Thread.currentThread().getName();
        System.err.printf("[%s] [%s] ERROR: '%s': %s%n", thread, executorType, application, queryName, runID, e.getMessage());
        // Write to a log file as well, called query_results.log
        // Append to the file
        try (java.io.FileWriter fw = new java.io.FileWriter(executorType+"query_results_" + runID + ".log", true);
        java.io.BufferedWriter bw = new java.io.BufferedWriter(fw);
        java.io.PrintWriter out = new java.io.PrintWriter(bw)) {
         out.printf("[%s] [%s] [%s] ERROR: '%s': %s%n", thread, executorType, application, queryName, e.getMessage());
        } catch (Exception f) {
            f.printStackTrace();
        }
    }
}