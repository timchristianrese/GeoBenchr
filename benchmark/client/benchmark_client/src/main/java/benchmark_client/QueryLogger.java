package benchmark_client;

public class QueryLogger {
    // (2026-10-07) Every result line ends with " [rep=<n> sql=<hash> at=<time>]": which repetition of
    // the query it was (1-based, in YAML order) and a hash of its SQL, so each timing can be matched
    // to its exact parameters even if a timed-out query finishes and logs late.
    private static final ThreadLocal<String> TAG = new ThreadLocal<>();

    public static void setTag(String tag) {
        TAG.set(tag);
    }

    public static void clearTag() {
        TAG.remove();
    }

    private static String suffix(String tag) {
        String t = tag != null ? tag : TAG.get();
        return " [" + (t != null ? t + " " : "") + "at=" + java.time.Instant.now() + "]";
    }

    public static void logStart(String executorType, String application, String queryName, String runID) {
        String thread = Thread.currentThread().getName();
        System.out.printf("[%s] [%s] [%s] INFO: Starting '%s'%n", thread, executorType, application, queryName);
    }

    public static void logSuccess(String executorType, String application, String queryName, String runID, long durationMs) {
        String thread = Thread.currentThread().getName();
        System.out.printf("[%s] [%s] [%s] SUCCESS: '%s': %dms%s%n", thread, executorType, application, queryName, durationMs, suffix(null));
        // Write to a log file as well, called query_results.log
        // Append to the file
        // get the current hour of day as a String
        try (java.io.FileWriter fw = new java.io.FileWriter(executorType+"query_results_" + runID + ".log", true);
             java.io.BufferedWriter bw = new java.io.BufferedWriter(fw);
             java.io.PrintWriter out = new java.io.PrintWriter(bw)) {
            out.printf("[%s] [%s] [%s] SUCCESS: '%s': %dms%s%n", thread, executorType, application, queryName, durationMs, suffix(null));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void logTimeout(String executorType, String application, String queryName, String runID, long timeoutMs) {
        logTimeout(executorType, application, queryName, runID, timeoutMs, null);
    }

    public static void logTimeout(String executorType, String application, String queryName, String runID, long timeoutMs, String tag) {
        String thread = Thread.currentThread().getName();
        System.err.printf("[%s] [%s] [%s] TIMEOUT: '%s': exceeded %dms cutoff%n", thread, executorType, application, queryName, timeoutMs);
        try (java.io.FileWriter fw = new java.io.FileWriter(executorType+"query_results_" + runID + ".log", true);
             java.io.BufferedWriter bw = new java.io.BufferedWriter(fw);
             java.io.PrintWriter out = new java.io.PrintWriter(bw)) {
            out.printf("[%s] [%s] [%s] TIMEOUT: '%s': exceeded %dms cutoff%s%n", thread, executorType, application, queryName, timeoutMs, suffix(tag));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // (2026-10-14) an attempt interrupted by a DuckDB server restart that BenchmarkRunner runs again: informational
    // only, build_results.py ignores RETRIED lines and counts the attempt that follows (same rep= tag)
    public static void logRetried(String executorType, String application, String queryName, String runID, String reason, int attempt) {
        String thread = Thread.currentThread().getName();
        System.err.printf("[%s] [%s] [%s] RETRIED: '%s': attempt %d %s%n", thread, executorType, application, queryName, attempt, reason);
        try (java.io.FileWriter fw = new java.io.FileWriter(executorType+"query_results_" + runID + ".log", true);
             java.io.BufferedWriter bw = new java.io.BufferedWriter(fw);
             java.io.PrintWriter out = new java.io.PrintWriter(bw)) {
            out.printf("[%s] [%s] [%s] RETRIED: '%s': attempt %d %s%s%n", thread, executorType, application, queryName, attempt,
                    String.valueOf(reason).replace('\n', ' '), suffix(null));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void logError(String executorType, String application, String queryName, String runID, Exception e) {
        String thread = Thread.currentThread().getName();
        System.err.printf("[%s] [%s] [%s] ERROR: '%s': %s%n", thread, executorType, application, queryName, e.getMessage());
        // Write to a log file as well, called query_results.log
        // Append to the file
        try (java.io.FileWriter fw = new java.io.FileWriter(executorType+"query_results_" + runID + ".log", true);
        java.io.BufferedWriter bw = new java.io.BufferedWriter(fw);
        java.io.PrintWriter out = new java.io.PrintWriter(bw)) {
         out.printf("[%s] [%s] [%s] ERROR: '%s': %s%s%n", thread, executorType, application, queryName,
                 String.valueOf(e.getMessage()).replace('\n', ' '), suffix(null));
        } catch (Exception f) {
            f.printStackTrace();
        }
    }
}